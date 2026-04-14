"""
COSC 416 — Part 3 Live Performance Monitor
Run this during the demo to show real-time system health
Usage: python demo_monitoring.py
"""

import psycopg2
import time
import os
from datetime import datetime

DB_CONFIG = {
    "host":     "127.0.0.1",
    "port":     15433,
    "dbname":   "dolcy20_db",
    "user":     "dolcy20",
    "password": "",
}



SLO = {
    "buffer_hit_pct_min": 90.0,
    "golden_query_max_ms": 2000.0,
    "index_hit_pct_min":   90.0,
}

def clear():
    os.system('clear' if os.name == 'posix' else 'cls')

def get_buffer_health(cur):
    cur.execute("""
        SELECT relname,
               ROUND(heap_blks_hit::numeric /
                     NULLIF(heap_blks_read + heap_blks_hit, 0) * 100, 2) AS hit_pct,
               heap_blks_read  AS disk_reads,
               heap_blks_hit   AS buffer_hits
        FROM pg_statio_user_tables
        WHERE schemaname IN ('core_dbms', 'dw')
          AND (heap_blks_read + heap_blks_hit) > 0
        ORDER BY heap_blks_read DESC
    """)
    return cur.fetchall()

def get_index_health(cur):
    cur.execute("""
        SELECT s.indexrelname,
               ROUND(s.idx_blks_hit::numeric /
                     NULLIF(s.idx_blks_read + s.idx_blks_hit, 0) * 100, 2) AS hit_pct,
               u.idx_scan AS scans
        FROM pg_statio_user_indexes s
        JOIN pg_stat_user_indexes u
          ON s.indexrelname = u.indexrelname
         AND s.schemaname   = u.schemaname
        WHERE s.schemaname IN ('core_dbms', 'dw')
          AND (s.idx_blks_read + s.idx_blks_hit) > 0
        ORDER BY u.idx_scan DESC
        LIMIT 8
    """)
    return cur.fetchall()

def get_active_queries(cur):
    cur.execute("""
        SELECT pid,
               ROUND(EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000) AS ms,
               state,
               LEFT(query, 60) AS query_snippet
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query NOT LIKE '%pg_stat_activity%'
          AND query_start IS NOT NULL
        ORDER BY query_start
        LIMIT 5
    """)
    return cur.fetchall()

def get_golden_query_time(cur):
    cur.execute("""
        SELECT s.symbol, COUNT(*) AS bars, AVG(f.close) AS avg_close
        FROM dw.fact_market_data f
        JOIN dw.dim_symbol s ON f.symbol_id = s.symbol_id
        JOIN dw.dim_time   t ON f.time_id   = t.time_id
        WHERE t.year = 2024 AND t.month BETWEEN 1 AND 6
        GROUP BY s.symbol
        ORDER BY avg_close DESC
    """)
    cur.fetchall()

def status(val, threshold, higher_is_better=True):
    ok = val >= threshold if higher_is_better else val <= threshold
    if ok:
        return "✅ OK"
    return "🚨 ALERT"

def run_monitor():
    print("Connecting to DRI PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    print("Connected. Starting monitor (Ctrl+C to stop)...\n")
    time.sleep(1)

    iteration = 0
    while True:
        iteration += 1
        clear()

        print("=" * 70)
        print(f"  COSC 416 — Live Performance Monitor  |  {datetime.now().strftime('%H:%M:%S')}")
        print(f"  Database: dolcy_db  |  Refresh #{iteration}")
        print("=" * 70)

        # ── Golden Query Timing ──────────────────────────────────────────────
        print("\n📊 GOLDEN QUERY BENCHMARK (SLO: < 2000ms)")
        print("-" * 50)
        t0 = time.perf_counter()
        get_golden_query_time(cur)
        elapsed = (time.perf_counter() - t0) * 1000
        slo_status = status(elapsed, SLO["golden_query_max_ms"], higher_is_better=False)
        print(f"  Execution time : {elapsed:.1f} ms   {slo_status}")
        print(f"  SLO threshold  : {SLO['golden_query_max_ms']:.0f} ms")

        # ── Buffer Health ────────────────────────────────────────────────────
        print("\n💾 BUFFER CACHE HIT RATIOS (SLO: >= 90%)")
        print("-" * 50)
        rows = get_buffer_health(cur)
        if rows:
            print(f"  {'Table':<30} {'Hit %':>8}  {'Status'}")
            print(f"  {'-'*30} {'-'*8}  {'-'*15}")
            for relname, hit_pct, disk_reads, buf_hits in rows:
                if hit_pct is None:
                    continue
                st = status(float(hit_pct), SLO["buffer_hit_pct_min"])
                print(f"  {relname:<30} {float(hit_pct):>7.2f}%  {st}")
        else:
            print("  No data (queries not yet run against these tables)")

        # ── Index Health ─────────────────────────────────────────────────────
        print("\n🔍 INDEX BUFFER HIT RATIOS (SLO: >= 90%)")
        print("-" * 50)
        irows = get_index_health(cur)
        if irows:
            print(f"  {'Index':<35} {'Hit %':>8}  {'Scans':>8}  {'Status'}")
            print(f"  {'-'*35} {'-'*8}  {'-'*8}  {'-'*10}")
            for iname, hit_pct, scans in irows:
                if hit_pct is None:
                    continue
                st = status(float(hit_pct), SLO["index_hit_pct_min"])
                print(f"  {iname:<35} {float(hit_pct):>7.2f}%  {scans:>8}  {st}")
        else:
            print("  No index I/O data yet")

        # ── Active Queries ───────────────────────────────────────────────────
        print("\n⚡ ACTIVE QUERIES")
        print("-" * 50)
        aq = get_active_queries(cur)
        if aq:
            for pid, ms, state, snippet in aq:
                warn = "SLOW" if ms and float(ms) > 2000 else ""
                print(f"  PID {pid} | {ms}ms | {state} | {snippet}{warn}")
        else:
            print("  No active queries")

        print("\n" + "=" * 70)
        print("  SLOs:  Buffer Hit >= 90%  |  Golden Query < 2000ms  |  Index Hit >= 90%")
        print("  Press Ctrl+C to stop monitoring")
        print("=" * 70)

        time.sleep(5)

if __name__ == "__main__":
    try:
        run_monitor()
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
    except Exception as e:
        print(f"\nError: {e}")
        print("Check DB_CONFIG credentials and ensure DRI tunnel is active.")
