

import psycopg2
import time
import numpy as np

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 15433,
    "dbname": "dolcy20_db",
    "user": "dolcy20",
    "password": "",
}

# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────
def connect():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn

# ─────────────────────────────────────────────
# QUERY RUNNER
# ─────────────────────────────────────────────
def run_query(conn, sql):
    cur = conn.cursor()
    start = time.perf_counter()
    cur.execute(sql)
    cur.fetchall()
    elapsed = (time.perf_counter() - start) * 1000
    cur.close()
    return elapsed

# ─────────────────────────────────────────────
# EXPLAIN ANALYZE
# ─────────────────────────────────────────────
def explain_query(conn, sql):
    cur = conn.cursor()
    cur.execute("EXPLAIN ANALYZE " + sql)
    plan = "\n".join(r[0] for r in cur.fetchall())
    cur.close()
    return plan

# ─────────────────────────────────────────────
# EXPERIMENT 1
# ─────────────────────────────────────────────
def experiment_1(conn):
    print("\n" + "="*60)
    print("EXPERIMENT 1: Time-Range Query")
    print("="*60)

    sql_base = """
        SELECT symbol, ts, close
        FROM core_dbms.market_data_5m
        WHERE symbol = 'tsla'
          AND ts >= '2024-01-01'
          AND ts < '2024-02-01'
        ORDER BY ts
    """

    sql_partition = """
        SELECT symbol, ts, close
        FROM core_dbms.market_data_5m_partitioned
        WHERE symbol = 'tsla'
          AND ts >= '2024-01-01'
          AND ts < '2024-02-01'
        ORDER BY ts
    """

    print("\n--- EXPLAIN: Base Table ---")
    print(explain_query(conn, sql_base))

    print("\n--- EXPLAIN: Partition ---")
    print(explain_query(conn, sql_partition))

    times_base = [run_query(conn, sql_base) for _ in range(5)]
    times_part = [run_query(conn, sql_partition) for _ in range(5)]

    base_avg = np.mean(times_base)
    part_avg = np.mean(times_part)

    print(f"\nBase Avg: {base_avg:.1f}ms")
    print(f"Partition Avg: {part_avg:.1f}ms")
    print(f"Speedup: {base_avg/part_avg:.2f}x")

    return base_avg, part_avg

# ─────────────────────────────────────────────
# EXPERIMENT 2 (FIXED)
# ─────────────────────────────────────────────
def experiment_2(conn):
    print("\n" + "="*60)
    print("EXPERIMENT 2: DW Analytical")
    print("="*60)

    sql = """
        SELECT s.symbol,
               COUNT(*) AS bars,
               AVG(f.close),
               MAX(f.high),
               MIN(f.low)
        FROM dw.fact_market_data f
        JOIN dw.dim_symbol s ON f.symbol_id = s.symbol_id
        JOIN dw.dim_time t   ON f.time_id = t.time_id
        WHERE t.year = 2024
          AND t.month BETWEEN 1 AND 6
        GROUP BY s.symbol
        ORDER BY AVG(f.close) DESC
    """

    print("\n--- EXPLAIN: Query ---")
    print(explain_query(conn, sql))

    times = [run_query(conn, sql) for _ in range(3)]
    avg = np.mean(times)

    print(f"\nAvg Execution Time: {avg:.1f}ms")

    return avg

# ─────────────────────────────────────────────
# EXPERIMENT 3 (FIXED PROPERLY)
# ─────────────────────────────────────────────
def experiment_3(conn):
    print("\n" + "="*60)
    print("EXPERIMENT 3: SMA — FULL COMPUTATION (FIXED)")
    print("="*60)

    # ❗ FULL computation (NO LIMIT)
    sql_otf = """
        SELECT AVG(close) OVER (
            ORDER BY ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )
        FROM core_dbms.market_data_5m
        WHERE symbol = 'tsla'
          AND ts >= '2024-07-01'
          AND ts < '2025-04-01'
    """

    sql_mv = """
        SELECT sma_20
        FROM core_dbms.mv_sma_20
        WHERE symbol = 'tsla'
          AND ts >= '2025-01-01'
          AND ts < '2025-04-01'
    """

    print("\n--- EXPLAIN: On-the-fly ---")
    print(explain_query(conn, sql_otf))

    print("\n--- EXPLAIN: MV ---")
    print(explain_query(conn, sql_mv))

    times_otf = [run_query(conn, sql_otf) for _ in range(3)]
    times_mv  = [run_query(conn, sql_mv) for _ in range(3)]

    avg_otf = np.mean(times_otf)
    avg_mv  = np.mean(times_mv)

    print(f"\nOn-the-fly Avg: {avg_otf:.1f}ms")
    print(f"MV Avg:         {avg_mv:.1f}ms")
    print(f"Speedup:        {avg_otf/avg_mv:.2f}x")

    return avg_otf, avg_mv

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("="*60)
    print("COSC 416 — FINAL PERFORMANCE DEMO (FIXED)")
    print("="*60)

    conn = connect()
    print("Connected.\n")

    r1_base, r1_part = experiment_1(conn)
    r2 = experiment_2(conn)
    r3_otf, r3_mv = experiment_3(conn)

    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)

    print(f"Exp1 Speedup (Partition): {r1_base/r1_part:.2f}x")
    print(f"Exp2 Avg Time: {r2:.1f}ms")
    print(f"Exp3 Speedup (MV): {r3_otf/r3_mv:.2f}x")

    conn.close()
