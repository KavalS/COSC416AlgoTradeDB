import psycopg2
import threading
import time
import statistics

DB_CONFIG = {
    "host":     "127.0.0.1",
    "port":     15433,
    "dbname":   "dolcy20_db",
    "user":     "dolcy20",
    "password": "", 
}

QUERIES = {
    "oltp_insert": """
        INSERT INTO core_dbms.market_data_5m (symbol, ts, open, high, low, close, volume, asset_type, created_at)
        VALUES ('TEST', NOW() - (random() * interval '365 days'), 
                100, 105, 98, 102, 50000, 'stock', NOW())
        ON CONFLICT (symbol, ts) DO NOTHING
    """,
    "dw_analytical": """
        SELECT s.symbol, COUNT(*) as bars, AVG(f.close) as avg_close,
               MAX(f.high) as peak, MIN(f.low) as trough
        FROM dw.fact_market_data f
        JOIN dw.dim_symbol s ON f.symbol_id = s.symbol_id
        JOIN dw.dim_time t ON f.time_id = t.time_id
        WHERE t.year = 2024 AND t.month BETWEEN 1 AND 6
        GROUP BY s.symbol ORDER BY avg_close DESC
    """,
    "mv_sma_lookup": """
        SELECT symbol, ts, sma_20 
        FROM core_dbms.mv_sma_20 
        WHERE symbol = 'tsla' 
          AND ts >= NOW() - INTERVAL '30 days'
        ORDER BY ts DESC LIMIT 100
    """
}

results = {q: [] for q in QUERIES}
lock = threading.Lock()

def run_worker(worker_id, query_name, sql, num_iterations=20):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        for _ in range(num_iterations):
            start = time.perf_counter()
            cur.execute(sql)
            elapsed = (time.perf_counter() - start) * 1000
            with lock:
                results[query_name].append(elapsed)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Worker {worker_id} error: {e}")

def run_scenario(scenario_name, query_name, sql, num_workers):
    print(f"\n--- {scenario_name} | {num_workers} concurrent workers ---")
    threads = [threading.Thread(target=run_worker, args=(i, query_name, sql))
               for i in range(num_workers)]
    start = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed_total = time.perf_counter() - start
    times = results[query_name][-num_workers * 20:]
    print(f"  Total wall time : {elapsed_total:.2f}s")
    print(f"  Avg latency     : {statistics.mean(times):.2f} ms")
    print(f"  P95 latency     : {sorted(times)[int(len(times) * 0.95)]:.2f} ms")
    print(f"  Max latency     : {max(times):.2f} ms")
    print(f"  Throughput      : {len(times) / elapsed_total:.1f} queries/sec")

if __name__ == "__main__":
    print("COSC 416 - Concurrent Workload Stress Test")
    print("===========================================")

    for workers in [1, 5, 10]:
        run_scenario("OLTP Insert", "oltp_insert", QUERIES["oltp_insert"], workers)

    for workers in [1, 5, 10]:
        run_scenario("DW Analytical", "dw_analytical", QUERIES["dw_analytical"], workers)

    for workers in [1, 5, 10]:
        run_scenario("MV SMA Lookup", "mv_sma_lookup", QUERIES["mv_sma_lookup"], workers)

    # Cleanup test rows
    print("\nCleaning up test rows...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    conn.cursor().execute("DELETE FROM core_dbms.market_data_5m WHERE symbol = 'TEST'")
    conn.close()
    print("Done.")
