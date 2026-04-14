

-- DEMO 1: Baseline Health Check (SLO Dashboard)
-- roves the system is in a healthy state


SELECT
    relname                                          AS table_name,
    heap_blks_hit + heap_blks_read                   AS total_accesses,
    heap_blks_hit                                    AS buffer_hits,
    heap_blks_read                                   AS disk_reads,
    ROUND(heap_blks_hit::numeric /
          NULLIF(heap_blks_read + heap_blks_hit, 0)
          * 100, 2)                                  AS buffer_hit_pct,
    CASE
        WHEN ROUND(heap_blks_hit::numeric /
             NULLIF(heap_blks_read + heap_blks_hit, 0) * 100, 2) >= 99
        THEN 'HEALTHY'
        WHEN ROUND(heap_blks_hit::numeric /
             NULLIF(heap_blks_read + heap_blks_hit, 0) * 100, 2) >= 90
        THEN 'WARNING'
        ELSE 'CRITICAL - SLO BREACH'
    END                                              AS slo_status
FROM pg_statio_user_tables
WHERE schemaname IN ('core_dbms', 'dw')
ORDER BY heap_blks_read DESC;


-- DEMO 2: Golden Query — SLO Benchmark

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    s.symbol,
    COUNT(*)        AS bars,
    AVG(f.close)    AS avg_close,
    MAX(f.high)     AS peak,
    MIN(f.low)      AS trough
FROM dw.fact_market_data f
JOIN dw.dim_symbol s ON f.symbol_id = s.symbol_id
JOIN dw.dim_time   t ON f.time_id   = t.time_id
WHERE t.year = 2024
  AND t.month BETWEEN 1 AND 6
GROUP BY s.symbol
ORDER BY avg_close DESC;


-- DEMO 3: Index Utilization — show what's being used

SELECT
    schemaname,
    relname         AS table_name,
    indexrelname    AS index_name,
    idx_scan        AS times_used,
    idx_tup_read    AS rows_read_via_index,
    idx_blks_hit    AS index_buffer_hits,
    idx_blks_read   AS index_disk_reads,
    ROUND(idx_blks_hit::numeric /
          NULLIF(idx_blks_read + idx_blks_hit, 0)
          * 100, 2) AS index_hit_pct,
    CASE
        WHEN idx_scan = 0 THEN 'UNUSED — candidate for removal'
        WHEN idx_scan < 5 THEN 'LOW USAGE'
        ELSE 'ACTIVE'
    END             AS usage_status
FROM pg_statio_user_indexes
WHERE schemaname IN ('core_dbms', 'dw')
ORDER BY idx_scan DESC;


-- DEMO 4: Partition Pruning — show only relevant partitions are scanned

EXPLAIN (ANALYZE, BUFFERS)
SELECT symbol, ts, close
FROM core_dbms.market_data_5m_partitioned
WHERE symbol = 'tsla'
  AND ts >= '2024-01-01'
  AND ts <  '2024-02-01'
ORDER BY ts;


-- DEMO 5: Materialized View vs On-the-fly

-- On-the-fly (SLOW):
EXPLAIN (ANALYZE, BUFFERS)
SELECT symbol, ts, close,
       AVG(close) OVER (
           PARTITION BY symbol ORDER BY ts
           ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
       ) AS sma_20
FROM core_dbms.market_data_5m
WHERE symbol = 'tsla'
  AND ts >= NOW() - INTERVAL '30 days'
ORDER BY ts DESC
LIMIT 100;

-- Materialized view (FAST):
EXPLAIN (ANALYZE, BUFFERS)
SELECT symbol, ts, sma_20
FROM core_dbms.mv_sma_20
WHERE symbol = 'tsla'
  AND ts >= NOW() - INTERVAL '30 days'
ORDER BY ts DESC
LIMIT 100;


-- DEMO 6: Post-Deployment Monitoring Alert Query


SELECT
    NOW()                                            AS checked_at,
    relname                                          AS table_name,
    ROUND(heap_blks_hit::numeric /
          NULLIF(heap_blks_read + heap_blks_hit, 0)
          * 100, 2)                                  AS buffer_hit_pct,
    CASE
        WHEN ROUND(heap_blks_hit::numeric /
             NULLIF(heap_blks_read + heap_blks_hit, 0) * 100, 2) < 90
        THEN '*** ALERT: Buffer hit ratio below 90% SLO ***'
        ELSE 'OK'
    END                                              AS alert_status
FROM pg_statio_user_tables
WHERE schemaname IN ('core_dbms', 'dw')

UNION ALL

SELECT
    NOW(),
    'SYSTEM: avg_wait_ms' AS table_name,
    ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000)::numeric, 2),
    CASE
        WHEN AVG(EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000) > 2000
        THEN '*** ALERT: Active queries exceed 2000ms SLO ***'
        ELSE 'OK'
    END
FROM pg_stat_activity
WHERE state = 'active'
  AND query_start IS NOT NULL
  AND query NOT LIKE '%pg_stat_activity%';



-- DEMO 7: Runbook — Reindex Script

-- Step 1: Identify degraded indexes
SELECT indexrelname, idx_blks_read, idx_blks_hit,
       ROUND(idx_blks_hit::numeric /
             NULLIF(idx_blks_read + idx_blks_hit, 0) * 100, 2) AS hit_pct
FROM pg_statio_user_indexes
WHERE schemaname = 'core_dbms'
  AND ROUND(idx_blks_hit::numeric /
            NULLIF(idx_blks_read + idx_blks_hit, 0) * 100, 2) < 90;

-- Step 2: Reindex concurrently (non-blocking)
REINDEX INDEX CONCURRENTLY core_dbms.idx_market_data_5m_ts;

-- Step 3: Update statistics
ANALYZE core_dbms.market_data_5m;
ANALYZE dw.fact_market_data;

-- Step 4: Verify recovery
SELECT indexrelname,
       ROUND(idx_blks_hit::numeric /
             NULLIF(idx_blks_read + idx_blks_hit, 0) * 100, 2) AS hit_pct
FROM pg_statio_user_indexes
WHERE schemaname IN ('core_dbms', 'dw')
ORDER BY hit_pct ASC;

