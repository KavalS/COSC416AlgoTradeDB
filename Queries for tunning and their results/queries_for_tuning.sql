-- 20-period Simple Moving Average for dw
CREATE MATERIALIZED VIEW dw.mv_sma_20 AS
SELECT
    f.fact_id,
    s.symbol,
    t.ts,
    f.close,
    AVG(f.close) OVER (
        PARTITION BY f.symbol_id
        ORDER BY t.ts
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM dw.fact_market_data f
JOIN dw.dim_symbol s ON s.symbol_id = f.symbol_id
JOIN dw.dim_time   t ON t.time_id   = f.time_id
ORDER BY s.symbol, t.ts;

-- 50-period Simple Moving Average for dw
CREATE MATERIALIZED VIEW dw.mv_sma_50 AS
SELECT
    f.fact_id,
    s.symbol,
    t.ts,
    f.close,
    AVG(f.close) OVER (
        PARTITION BY f.symbol_id
        ORDER BY t.ts
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS sma_50
FROM dw.fact_market_data f
JOIN dw.dim_symbol s ON s.symbol_id = f.symbol_id
JOIN dw.dim_time   t ON t.time_id   = f.time_id
ORDER BY s.symbol, t.ts;

-- Indexes for fast lookup on symbol and timestamp for dw.mv_sma_20 and dw.mv_sma_50
CREATE INDEX ON dw.mv_sma_20 (symbol, ts);
CREATE INDEX ON dw.mv_sma_50 (symbol, ts);

-- 20-period Simple Moving Average for core_dbms
CREATE MATERIALIZED VIEW core_dbms.mv_sma_20 AS
SELECT
    symbol,
    ts,
    close,
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY ts
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM core_dbms.market_data_5m
ORDER BY symbol, ts;

-- 50-period Simple Moving Average for core_dbms
CREATE MATERIALIZED VIEW core_dbms.mv_sma_50 AS
SELECT
    symbol,
    ts,
    close,
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY ts
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS sma_50
FROM core_dbms.market_data_5m
ORDER BY symbol, ts;

-- Indexes for fast lookup on symbol and timestamp for core_dbms.mv_sma_20 and core_dbms.mv_sma_50
CREATE INDEX ON core_dbms.mv_sma_20 (symbol, ts);
CREATE INDEX ON core_dbms.mv_sma_50 (symbol, ts);

-- btree indexes were already created on symbol and ts columns in the original tables, to comapare created Brin indexes
CREATE INDEX idx_market_data_5m_ts_brin
ON core_dbms.market_data_5m
USING BRIN (ts);

-- Disabled B-Tree temporarily so PostgreSQL uses BRIN
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN ANALYZE
SELECT * FROM core_dbms.market_data_5m
WHERE ts BETWEEN '2024-01-01' AND '2024-02-01';

-- Re-enabled after
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

-- To comapre performance after creating materialized views, on fly calculation vs pre-calculated values in materialized views was done by running the following queries and comparing execution times:

-- On-the-fly calculation for 20-period SMA
EXPLAIN ANALYZE
SELECT
    symbol, ts, close,
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY ts
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM core_dbms.market_data_5m
WHERE symbol = 'tsla'
ORDER BY ts;

-- FAST: Querying the pre-computed materialized view
EXPLAIN ANALYZE
SELECT symbol, ts, close, sma_20
FROM core_dbms.mv_sma_20
WHERE symbol = 'tsla'
ORDER BY ts;

-- BEFORE: Heavy analytical query with no covering index
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN ANALYZE
SELECT
    s.symbol,
    DATE_TRUNC('day', t.ts) AS trade_date,
    MIN(f.low)              AS day_low,
    MAX(f.high)             AS day_high,
    SUM(f.volume)           AS total_volume,
    AVG(f.close)            AS avg_close
FROM dw.fact_market_data f
JOIN dw.dim_symbol s ON s.symbol_id = f.symbol_id
JOIN dw.dim_time   t ON t.time_id   = f.time_id
WHERE s.symbol = 'tsla'
  AND t.ts BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY s.symbol, DATE_TRUNC('day', t.ts)
ORDER BY trade_date;

-- Re-enabled indexes after testing
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

-- Added a covering index that includes all columns needed by the query
CREATE INDEX idx_fact_covering
ON dw.fact_market_data (symbol_id, time_id)
INCLUDE (high, low, close, volume);

EXPLAIN ANALYZE
SELECT
    s.symbol,
    DATE_TRUNC('day', t.ts) AS trade_date,
    MIN(f.low)              AS day_low,
    MAX(f.high)             AS day_high,
    SUM(f.volume)           AS total_volume,
    AVG(f.close)            AS avg_close
FROM dw.fact_market_data f
JOIN dw.dim_symbol s ON s.symbol_id = f.symbol_id
JOIN dw.dim_time   t ON t.time_id   = f.time_id
WHERE s.symbol = 'tsla'
  AND t.ts BETWEEN '2024-01-01' AND '2024-06-30'
GROUP BY s.symbol, DATE_TRUNC('day', t.ts)
ORDER BY trade_date;

-- to find slowest queries currently running in the database, the following query was used to monitor active queries and their durations:
SELECT
    pid,
    now() - pg_stat_activity.query_start AS duration,
    query,
    state
FROM pg_stat_activity
WHERE state != 'idle'
AND query_start IS NOT NULL
ORDER BY duration DESC;

-- To check the most frequently scanned tables and their scan types, the following query was used to analyze table access patterns:
SELECT
    schemaname,
    relname                          AS table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_live_tup                       AS live_rows
FROM pg_stat_user_tables
WHERE schemaname IN ('core_dbms', 'dw')
ORDER BY seq_tup_read DESC;

-- To check the most frequently used indexes and their usage statistics, the following query was used to analyze index access patterns:
SELECT
    schemaname,
    relname                          AS table_name,
    indexrelname                     AS index_name,
    idx_scan                         AS times_used,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname IN ('core_dbms', 'dw')
ORDER BY idx_scan DESC;

-- To check the current configuration settings related to memory and parallelism, the following query was used to analyze database performance tuning parameters:
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'shared_buffers',
    'work_mem',
    'effective_cache_size',
    'max_parallel_workers',
    'max_worker_processes'
)
ORDER BY name;

-- partioning the market_data_5m table by month and seperate tables were created for it
CREATE TABLE core_dbms.market_data_5m_y2023m07
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');

CREATE TABLE core_dbms.market_data_5m_y2023m08
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');

CREATE TABLE core_dbms.market_data_5m_y2023m09
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');

CREATE TABLE core_dbms.market_data_5m_y2023m10
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');

CREATE TABLE core_dbms.market_data_5m_y2023m11
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');

CREATE TABLE core_dbms.market_data_5m_y2023m12
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

CREATE TABLE core_dbms.market_data_5m_y2023m07
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');

CREATE TABLE core_dbms.market_data_5m_y2023m08
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');

CREATE TABLE core_dbms.market_data_5m_y2023m09
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');

CREATE TABLE core_dbms.market_data_5m_y2023m10
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');

CREATE TABLE core_dbms.market_data_5m_y2023m11
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');

CREATE TABLE core_dbms.market_data_5m_y2023m12
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

CREATE TABLE core_dbms.market_data_5m_y2024m07
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE core_dbms.market_data_5m_y2024m08
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE core_dbms.market_data_5m_y2024m09
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE core_dbms.market_data_5m_y2024m10
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE core_dbms.market_data_5m_y2024m11
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE core_dbms.market_data_5m_y2024m12
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE core_dbms.market_data_5m_y2025m01
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE core_dbms.market_data_5m_y2025m02
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE core_dbms.market_data_5m_y2025m03
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE core_dbms.market_data_5m_y2025m04
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE core_dbms.market_data_5m_y2025m05
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE core_dbms.market_data_5m_y2025m06
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

CREATE TABLE core_dbms.market_data_5m_y2025m07
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE core_dbms.market_data_5m_y2025m08
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE core_dbms.market_data_5m_y2025m09
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE core_dbms.market_data_5m_y2025m10
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE core_dbms.market_data_5m_y2025m11
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE core_dbms.market_data_5m_y2025m12
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE core_dbms.market_data_5m_y2025m04
    PARTITION OF core_dbms.market_data_5m_partitioned
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

-- adding indexes on the partitioned tables for faster query performance
CREATE INDEX ON core_dbms.market_data_5m_partitioned (ts);
CREATE INDEX ON core_dbms.market_data_5m_partitioned (symbol, ts);

EXPLAIN ANALYZE
SELECT symbol, ts, open, high, low, close, volume
FROM core_dbms.market_data_5m_partitioned
WHERE ts BETWEEN '2024-01-01' AND '2024-02-01'
AND symbol = 'tsla';

