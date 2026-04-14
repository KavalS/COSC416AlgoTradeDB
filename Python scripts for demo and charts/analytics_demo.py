"""
COSC 416 — Live Database Analytics & ML Demo
Connects to dolcy_db, pulls real data, generates charts + ML model
Usage: python analytics_demo.py
"""

import os
import psycopg2
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from sklearn.ensemble import RandomForestClassifier

DB_CONFIG = {
    "host":     "127.0.0.1",
    "port":     15433,
    "dbname":   "dolcy20_db",
    "user":     "dolcy20",
    "password": "",
}

BLUE   = '#1F4E79'
MID    = '#2E75B6'
LIGHT  = '#9DC3E6'
GREEN  = '#375623'
LGREEN = '#70AD47'
RED    = '#C00000'
AMBER  = '#C55A11'
GRAY   = '#595959'

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.25,
    'grid.linestyle': '--',
    'axes.titleweight': 'bold',
    'axes.titlesize': 12,
})

# ───────────── DATABASE CONNECTION ─────────────
def connect():
    print("Connecting to dolcy_db...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected.\n")
    return conn

# ───────────── DATA FETCH FUNCTIONS ─────────────
def fetch_ohlcv(conn, symbol, start='2024-01-01', end='2024-12-31'):
    q = """
        SELECT ts, open, high, low, close, volume
        FROM core_dbms.market_data_5m
        WHERE symbol = %s AND ts >= %s AND ts < %s
        ORDER BY ts
    """
    df = pd.read_sql(q, conn, params=(symbol, start, end))
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df.set_index('ts', inplace=True)
    print(f"    {len(df):,} rows fetched for {symbol.upper()}")
    return df

def fetch_sma(conn, symbol, start='2024-01-01', end='2024-12-31'):
    q = """
        SELECT ts, close, sma_20
        FROM core_dbms.mv_sma_20
        WHERE symbol = %s AND ts >= %s AND ts < %s
        ORDER BY ts
    """
    df = pd.read_sql(q, conn, params=(symbol, start, end))
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df.set_index('ts', inplace=True)
    return df

def fetch_multi_symbol_daily(conn, symbols, start='2024-01-01', end='2024-12-31'):
    placeholders = ','.join(['%s'] * len(symbols))
    q = f"""
        SELECT symbol, DATE_TRUNC('day', ts) AS day, AVG(close) AS avg_close
        FROM core_dbms.market_data_5m
        WHERE symbol IN ({placeholders}) AND ts >= %s AND ts < %s
        GROUP BY symbol, DATE_TRUNC('day', ts)
        ORDER BY symbol, day
    """
    df = pd.read_sql(q, conn, params=symbols + [start, end])
    df['day'] = pd.to_datetime(df['day'], utc=True)
    return df

def fetch_db_health(conn):
    q = """
        SELECT s.relname AS table_name,
               ROUND(s.heap_blks_hit::numeric /
                     NULLIF(s.heap_blks_read + s.heap_blks_hit, 0) * 100, 2) AS hit_pct,
               s.heap_blks_read AS disk_reads,
               s.heap_blks_hit  AS buffer_hits,
               u.seq_scan, u.idx_scan, u.n_live_tup AS live_rows
        FROM pg_statio_user_tables s
        JOIN pg_stat_user_tables u ON s.relname = u.relname AND s.schemaname = u.schemaname
        WHERE s.schemaname IN ('core_dbms','dw')
          AND (s.heap_blks_read + s.heap_blks_hit) > 0
          AND s.relname NOT LIKE '%%y20%%'
        ORDER BY s.heap_blks_read DESC
    """
    return pd.read_sql(q, conn)

# ───────────── CHART FUNCTIONS ─────────────
def chart_price_sma_signals(conn, symbol='tsla'):
    df = fetch_ohlcv(conn, symbol)
    sma = fetch_sma(conn, symbol)

    daily = df.resample('D').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    sma_daily = sma.resample('D').last().dropna()
    merged = daily.join(sma_daily[['sma_20']], how='inner')
    merged['sma_50'] = merged['close'].rolling(50).mean()
    merged.dropna(inplace=True)

    merged['prev_sma20'] = merged['sma_20'].shift(1)
    merged['prev_sma50'] = merged['sma_50'].shift(1)
    buy_signals  = merged[(merged['sma_20'] > merged['sma_50']) & (merged['prev_sma20'] <= merged['prev_sma50'])]
    sell_signals = merged[(merged['sma_20'] < merged['sma_50']) & (merged['prev_sma20'] >= merged['prev_sma50'])]

    fig = plt.figure(figsize=(14, 8))
    gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)

    ax1.plot(merged.index, merged['close'], color=BLUE, linewidth=1, alpha=0.8, label='Close Price')
    ax1.plot(merged.index, merged['sma_20'], color=LGREEN, linewidth=1.5, label='SMA 20')
    ax1.plot(merged.index, merged['sma_50'], color=AMBER, linewidth=1.5, linestyle='--', label='SMA 50')
    ax1.scatter(buy_signals.index, buy_signals['close'], marker='^', color=LGREEN, s=120, zorder=5,
                label=f'Buy Signal ({len(buy_signals)})')
    ax1.scatter(sell_signals.index, sell_signals['close'], marker='v', color=RED, s=120, zorder=5,
                label=f'Sell Signal ({len(sell_signals)})')
    ax1.set_title(f'{symbol.upper()} — Daily Close Price with SMA20/SMA50 Signals', pad=12)
    ax1.set_ylabel('Price (USD)')
    ax1.legend(fontsize=9, loc='upper left')
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:.0f}'))
    plt.setp(ax1.get_xticklabels(), visible=False)

    colors = [LGREEN if c >= o else RED for c, o in zip(merged['close'], merged['open'])]
    ax2.bar(merged.index, merged['volume'], color=colors, alpha=0.6, width=0.8)
    ax2.set_ylabel('Volume', fontsize=9)
    ax2.set_xlabel('Date')
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M'))

    filename = os.path.join(OUTPUT_DIR, f'chart1_price_signals_{symbol}.png')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"  Saved {filename}")
    return len(buy_signals), len(sell_signals)

def chart_multi_symbol(conn, symbols=['tsla','aapl','goog']):
    df = fetch_multi_symbol_daily(conn, symbols)
    fig, ax = plt.subplots(figsize=(12,6))
    for sym in symbols:
        sym_df = df[df['symbol']==sym]
        ax.plot(sym_df['day'], sym_df['avg_close'], label=sym.upper())
    ax.set_title("Daily Close Price Comparison")
    ax.set_ylabel("Price (USD)")
    ax.set_xlabel("Date")
    ax.legend()
    filename = os.path.join(OUTPUT_DIR, 'chart_multi_symbol.png')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"  Saved {filename}")

def chart_volatility_heatmap(conn, symbol='tsla'):
    df = fetch_ohlcv(conn, symbol)
    daily = df['close'].resample('D').ohlc().dropna()
    daily['returns'] = daily['close'].pct_change()
    vol = daily['returns'].rolling(5).std() * np.sqrt(252)
    fig, ax = plt.subplots(figsize=(12,6))
    c = ax.imshow(vol.values.reshape(-1,1).T, cmap='Reds', aspect='auto')
    ax.set_title(f'{symbol.upper()} — 5-Day Rolling Volatility')
    ax.set_yticks([])
    ax.set_xticks(range(len(vol)))
    ax.set_xticklabels(vol.index.strftime('%b %d'), rotation=90, fontsize=8)
    fig.colorbar(c, ax=ax, orientation='vertical', label='Volatility')
    filename = os.path.join(OUTPUT_DIR, 'chart_volatility_heatmap.png')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"  Saved {filename}")

def chart_db_health(conn):
    df = fetch_db_health(conn)
    fig, ax = plt.subplots(figsize=(12,6))
    ax.barh(df['table_name'], df['hit_pct'], color=BLUE)
    ax.set_xlabel("Cache Hit %")
    ax.set_title("DB Health — Top Tables by Disk Reads")
    filename = os.path.join(OUTPUT_DIR, 'chart_db_health.png')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"  Saved {filename}")

def ml_price_direction(conn, symbol='tsla'):
    df = fetch_ohlcv(conn, symbol)
    df['returns'] = df['close'].pct_change().shift(-1)
    df['target'] = (df['returns'] > 0).astype(int)
    df.dropna(inplace=True)

    features = ['open','high','low','close','volume']
    X = df[features]
    y = df['target']

    model = RandomForestClassifier(n_estimators=50, random_state=42)
    model.fit(X, y)
    df['pred'] = model.predict(X)

    fig, ax = plt.subplots(figsize=(12,6))
    ax.plot(df.index, df['close'], label='Close Price', color=BLUE)
    ax.scatter(df.index[df['pred']==1], df['close'][df['pred']==1], marker='^', color=LGREEN, label='Predicted Up')
    ax.scatter(df.index[df['pred']==0], df['close'][df['pred']==0], marker='v', color=RED, label='Predicted Down')
    ax.set_title(f"{symbol.upper()} — ML Price Direction Predictions")
    ax.set_ylabel("Price (USD)")
    ax.set_xlabel("Date")
    ax.legend()
    filename = os.path.join(OUTPUT_DIR, 'ml_price_direction_results.png')
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f"  Saved {filename}")

# ───────────── MAIN ─────────────
if __name__ == "__main__":
    print("="*60)
    print("COSC 416 — Live Analytics & ML Demo")
    print("="*60)

    conn = connect()

    print("\n--- Generating Trading Charts ---")
    buys, sells = chart_price_sma_signals(conn, 'tsla')
    print(f"  Found {buys} buy signals and {sells} sell signals for TSLA in 2024")

    print("  Generating multi-symbol chart...")
    chart_multi_symbol(conn, ['tsla','aapl','goog'])

    print("  Generating volatility heatmap...")
    chart_volatility_heatmap(conn, 'tsla')

    print("  Generating DB health chart...")
    chart_db_health(conn)

    print("  Running ML price direction model...")
    ml_price_direction(conn, 'tsla')

    conn.close()
    print("\nALL DONE. Charts saved in ./output")
