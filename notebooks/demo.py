"""
demo.py
-------
Connects to the DuckDB warehouse and visualises the rolling metrics mart.
Run this after the pipeline has executed at least once.

Usage:
    python notebooks/demo.py
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = Path("warehouse/nsei.duckdb")

def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Rolling metrics mart
    df = con.execute("""
        SELECT trade_date, symbol, close, rolling_vol_21d, rolling_return_21d
        FROM marts.mart_nsei_rolling_metrics
        ORDER BY trade_date DESC
        LIMIT 500
    """).df()
    print(df.head(10))

    # Sector performance mart
    sector = con.execute("""
        SELECT sector, avg_return_21d, avg_vol_21d
        FROM marts.mart_sector_performance
        ORDER BY avg_return_21d DESC
    """).df()
    print("\nSector Performance:")
    print(sector)

    # Plot rolling vol for top 3 symbols
    for sym in df["symbol"].unique()[:3]:
        sub = df[df["symbol"] == sym].sort_values("trade_date")
        plt.plot(sub["trade_date"], sub["rolling_vol_21d"], label=sym)
    plt.title("21-Day Rolling Volatility — NSEI Top Symbols")
    plt.legend()
    plt.tight_layout()
    plt.savefig("notebooks/rolling_vol_demo.png", dpi=150)
    print("\nSaved → notebooks/rolling_vol_demo.png")

if __name__ == "__main__":
    main()
