-- models/marts/mart_sector_performance.sql
-- Daily sector-level performance aggregation

{{ config(materialized='table') }}

WITH daily AS (
    SELECT * FROM {{ ref('stg_nsei_daily') }}
),

meta AS (
    SELECT symbol, sector, market_cap_bucket
    FROM {{ ref('seed_symbol_metadata') }}
),

joined AS (
    SELECT
        d.trade_date,
        m.sector,
        m.market_cap_bucket,
        d.symbol,
        d.close_price,
        d.daily_return,
        d.dollar_volume
    FROM daily d
    LEFT JOIN meta m USING (symbol)
),

sector_agg AS (
    SELECT
        trade_date,
        sector,
        COUNT(DISTINCT symbol)                                          AS num_stocks,
        AVG(daily_return)                                               AS sector_return,
        STDDEV(daily_return)                                            AS sector_vol,
        SUM(dollar_volume)                                              AS sector_dollar_volume,
        SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END)              AS gainers,
        SUM(CASE WHEN daily_return < 0 THEN 1 ELSE 0 END)              AS losers,
        MAX(daily_return)                                               AS best_stock_return,
        MIN(daily_return)                                               AS worst_stock_return,

        AVG(AVG(daily_return)) OVER (
            PARTITION BY sector
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS sector_return_5d_avg

    FROM joined
    GROUP BY trade_date, sector
)

SELECT
    *,
    ROUND(gainers::DOUBLE / NULLIF(num_stocks, 0) * 100, 1)            AS breadth_pct,
    RANK() OVER (PARTITION BY trade_date ORDER BY sector_return DESC)   AS daily_rank
FROM sector_agg