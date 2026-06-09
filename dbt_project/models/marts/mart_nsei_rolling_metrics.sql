-- models/marts/mart_nsei_rolling_metrics.sql
-- Rolling 5/10/20-day metrics per symbol — analytics + ML feature store

{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_nsei_daily') }}
),

rolling AS (
    SELECT
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_return,
        vwap,
        dollar_volume,
        intraday_range_pct,
        gap_pct,
        is_green,

        -- Rolling returns
        AVG(daily_return) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS return_5d_avg,

        AVG(daily_return) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS return_20d_avg,

        -- Rolling volatility
        STDDEV(daily_return) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS vol_10d,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_20d,

        -- Rolling volume
        AVG(volume) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20d,

        -- Volume ratio: today vs prior-20d avg
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
            ), 0
        ) AS volume_ratio_20d,

        -- Price momentum (rate of change)
        (close_price - LAG(close_price, 5)  OVER (PARTITION BY symbol ORDER BY trade_date))
            / NULLIF(LAG(close_price, 5)  OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
        AS roc_5d,

        (close_price - LAG(close_price, 20) OVER (PARTITION BY symbol ORDER BY trade_date))
            / NULLIF(LAG(close_price, 20) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
        AS roc_20d,

        -- Drawdown from 20d high
        (close_price - MAX(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) / NULLIF(MAX(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) * 100 AS drawdown_from_20d_high,

        -- Annualised Sharpe (20d, risk-free = 5% p.a.)
        CASE
            WHEN STDDEV(daily_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) > 0
            THEN (
                    AVG(daily_return) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                    - 0.05 / 252
                 )
                 / STDDEV(daily_return) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                 * SQRT(252)
        END AS sharpe_20d

    FROM base
)

SELECT
    *,
    CASE
        WHEN vol_20d * SQRT(252) > 0.35 THEN 'high_vol'
        WHEN vol_20d * SQRT(252) > 0.18 THEN 'medium_vol'
        ELSE 'low_vol'
    END AS vol_regime,

    CASE
        WHEN roc_20d >  5 THEN 'uptrend'
        WHEN roc_20d < -5 THEN 'downtrend'
        ELSE 'sideways'
    END AS trend_label

FROM rolling