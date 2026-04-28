-- models/staging/stg_nsei_daily.sql

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'nsei_daily') }}
),

cleaned AS (
    SELECT
        symbol,
        trade_date,
        open::DOUBLE             AS open_price,
        high::DOUBLE             AS high_price,
        low::DOUBLE              AS low_price,
        close::DOUBLE            AS close_price,
        volume::BIGINT           AS volume,
        prev_close::DOUBLE       AS prev_close,
        daily_return::DOUBLE     AS daily_return,
        vwap::DOUBLE             AS vwap,
        dollar_volume::DOUBLE    AS dollar_volume,
        intraday_range_pct::DOUBLE AS intraday_range_pct,
        gap_pct::DOUBLE          AS gap_pct,
        is_green::BOOLEAN        AS is_green,
        ingested_at,
        processed_at,
        pipeline_run_id

    FROM source
    WHERE
        close::DOUBLE > 0
        AND volume::BIGINT > 0
        AND high::DOUBLE >= low::DOUBLE
        AND trade_date IS NOT NULL
)

SELECT * FROM cleaned