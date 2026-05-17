{{ config(materialized='view') }}

select
    exchange,
    symbol,
    latest_event_ts,
    latest_ingest_ts,
    staleness_seconds,
    freshness_status,
    case
        when staleness_seconds <= 60 then 1.0
        when staleness_seconds <= 120 then 0.5
        else 0.0
    end as health_score
from {{ ref('int_freshness_status') }}
