select
    exchange,
    symbol,
    max(exchange_event_ts)                                              as latest_event_ts,
    max(ingest_ts)                                                      as latest_ingest_ts,
    date_diff('second', max(exchange_event_ts), current_timestamp)      as staleness_seconds,
    case
        when date_diff('second', max(exchange_event_ts), current_timestamp) <= 60 then 'FRESH'
        when date_diff('second', max(exchange_event_ts), current_timestamp) <= 120 then 'WARN'
        else 'STALE'
    end                                                                 as freshness_status
from {{ ref('stg_book_ticker') }}
group by exchange, symbol
