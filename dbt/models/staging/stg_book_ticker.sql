select
    event_id,
    exchange,
    symbol,
    exchange_event_ts,
    ingest_ts,
    best_bid_price,
    best_bid_qty,
    best_ask_price,
    best_ask_qty,
    spread,
    mid_price,
    imbalance,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ source('normalized', 'book_ticker') }}
