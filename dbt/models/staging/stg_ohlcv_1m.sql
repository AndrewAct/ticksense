select
    exchange,
    symbol,
    window_start,
    window_end,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    tick_count,
    first_ts,
    last_ts
from {{ source('normalized', 'ohlcv_1m') }}
