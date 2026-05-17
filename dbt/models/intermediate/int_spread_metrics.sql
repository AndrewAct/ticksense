with ranked as (
    select
        exchange,
        symbol,
        exchange_event_ts,
        best_bid_price,
        best_ask_price,
        spread,
        mid_price,
        best_bid_qty,
        best_ask_qty,
        row_number() over (
            partition by exchange, symbol
            order by exchange_event_ts desc
        ) as rn
    from {{ ref('stg_book_ticker') }}
)
select
    exchange,
    symbol,
    exchange_event_ts                                    as latest_ts,
    best_bid_price,
    best_ask_price,
    spread,
    mid_price,
    round(spread / nullif(mid_price, 0) * 10000, 4)     as spread_bps,
    best_bid_qty,
    best_ask_qty
from ranked
where rn = 1
