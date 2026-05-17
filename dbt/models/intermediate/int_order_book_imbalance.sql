with ranked as (
    select
        exchange,
        symbol,
        exchange_event_ts,
        best_bid_qty,
        best_ask_qty,
        imbalance,
        row_number() over (
            partition by exchange, symbol
            order by exchange_event_ts desc
        ) as rn
    from {{ ref('stg_book_ticker') }}
)
select
    exchange,
    symbol,
    exchange_event_ts   as latest_ts,
    best_bid_qty,
    best_ask_qty,
    imbalance,
    case
        when imbalance > 0.6 then 'BUY_PRESSURE'
        when imbalance < 0.4 then 'SELL_PRESSURE'
        else 'NEUTRAL'
    end                 as market_signal
from ranked
where rn = 1
