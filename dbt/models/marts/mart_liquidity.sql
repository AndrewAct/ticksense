{{ config(materialized='view') }}

select
    s.exchange,
    s.symbol,
    s.latest_ts,
    s.best_bid_price,
    s.best_ask_price,
    s.spread,
    s.mid_price,
    s.spread_bps,
    s.best_bid_qty,
    s.best_ask_qty,
    i.imbalance,
    i.market_signal,
    f.staleness_seconds,
    f.freshness_status
from {{ ref('int_spread_metrics') }}      s
left join {{ ref('int_order_book_imbalance') }} i
    on s.exchange = i.exchange
    and s.symbol  = i.symbol
left join {{ ref('int_freshness_status') }} f
    on s.exchange = f.exchange
    and s.symbol  = f.symbol
