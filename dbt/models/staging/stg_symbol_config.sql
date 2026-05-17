select
    symbol,
    exchange,
    status,
    base_asset,
    quote_asset,
    lot_size_min,
    lot_size_step,
    tick_size,
    created_at,
    updated_at
from {{ source('normalized', 'symbol_config') }}
