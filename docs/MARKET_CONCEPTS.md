# Market Microstructure Concepts

> Reference for terms used in TickSense's liquidity and analytics layer.
> 
> 中文文档：[点击这里](./MARKET_CONCEPTS_ZH.md)

---

## Bid / Ask / Mid Price

The order book always has two sides:

- **Best Bid** (`best_bid_price`): the highest price any buyer is currently willing to pay.
- **Best Ask** (`best_ask_price`): the lowest price any seller is currently willing to accept.
- **Mid Price** (`mid_price`): the arithmetic average of the two — `(bid + ask) / 2`. Used as a fair-value reference that is neutral to either side.

A trade executes when a buyer and seller agree: the buyer pays the ask, the seller receives the bid. The difference between them is profit for the market maker.

---

## Spread

The **spread** is the gap between the best ask and best bid:

```
spread = best_ask_price - best_bid_price
```

It represents the **cost of immediate execution**: if you buy at the ask and sell at the bid right away, you lose exactly one spread. A narrow spread means the asset is liquid and easy to trade cheaply; a wide spread means the market is illiquid or uncertain and entering/exiting a position is expensive.

---

## bps (Basis Points)

**1 bps = 0.01% = 0.0001**

Basis points express small relative changes in a way that avoids the ambiguity of percentages. In finance, "a 1% change in 1%" is confusing — "1 basis point" is not.

The **spread in bps** normalises the raw spread against the mid price:

```
spread_bps = (spread / mid_price) × 10,000
```

This makes spreads comparable across assets of very different prices. A $14 spread on a $78,000 BTC is only **1.8 bps** — tighter than a $0.04 spread on a $2,181 ETH which is **0.18 bps** (ETH is actually more liquid in relative terms).

| spread_bps | typical interpretation |
|---|---|
| < 1 | extremely liquid (major pairs on top exchanges) |
| 1 – 5 | liquid, normal for large-cap crypto |
| 5 – 20 | moderate; watch for slippage on large orders |
| > 20 | illiquid or stressed market |

---

## Order Book Imbalance

Imbalance measures the **asymmetry of supply vs demand** at the top of the order book:

```
imbalance = (best_bid_qty - best_ask_qty) / (best_bid_qty + best_ask_qty)
```

Range: **−1 to +1**

| value | meaning |
|---|---|
| +1.0 | all quoted volume is on the buy side; zero supply |
| +0.5 | bids are 3× larger than asks |
|  0.0 | perfectly balanced |
| −0.5 | asks are 3× larger than bids |
| −1.0 | all quoted volume is on the sell side; zero demand |

Imbalance is a **leading microstructure signal**: the side with more volume is more "committed" and price tends to move toward the thinner side to find liquidity. A strongly positive imbalance often precedes a short-term price uptick; strongly negative often precedes a dip.

It is a signal, not a guarantee — informed traders can and do spoof (place large visible orders they intend to cancel).

---

## Buy Pressure / Sell Pressure

`market_signal` is a categorical label derived from the imbalance value:

```
imbalance > +0.2  →  BUY_PRESSURE
imbalance < −0.2  →  SELL_PRESSURE
otherwise         →  NEUTRAL
```

It summarises the current order book posture in human-readable form. In TickSense it is computed in the `int_order_book_imbalance` dbt model and exposed via the `/liquidity/{symbol}` API endpoint.

**Buy Pressure** does not mean "price will go up." It means buyers are currently more aggressively quoted than sellers at the top of the book — a short-term directional lean, useful as one input among many.

---

## Staleness / Freshness

`staleness_seconds` is how many seconds have elapsed since the last market event was ingested:

```
staleness_seconds = current_timestamp − latest_event_ts
```

`freshness_status` is a derived label:

| status | staleness |
|---|---|
| `FRESH` | < 30 seconds |
| `STALE` | 30 – 120 seconds |
| `DEAD` | > 120 seconds |

For a system targeting < 30s end-to-end latency, any symbol with `STALE` or `DEAD` status indicates a pipeline problem worth investigating.

---

## Putting It Together

A healthy, liquid market looks like:

| metric | healthy value |
|---|---|
| `spread_bps` | < 5 bps |
| `staleness_seconds` | < 30 s |
| `freshness_status` | `FRESH` |
| `health_score` | 1.0 |
| `imbalance` | near 0 (balanced) |
