import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const BASE_URL = 'http://api:8000';
const SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt'];

export const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '30s', target: 3  },  // warm up
    { duration: '60s', target: 3  },  // baseline
    { duration: '30s', target: 10 },  // ramp to moderate load
    { duration: '60s', target: 10 },  // hold
    { duration: '30s', target: 0  },  // cool down
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],  // 95th percentile under 2s (Trino cold query)
    http_req_failed:   ['rate<0.05'],   // error rate under 5%
    checks:            ['rate>0.95'],   // 95% of assertions must pass
  },
};

function tag(name) {
  return { tags: { name } };
}

export default function () {
  const symbol = SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];

  // ── /health ────────────────────────────────────────────────────────────────
  let res = http.get(`${BASE_URL}/health`, tag('health'));
  check(res, {
    'health 200':      (r) => r.status === 200,
    'health body ok':  (r) => r.json('status') === 'ok',
  });
  errorRate.add(res.status !== 200);
  sleep(0.3);

  // ── /ohlcv/{symbol} ────────────────────────────────────────────────────────
  res = http.get(`${BASE_URL}/ohlcv/${symbol}`, tag('ohlcv'));
  const ohlcvOk = res.status === 200;
  check(res, {
    'ohlcv 200':       (r) => r.status === 200,
    'ohlcv has bars':  (r) => ohlcvOk && r.json('count') > 0,
    'ohlcv interval':  (r) => ohlcvOk && r.json('interval') === '1m',
  });
  errorRate.add(res.status >= 500);  // 404 acceptable (symbol may have no data yet)
  sleep(0.5);

  // ── /spread/{symbol} ───────────────────────────────────────────────────────
  res = http.get(`${BASE_URL}/spread/${symbol}`, tag('spread'));
  const spreadOk = res.status === 200;
  check(res, {
    'spread 200':      (r) => r.status === 200,
    'spread_bps > 0':  (r) => spreadOk && r.json('spread_bps') > 0,
    'mid_price > 0':   (r) => spreadOk && r.json('mid_price') > 0,
  });
  errorRate.add(res.status >= 500);
  sleep(0.5);

  // ── /liquidity/{symbol} ────────────────────────────────────────────────────
  res = http.get(`${BASE_URL}/liquidity/${symbol}`, tag('liquidity'));
  const liqOk = res.status === 200;
  check(res, {
    'liquidity 200':          (r) => r.status === 200,
    'liquidity has signal':   (r) => liqOk && ['BUY_PRESSURE','SELL_PRESSURE','NEUTRAL'].includes(r.json('market_signal')),
    'liquidity freshness':    (r) => liqOk && ['FRESH','WARN','STALE'].includes(r.json('freshness_status')),
  });
  errorRate.add(res.status >= 500);
  sleep(0.5);

  // ── /pipeline/lag ──────────────────────────────────────────────────────────
  res = http.get(`${BASE_URL}/pipeline/lag`, tag('pipeline'));
  check(res, {
    'pipeline 200':          (r) => r.status === 200,
    'pipeline has items':    (r) => r.status === 200 && r.json('total_count') > 0,
    'pipeline all healthy':  (r) => r.status === 200 && r.json('healthy_count') === r.json('total_count'),
  });
  errorRate.add(res.status !== 200);
  sleep(0.3);

  // ── /symbols ───────────────────────────────────────────────────────────────
  res = http.get(`${BASE_URL}/symbols`, tag('symbols'));
  check(res, {
    'symbols 200':       (r) => r.status === 200,
    'symbols count > 0': (r) => r.status === 200 && r.json('count') > 0,
  });
  errorRate.add(res.status !== 200);
  sleep(0.4);
}
