// Mixed API load through Envoy (Experiment 003).
//
//   k6 run -e BASE=http://localhost:10000 -e RATE=300 -e DURATION=30s load-tests/api-mixed.js
//
// Players MIN_ID..MAX_ID must exist (experiments/003-week3-resilience/seed.sql).
// HOT=<id> sends every profile read to one player (cache stampede scenario).
// 402/409/429 are expected business outcomes, not failures.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:10000';
const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '30s';
const MIN_ID = Number(__ENV.MIN_ID || 1);
const MAX_ID = Number(__ENV.MAX_ID || 1000);
const HOT = Number(__ENV.HOT || 0);

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 402, 409, 429));

const cacheHits = new Counter('cache_hits');
const rateLimited = new Counter('rate_limited');
const unavailable = new Counter('status_5xx');

const EPS = ['profile', 'history', 'cosmetics', 'presence', 'purchase'];
const loose = ['p(99)<60000']; // declared only so per-endpoint submetrics appear in the summary

export const options = {
  scenarios: {
    mixed: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 1000,
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: Object.assign(
    { http_req_failed: ['rate<0.01'] },
    ...EPS.map((e) => ({ [`http_req_duration{name:${e}}`]: loose, [`http_req_failed{name:${e}}`]: ['rate<1'] })),
  ),
};

const pick = () => HOT || MIN_ID + Math.floor(Math.random() * (MAX_ID - MIN_ID + 1));

export default function () {
  const id = pick();
  const r = Math.random();
  let res;
  if (HOT || r < 0.70) {
    res = http.get(`${BASE}/v1/players/${id}`, { tags: { name: 'profile' }, timeout: '5s' });
    if (res.headers['X-Served-By'] === 'cache') cacheHits.add(1);
  } else if (r < 0.80) {
    res = http.get(`${BASE}/v1/players/${id}/matches?limit=20`, { tags: { name: 'history' }, timeout: '5s' });
  } else if (r < 0.88) {
    res = http.get(`${BASE}/v1/cosmetics`, { tags: { name: 'cosmetics' }, timeout: '5s' });
  } else if (r < 0.95) {
    res = http.put(`${BASE}/v1/players/${id}/presence`, null, { tags: { name: 'presence' }, timeout: '5s' });
  } else {
    // One key per (player, item): repeats are idempotent replays.
    const item = 1 + Math.floor(Math.random() * 3);
    res = http.post(`${BASE}/v1/purchases`, JSON.stringify({ player_id: id, cosmetic_id: item }), {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `k6-${id}-${item}` },
      tags: { name: 'purchase' }, timeout: '5s',
    });
  }
  if (res.status === 429) rateLimited.add(1);
  if (res.status >= 500 || res.status === 0) unavailable.add(1);
  check(res, { 'no 5xx/transport error': (x) => x.status > 0 && x.status < 500 });
}

export function handleSummary(data) {
  const m = data.metrics;
  const v = (k, f) => (m[k] ? m[k].values[f] : undefined);
  const round = (x) => (x === undefined ? null : Math.round(x * 100) / 100);
  const out = {
    base: BASE, rate: RATE, duration: DURATION,
    requests: v('http_reqs', 'count'),
    failed_rate: round(v('http_req_failed', 'rate')),
    status_5xx: v('status_5xx', 'count') || 0,
    rate_limited: v('rate_limited', 'count') || 0,
    cache_hits: v('cache_hits', 'count') || 0,
    dropped_iterations: v('dropped_iterations', 'count') || 0,
    p50: round(v('http_req_duration', 'p(50)')), p95: round(v('http_req_duration', 'p(95)')),
    p99: round(v('http_req_duration', 'p(99)')), max: round(v('http_req_duration', 'max')),
    endpoints: {},
  };
  for (const e of EPS) {
    const d = `http_req_duration{name:${e}}`;
    out.endpoints[e] = { p50: round(v(d, 'p(50)')), p95: round(v(d, 'p(95)')), p99: round(v(d, 'p(99)')),
      failed: round(v(`http_req_failed{name:${e}}`, 'rate')) };
  }
  const res = { stdout: JSON.stringify(out) + '\n' };
  if (__ENV.SUMMARY) res[__ENV.SUMMARY] = JSON.stringify(out, null, 1);
  return res;
}
