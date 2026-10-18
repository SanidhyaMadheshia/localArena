// Retry storm: naive client retries vs exponential backoff + jitter + retry budget.
//
//   k6 run -e MODE=naive   load-tests/retry-storm.js
//   k6 run -e MODE=backoff load-tests/retry-storm.js
//
// Each iteration is one *logical* request (GET history) at a fixed arrival
// rate. naive: up to 5 immediate retries on any failure or 2 s client timeout.
// backoff: up to 3 retries, full-jitter exponential backoff (50 ms * 2^n, cap
// 1 s), honours Retry-After, never retries 4xx, and spends tokens from a
// per-client retry budget (10% of requests, burst 10) so retries cannot exceed
// a fixed fraction of load.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:10000';
const MODE = __ENV.MODE || 'naive';
const RATE = Number(__ENV.RATE || 100);
const DURATION = __ENV.DURATION || '30s';
const MIN_ID = Number(__ENV.MIN_ID || 1);
const MAX_ID = Number(__ENV.MAX_ID || 1000);

const attempts = new Counter('attempts');
const retries = new Counter('retries');
const budgetDenied = new Counter('retry_budget_denied');
const logicalOK = new Rate('logical_success');
const logicalDur = new Trend('logical_duration', true);

export const options = {
  scenarios: {
    storm: { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION, preAllocatedVUs: 100, maxVUs: 2000 },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

let budget = 10; // per-VU token bucket

export default function () {
  const id = MIN_ID + Math.floor(Math.random() * (MAX_ID - MIN_ID + 1));
  const url = `${BASE}/v1/players/${id}/matches?limit=20`;
  const start = Date.now();
  const max = MODE === 'naive' ? 6 : 4;
  budget = Math.min(10, budget + 0.1);
  let ok = false;
  for (let i = 0; i < max; i++) {
    if (i > 0) {
      if (MODE === 'backoff') {
        if (budget < 1) { budgetDenied.add(1); break; }
        budget -= 1;
      }
      retries.add(1);
    }
    attempts.add(1);
    const r = http.get(url, { timeout: '2s', tags: { name: 'history' } });
    if (r.status === 200) { ok = true; break; }
    if (MODE !== 'backoff') continue;
    if (r.status >= 400 && r.status < 500 && r.status !== 429) break;
    const ra = Number(r.headers['Retry-After']);
    const backoff = Math.random() * Math.min(1000, 50 * 2 ** i); // full jitter
    sleep(Math.max(backoff, ra > 0 ? ra * 1000 * Math.random() : 0) / 1000);
  }
  logicalOK.add(ok);
  logicalDur.add(Date.now() - start);
}

export function handleSummary(data) {
  const m = data.metrics;
  const v = (k, f) => (m[k] ? m[k].values[f] : 0);
  const secs = parseFloat(DURATION);
  const logical = v('logical_success', 'passes') + v('logical_success', 'fails');
  const out = {
    mode: MODE, rate: RATE, duration: DURATION,
    logical_requests: logical,
    logical_success_rate: Math.round(v('logical_success', 'rate') * 1000) / 1000,
    goodput_per_s: Math.round(v('logical_success', 'passes') / secs),
    attempts: v('attempts', 'count'),
    amplification: Math.round((v('attempts', 'count') / Math.max(logical, 1)) * 100) / 100,
    offered_attempts_per_s: Math.round(v('attempts', 'count') / secs),
    retry_budget_denied: v('retry_budget_denied', 'count'),
    dropped_iterations: v('dropped_iterations', 'count'),
    logical_p50_ms: Math.round(v('logical_duration', 'p(50)')),
    logical_p95_ms: Math.round(v('logical_duration', 'p(95)')),
    logical_p99_ms: Math.round(v('logical_duration', 'p(99)')),
  };
  const res = { stdout: JSON.stringify(out) + '\n' };
  if (__ENV.SUMMARY) res[__ENV.SUMMARY] = JSON.stringify(out, null, 1);
  return res;
}
