import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

import { clientSession } from './session.js';

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const authURL = __ENV.AUTH_URL || 'http://localhost:8084';
const scenario = __ENV.SCENARIO || 'sustained';
const duration = __ENV.DURATION || '150s';
const thinkTime = Number(__ENV.THINK_TIME || 0);

const unexpectedErrors = new Rate('unexpected_errors');
const rejections = new Rate('rejections');
// Throughput counts only complete answers. http_reqs also holds Auth calls, 429s, and partial pages.
const completeResponses = new Counter('complete_responses');
const completeDuration = new Trend('complete_duration', true);
const partialResponses = new Counter('partial_responses');
const rejectedResponses = new Counter('rejected_responses');
const failedRequests = new Counter('failed_requests');

const mediaToken = clientSession(authURL, __ENV.MEDIA_CLIENT_ID, __ENV.MEDIA_CLIENT_SECRET);
const fieldToken = clientSession(authURL, __ENV.FIELD_TEAM_CLIENT_ID, __ENV.FIELD_TEAM_CLIENT_SECRET);

const sustained = {
  media: {
    executor: 'ramping-vus',
    exec: 'mediaRead',
    startVUs: 0,
    stages: [
      { duration: '30s', target: Number(__ENV.VUS || 10000) },
      { duration, target: Number(__ENV.VUS || 10000) },
    ],
  },
};

const scenarios = {
  // P2 criterion 6: 10,000 users for at least 120 s while scale.sh moves the Aggregator from 3 to 5 to 2 instances.
  sustained,
  // P2 criterion 8: the same load while scale.sh rollout swaps the Aggregator version. No request may fail.
  rollout: sustained,
  // P2 criterion 7: a saturating closed load, run once at one instance and once at three.
  throughput: { media: { executor: 'constant-vus', exec: 'mediaRead', vus: Number(__ENV.VUS || 500), duration } },
  // Latency figures with an open model, so a slow system cannot lower the arrival rate.
  latency: {
    field: {
      executor: 'constant-arrival-rate',
      exec: 'fieldRead',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration,
      preAllocatedVUs: Number(__ENV.VUS || 400),
    },
  },
  // P5 criterion 5: Media floods the Client API while Field Team keeps reading.
  overload: {
    media: { executor: 'constant-vus', exec: 'mediaRead', vus: Number(__ENV.MEDIA_VUS || 2000), duration },
    field: { executor: 'constant-vus', exec: 'fieldRead', vus: Number(__ENV.FIELD_VUS || 50), duration },
  },
};

const thresholds = {
  sustained: { unexpected_errors: ['rate<0.01'] },
  rollout: { failed_requests: ['count==0'] },
  throughput: { unexpected_errors: ['rate<0.01'] },
  latency: { unexpected_errors: ['rate<0.01'] },
  overload: { 'rejections{class:field}': ['rate<0.01'], 'rejections{class:media}': ['rate>0'] },
};

if (!scenarios[scenario]) {
  throw new Error(`unknown SCENARIO ${scenario}`);
}

export const options = {
  scenarios: scenarios[scenario],
  thresholds: thresholds[scenario],
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

export function fieldRead() {
  read(fieldToken, 'field');
}

export function handleSummary(data) {
  const seconds = data.state.testRunDurationMs / 1000;
  const complete = metric(data, 'complete_responses', 'count');
  const lines = [
    `M2 ${scenario} run, ${seconds.toFixed(0)} s, up to ${metric(data, 'vus_max', 'max')} VUs`,
    `Throughput ${(complete / seconds).toFixed(1)} complete 200/s over ${complete} complete responses`,
    `Complete 200 p95 ${metric(data, 'complete_duration', 'p(95)').toFixed(1)} ms, p99 ${metric(data, 'complete_duration', 'p(99)').toFixed(1)} ms`,
    `Partial 200 ${metric(data, 'partial_responses', 'count')}, 429 ${metric(data, 'rejected_responses', 'count')}, failed ${metric(data, 'failed_requests', 'count')}`,
    `Unexpected errors ${(metric(data, 'unexpected_errors', 'rate') * 100).toFixed(2)}%, target below 1%`,
    `Rejections ${(metric(data, 'rejections', 'rate') * 100).toFixed(2)}%, auth failures ${metric(data, 'auth_failures', 'count')}`,
    '',
  ];
  const out = { stdout: lines.join('\n') };

  if (__ENV.SUMMARY_JSON) {
    out[__ENV.SUMMARY_JSON] = JSON.stringify(data, null, 2);
  }

  return out;
}

export function mediaRead() {
  read(mediaToken, 'media');
}

function metric(data, name, stat) {
  const m = data.metrics[name];

  return m && m.values[stat] !== undefined ? m.values[stat] : 0;
}

function read(token, cls) {
  const access = token();
  if (!access) {
    unexpectedErrors.add(true, { class: cls });
    failedRequests.add(1, { class: cls });
    sleep(1);

    return;
  }

  const res = http.get(`${baseURL}/hazards?limit=20`, {
    headers: { Authorization: `Bearer ${access}`, 'X-Correlation-ID': `k6-${cls}-${__VU}-${__ITER}` },
    tags: { class: cls, name: 'GET /hazards' },
  });

  // A 429 is a controlled rejection, so it counts as a rejection and not as an error.
  rejections.add(res.status === 429, { class: cls });

  if (res.status === 429) {
    rejectedResponses.add(1, { class: cls });
  } else {
    unexpectedErrors.add(res.status !== 200, { class: cls });
  }

  if (res.status === 200) {
    // The Client API writes compact JSON, so a substring check avoids parsing every body.
    if (res.body && res.body.includes('"partial":true')) {
      partialResponses.add(1, { class: cls });
    } else {
      completeResponses.add(1, { class: cls });
      completeDuration.add(res.timings.duration, { class: cls });
    }
  } else if (res.status !== 429) {
    failedRequests.add(1, { class: cls });
  }

  sleep(thinkTime);
}
