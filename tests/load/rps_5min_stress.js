import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        high_rps: {
            executor: 'constant-arrival-rate',
            rate: 20000,
            timeUnit: '1s',
            duration: '5m',
            preAllocatedVUs: 2000,
            maxVUs: 5000,
        },
    },
    discardResponseBodies: true,
    thresholds: {
        http_req_failed: ['rate<0.05'], // Max 5% errors
    },
};

const url = 'http://nginx:80/track';
const payload = JSON.stringify({
    campaign_id: '123e4567-e89b-12d3-a456-426614174000',
    type: 'click',
    click_id: 'test-click-id',
    payload: {
        key: 'value'
    }
});

const params = {
    headers: {
        'Content-Type': 'application/json',
    },
};

export default function () {
    const res = http.post(url, payload, params);
    check(res, {
        'status is 202': (r) => r.status === 202,
    });
}
