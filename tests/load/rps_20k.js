import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        high_rps: {
            executor: 'constant-arrival-rate',
            rate: 20000,
            timeUnit: '1s',
            duration: '1m',
            preAllocatedVUs: 2000,
            maxVUs: 5000,
        },
    },
    discardResponseBodies: true, // Crucial for high RPS to save memory
    thresholds: {
        http_req_failed: ['rate<0.05'], // Allow 5% failure for extreme peaks
    },
};

const BASE_URL = 'http://nginx';

export default function () {
    const payload = JSON.stringify({
        campaign_id: '123e4567-e89b-12d3-a456-426614174000',
        type: 'impression',
        click_id: `clk_${Math.random().toString(36).substring(7)}`,
        payload: { source: 'k6-extreme-load' },
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${BASE_URL}/track`, payload, params);

    check(res, {
        'status is 202': (r) => r.status === 202,
    });
}
