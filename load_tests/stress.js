import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { target: 100, duration: '1m' },      // Ramp up
        { target: 1000, duration: '5m' },     // High load
        { target: 2000, duration: '2m' },     // Peak load
        { target: 0, duration: '30s' },       // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<100'],     // 95% of requests must complete below 100ms
        http_req_failed: ['rate<0.01'],       // Error rate must be less than 1%
    },
};

export default function () {
    const payload = JSON.stringify({
        campaign_id: '550e8400-e29b-41d4-a716-446655440000',
        type: 'impression',
        click_id: `clk_${Math.random().toString(36).substring(7)}`,
        payload: { source: 'k6-stress-test' },
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post('http://localhost:8080/track', payload, params);

    check(res, {
        'status is 202': (r) => r.status === 202,
    });

    sleep(0.1);
}
