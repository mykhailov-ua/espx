import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        high_rps: {
            executor: 'constant-arrival-rate',
            rate: 100000,
            timeUnit: '1s',
            duration: '5m',
            preAllocatedVUs: 3000,
            maxVUs: 10000,
        },
    },
    discardResponseBodies: true,
    thresholds: {
        http_req_failed: ['rate<0.01'],
    },
};

const BASE_URL = 'http://127.0.0.1';

// Minimal Protobuf encoder for AdEvent
// 1: campaign_id (string)
// 2: event_type (string)
function strToBytes(str) {
    const bytes = new Uint8Array(str.length);
    for (let i = 0; i < str.length; i++) {
        bytes[i] = str.charCodeAt(i);
    }
    return bytes;
}

function encodeAdEvent(campaignId, eventType) {
    const campaignIdBytes = strToBytes(campaignId);
    const eventTypeBytes = strToBytes(eventType);

    const totalLen = (1 + 1 + campaignIdBytes.length) + (1 + 1 + eventTypeBytes.length);
    const buf = new Uint8Array(totalLen);
    
    let offset = 0;
    // Tag 1 (campaign_id)
    buf[offset++] = 0x0a;
    buf[offset++] = campaignIdBytes.length;
    buf.set(campaignIdBytes, offset);
    offset += campaignIdBytes.length;

    // Tag 2 (event_type)
    buf[offset++] = 0x12;
    buf[offset++] = eventTypeBytes.length;
    buf.set(eventTypeBytes, offset);
    
    return buf.buffer;
}

export default function () {
    const campaignId = '123e4567-e89b-12d3-a456-426614174000';
    const eventType = 'impression';
    
    const payload = encodeAdEvent(campaignId, eventType);

    const params = {
        headers: {
            'Content-Type': 'application/x-protobuf',
        },
    };

    const res = http.post(`${BASE_URL}/track`, payload, params);

    check(res, {
        'status is 202': (r) => r.status === 202,
    });
}
