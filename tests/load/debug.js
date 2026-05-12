import http from 'k6/http';
export default function() {
  const res = http.post('http://nginx/track', JSON.stringify({
        campaign_id: '00000000-0000-0000-0000-000000000001',
        type: 'impression',
        click_id: 'test-click',
    }), {headers:{'Content-Type':'application/json'}});
  console.log('Status: ' + res.status);
  console.log('Error code: ' + res.error_code);
}
