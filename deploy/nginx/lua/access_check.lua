local redis = require "resty.redis"
local shared_dict = ngx.shared.circuit_breaker

-- Configuration
local REDIS_HOST = os.getenv("REDIS_HOST") or "127.0.0.1"
local REDIS_PORT = os.getenv("REDIS_PORT") or 6379
local REDIS_PASS = os.getenv("REDIS_PASS") or ""
local FAIL_THRESHOLD = 0.95 -- 95%
local SAMPLE_WINDOW = 100

-- Stats tracking (10-second sliding window over 2 buckets)
local now = ngx.time()
local bucket_curr = math.floor(now / 10)
local bucket_prev = bucket_curr - 1

shared_dict:incr(bucket_curr .. ":total", 1, 0, 30)

local total_curr = shared_dict:get(bucket_curr .. ":total") or 0
local total_prev = shared_dict:get(bucket_prev .. ":total") or 0
local total_reqs = total_curr + total_prev

local errs_curr = shared_dict:get(bucket_curr .. ":errs") or 0
local errs_prev = shared_dict:get(bucket_prev .. ":errs") or 0
local redis_errs = errs_curr + errs_prev

-- Circuit Breaker Logic
if total_reqs > SAMPLE_WINDOW then
    if (redis_errs / total_reqs) > FAIL_THRESHOLD then
        ngx.log(ngx.ERR, "Edge Circuit Breaker OPEN: fail rate ", (redis_errs / total_reqs))
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end
end

-- IP Blacklist Check
local red = redis:new()
red:set_timeout(100) -- 100ms

local ok, err = red:connect(REDIS_HOST, REDIS_PORT)
if not ok then
    shared_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "failed to connect to redis: ", err)
    return -- Fail-open
end

if REDIS_PASS ~= "" then
    local res, err = red:auth(REDIS_PASS)
    if not res then
        shared_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
        return -- Fail-open
    end
end

local client_ip = ngx.var.remote_addr

-- Check manual blacklist
local is_manual, err = red:sismember("blacklist:manual", client_ip)
if is_manual == 1 then
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

-- Check auto blacklist (or Bloom filter)
local is_auto, err = red:sismember("blacklist:auto", client_ip)
if is_auto == 1 then
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

-- Put connection back to pool
red:set_keepalive(10000, 100)
