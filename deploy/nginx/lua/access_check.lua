local redis = require "resty.redis"
local circuit_dict = ngx.shared.circuit_breaker
local blacklist_cache = ngx.shared.blacklist_cache

local REDIS_HOST = os.getenv("REDIS_HOST") or "127.0.0.1"
local REDIS_PORT = os.getenv("REDIS_PORT") or 6379
local REDIS_PASS = os.getenv("REDIS_PASS") or ""
local REDIS_ADDRS = os.getenv("REDIS_ADDRS") or ""
local FAIL_THRESHOLD = 0.95
local SAMPLE_WINDOW = 100

local now = ngx.time()
local bucket_curr = math.floor(now / 10)
local bucket_prev = bucket_curr - 1

circuit_dict:incr(bucket_curr .. ":total", 1, 0, 30)

local total_curr = circuit_dict:get(bucket_curr .. ":total") or 0
local total_prev = circuit_dict:get(bucket_prev .. ":total") or 0
local total_reqs = total_curr + total_prev

local errs_curr = circuit_dict:get(bucket_curr .. ":errs") or 0
local errs_prev = circuit_dict:get(bucket_prev .. ":errs") or 0
local redis_errs = errs_curr + errs_prev

if total_reqs > SAMPLE_WINDOW then
    if (redis_errs / total_reqs) > FAIL_THRESHOLD then
        ngx.log(ngx.ERR, "Edge Circuit Breaker OPEN: fail rate ", (redis_errs / total_reqs))
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end
end

local client_ip = ngx.var.remote_addr

local cached_status = blacklist_cache:get(client_ip)
if cached_status == "b" then
    ngx.exit(ngx.HTTP_FORBIDDEN)
elseif cached_status == "c" then
    return
end

local shards = {}
if REDIS_ADDRS ~= "" then
    for addr in string.gmatch(REDIS_ADDRS, "([^,]+)") do
        local host, port = string.match(addr, "([^:]+):(%d+)")
        if host and port then
            table.insert(shards, {host = host, port = tonumber(port)})
        end
    end
end

if #shards == 0 then
    table.insert(shards, {host = REDIS_HOST, port = tonumber(REDIS_PORT)})
end

local shard_idx = 1
if #shards > 1 then
    local hash = ngx.crc32_long(client_ip)
    shard_idx = (hash % #shards) + 1
end
local target_shard = shards[shard_idx]

local red = redis:new()
red:set_timeout(100)

local ok, err = red:connect(target_shard.host, target_shard.port)
if not ok then
    circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "failed to connect to redis shard ", target_shard.host, ":", target_shard.port, " error: ", err)
    return -- fail-open
end

if REDIS_PASS ~= "" then
    local res, err = red:auth(REDIS_PASS)
    if not res then
        circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
        ngx.log(ngx.ERR, "failed to auth redis shard ", target_shard.host, ":", target_shard.port, " error: ", err)
        return -- fail-open
    end
end

local is_manual, err = red:sismember("blacklist:manual", client_ip)
if err then
    circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "redis sismember blacklist:manual failed on shard ", target_shard.host, ":", target_shard.port, " error: ", err)
    return -- fail-open
end

if is_manual == 1 then
    blacklist_cache:set(client_ip, "b", 300)
    red:set_keepalive(10000, 1024)
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

local is_auto, err = red:sismember("blacklist:auto", client_ip)
if err then
    circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "redis sismember blacklist:auto failed on shard ", target_shard.host, ":", target_shard.port, " error: ", err)
    return -- fail-open
end

if is_auto == 1 then
    blacklist_cache:set(client_ip, "b", 300)
    red:set_keepalive(10000, 1024)
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

blacklist_cache:set(client_ip, "c", 300)
red:set_keepalive(10000, 1024)
