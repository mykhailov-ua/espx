-- KEYS[1..11]: rate, dup, budget, idempotency, campaign/customer sync, dirty sets,
-- stream, daily spend, fcap. ARGV mapping in unified_filter.go.
-- Returns: -1 budget miss (Go reloads PG), 0 success/idempotent, 1 rate, 2 dup, 3 budget, 4 pacing, 5 fcap.

local batch = redis.call("MGET", KEYS[3], KEYS[4], KEYS[10], KEYS[11])
local b = batch[1]
local idem_exists = batch[2]
local daily_spent_raw = batch[3]
local fcap_raw = batch[4]

if not b then
    return -1
end

if idem_exists then
    return 0
end

local budget = tonumber(b) or 0
local amount = tonumber(ARGV[4]) or 0
local freq_limit = tonumber(ARGV[18]) or 0
local user_id = ARGV[17] or ""

if budget < amount then
    return 3
end

if ARGV[14] == "1" then
    local daily_spent = tonumber(daily_spent_raw or 0)
    local daily_limit = tonumber(ARGV[15]) or 0
    local hour_num = tonumber(ARGV[16]) or 24
    local cumulative_limit = math.floor((daily_limit * hour_num) / 24)

    if daily_spent + amount > cumulative_limit then
        return 4
    end
end

if freq_limit > 0 and user_id ~= "" then
    local current_fcap = tonumber(fcap_raw or 0)
    if current_fcap >= freq_limit then
        return 5
    end
end

local rl_max = tonumber(ARGV[2]) or 0
if rl_max > 0 then
    local rl_count = redis.call("INCR", KEYS[1])
    if rl_count == 1 then
        redis.call("EXPIRE", KEYS[1], ARGV[1])
    end
    if rl_count > rl_max then
        return 1
    end
end

local is_dup = redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[3])
if not is_dup then
    return 2
end

redis.call("INCRBY", KEYS[3], -amount)
local c_sync = redis.call("INCRBY", KEYS[5], amount)
if c_sync == amount then
    redis.call("SADD", KEYS[7], ARGV[6]) -- first increment only; avoids repeated SADD on hot keys
end

local cust_sync = redis.call("INCRBY", KEYS[6], amount)
if cust_sync == amount then
    redis.call("SADD", KEYS[8], ARGV[7])
end

redis.call("SET", KEYS[4], "1", "EX", ARGV[5])

if ARGV[14] == "1" then
    local ds = redis.call("INCRBY", KEYS[10], amount)
    if ds == amount then
        redis.call("EXPIRE", KEYS[10], 172800)
    end
end

if freq_limit > 0 and user_id ~= "" then
    local new_fcap = redis.call("INCR", KEYS[11])
    if new_fcap == 1 then
        redis.call("EXPIRE", KEYS[11], tonumber(ARGV[19]))
    end
end

redis.call("XADD", KEYS[9], "MAXLEN", "~", ARGV[8], "*",
    "click_id", ARGV[9],
    "campaign_id", ARGV[6],
    "user_id", user_id,
    "type", ARGV[10],
    "payload", ARGV[11],
    "ip", ARGV[12],
    "ua", ARGV[13]
)

return 0
