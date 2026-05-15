-- Unified Filter Lua Script
-- Keys:
-- KEYS[1]: Rate limit key
-- KEYS[2]: Duplicate key
-- KEYS[3]: Budget source key (budget:campaign:{id})
-- KEYS[4]: Idempotency key (for budget specifically)
-- KEYS[5]: Campaign sync key (budget:sync:campaign:{id})
-- KEYS[6]: Customer sync key (budget:sync:customer:{id})
-- KEYS[7]: Dirty campaigns set
-- KEYS[8]: Dirty customers set
-- KEYS[9]: Stream name
-- KEYS[10]: Daily spend key (budget:daily_spent:campaign:{id}:{date})
-- KEYS[11]: Frequency capping key (fcap:c:{cid}:u:{uid})

-- Args:
-- ...
-- ARGV[13]: User Agent
-- ARGV[14]: Is Even Pacing (1 or 0)
-- ARGV[15]: Daily Budget
-- ARGV[16]: Current Hour Number (1-24)
-- ARGV[17]: User ID
-- ARGV[18]: Freq Limit
-- ARGV[19]: Freq Window (seconds)

-- 1. Budget Cache Miss Check
local b = redis.call("GET", KEYS[3])
if not b then
    return -1
end

-- 2. Deduplication
local is_dup = redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[3])
if not is_dup then
    return 2
end

-- 3. Rate Limiting
local rl_count = redis.call("INCR", KEYS[1])
if rl_count == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
if rl_count > tonumber(ARGV[2]) then
    return 1
end

-- 4. Budget Idempotency
if redis.call("EXISTS", KEYS[4]) == 1 then
    return 0 
end

-- 5. Checks (Pacing, Frequency, Budget)
local budget = tonumber(b)
local amount = tonumber(ARGV[4])
local freq_limit = tonumber(ARGV[18])
local user_id = ARGV[17]

if budget < amount then
    return 3
end

if ARGV[14] == "1" then
    local daily_spent = tonumber(redis.call("GET", KEYS[10]) or 0)
    local daily_limit = tonumber(ARGV[15])
    local hour_num = tonumber(ARGV[16])
    local cumulative_limit = (daily_limit / 24) * hour_num
    if hour_num == 24 then cumulative_limit = daily_limit end
    
    if daily_spent + amount > cumulative_limit then
        return 4
    end
end

if freq_limit > 0 and user_id ~= "" then
    local current_fcap = tonumber(redis.call("GET", KEYS[11]) or 0)
    if current_fcap >= freq_limit then
        return 5
    end
end

-- 6. Atomic Updates
redis.call("INCRBYFLOAT", KEYS[3], -amount)
redis.call("INCRBYFLOAT", KEYS[5], amount)
redis.call("INCRBYFLOAT", KEYS[6], amount)
redis.call("SADD", KEYS[7], ARGV[6])
redis.call("SADD", KEYS[8], ARGV[7])
redis.call("SET", KEYS[4], "1", "EX", ARGV[5])

if ARGV[14] == "1" then
    redis.call("INCRBYFLOAT", KEYS[10], amount)
    redis.call("EXPIRE", KEYS[10], 172800)
end

if freq_limit > 0 and user_id ~= "" then
    local new_fcap = redis.call("INCR", KEYS[11])
    if new_fcap == 1 then
        redis.call("EXPIRE", KEYS[11], tonumber(ARGV[19]))
    end
end

-- 7. XADD to Stream
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
