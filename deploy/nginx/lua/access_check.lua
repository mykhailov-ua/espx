local redis = require "resty.redis"
local cjson = require "cjson.safe"
local bit = require "bit"

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

local function decode_varint(data, pos)
    local val = 0
    local shift = 0
    local len = #data
    if pos > len then return nil, nil end
    while pos <= len do
        local b = string.byte(data, pos)
        if not b then return nil, nil end
        pos = pos + 1
        val = val + bit.lshift(bit.band(b, 0x7f), shift)
        if bit.band(b, 0x80) == 0 then
            return val, pos
        end
        shift = shift + 7
        if shift >= 35 then return nil, nil end
    end
    return nil, nil
end

local function bytes_to_uuid_string(b)
    if not b or #b ~= 16 then return b end
    return string.format("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        string.byte(b, 1), string.byte(b, 2), string.byte(b, 3), string.byte(b, 4),
        string.byte(b, 5), string.byte(b, 6),
        string.byte(b, 7), string.byte(b, 8),
        string.byte(b, 9), string.byte(b, 10),
        string.byte(b, 11), string.byte(b, 12), string.byte(b, 13), string.byte(b, 14), string.byte(b, 15), string.byte(b, 16))
end

local function parse_proto(body)
    if not body or #body == 0 then return nil, nil end
    local pos = 1
    local len = #body
    local campaign_id, user_id
    while pos <= len do
        local tag_b = string.byte(body, pos)
        if not tag_b then break end
        pos = pos + 1
        local wire = bit.band(tag_b, 0x07)
        local field = bit.rshift(tag_b, 3)
        if wire == 0 then
            local _, next_pos = decode_varint(body, pos)
            if not next_pos then break end
            pos = next_pos
        elseif wire == 1 then
            pos = pos + 8
        elseif wire == 2 then
            local field_len, new_pos = decode_varint(body, pos)
            if not field_len or new_pos + field_len - 1 > len then break end
            pos = new_pos
            if field == 1 then
                campaign_id = string.sub(body, pos, pos + field_len - 1)
                pos = pos + field_len
            elseif field == 3 then
                local sub_end = pos + field_len - 1
                local sub_pos = pos
                while sub_pos < sub_end do
                    local sub_tag = string.byte(body, sub_pos)
                    if not sub_tag then break end
                    sub_pos = sub_pos + 1
                    local sub_wire = bit.band(sub_tag, 0x07)
                    local sub_field = bit.rshift(sub_tag, 3)
                    if sub_wire == 0 then
                        local _, next_sub_pos = decode_varint(body, sub_pos)
                        if not next_sub_pos then break end
                        sub_pos = next_sub_pos
                    elseif sub_wire == 1 then
                        sub_pos = sub_pos + 8
                    elseif sub_wire == 2 then
                        local sub_len, next_sub_pos = decode_varint(body, sub_pos)
                        if not sub_len or next_sub_pos + sub_len - 1 > sub_end then break end
                        sub_pos = next_sub_pos
                        if sub_field == 2 then
                            user_id = string.sub(body, sub_pos, sub_pos + sub_len - 1)
                        end
                        sub_pos = sub_pos + sub_len
                    elseif sub_wire == 5 then
                        sub_pos = sub_pos + 4
                    else
                        break
                    end
                end
                pos = pos + field_len
            else
                pos = pos + field_len
            end
        elseif wire == 5 then
            pos = pos + 4
        else
            break
        end
    end
    return campaign_id, user_id
end

local body = nil
local read_ok, read_err = pcall(ngx.req.read_body)
if read_ok then
    body = ngx.req.get_body_data()
    if not body then
        local filename = ngx.req.get_body_file()
        if filename then
            local fh = io.open(filename, "rb")
            if fh then
                body = fh:read("*a")
                fh:close()
            end
        end
    end
else
    ngx.log(ngx.ERR, "failed to read body: ", read_err)
end

local campaign_id, user_id
if body and #body > 0 then
    local headers = ngx.req.get_headers()
    local ct = headers["content-type"] or ""
    if string.find(ct, "application/json", 1, true) then
        local data = cjson.decode(body)
        if data then
            campaign_id = data.campaign_id
            user_id = data.user_id
        end
    else
        local raw_camp, raw_user = parse_proto(body)
        if raw_camp then
            campaign_id = bytes_to_uuid_string(raw_camp)
        end
        user_id = raw_user
    end
end

local composite_key = ""
if campaign_id and campaign_id ~= "" then
    composite_key = composite_key .. campaign_id
end
if user_id and user_id ~= "" then
    composite_key = composite_key .. user_id
end

if composite_key == "" then
    composite_key = client_ip
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
    local hash = ngx.crc32_long(composite_key)
    shard_idx = (hash % #shards) + 1
end
local target_shard = shards[shard_idx]

local red = redis:new()
red:set_timeout(100)

local ok, err = red:connect(target_shard.host, target_shard.port)
if not ok then
    circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "failed to connect to redis shard ", target_shard.host, ":", target_shard.port, " error: ", err)
    return
end

if REDIS_PASS ~= "" then
    local res, err = red:auth(REDIS_PASS)
    if not res then
        circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
        ngx.log(ngx.ERR, "failed to auth redis shard ", target_shard.host, ":", target_shard.port, " error: ", err)
        return
    end
end

local is_manual, err = red:sismember("blacklist:manual", client_ip)
if err then
    circuit_dict:incr(bucket_curr .. ":errs", 1, 0, 30)
    ngx.log(ngx.ERR, "redis sismember blacklist:manual failed on shard ", target_shard.host, ":", target_shard.port, " error: ", err)
    return
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
    return
end

if is_auto == 1 then
    blacklist_cache:set(client_ip, "b", 300)
    red:set_keepalive(10000, 1024)
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

blacklist_cache:set(client_ip, "c", 300)
red:set_keepalive(10000, 1024)
