local server_time = redis.call("TIME")
local actual = tonumber(server_time[1]) * 1000 + floor(tonumber(server_time[2]) / 1000)
-- Align with internal/clock.MaxTimestamp and leave room for two long windows.
if actual < 0 or actual > 2^53 - 2 * (365 * 24 * 60 * 60 * 1000) - 1 then
    return redis.error_reply("RATELIMITER_CLOCK")
end

local proposed, previous, values, ttl = {}, {}, {}, {}
local all_allowed = true
for i, key in ipairs(KEYS) do
    local offset = (i - 1) * 5
    local rule = {
        limit = tonumber(ARGV[offset + 2]), window = tonumber(ARGV[offset + 3]),
        rate = tonumber(ARGV[offset + 4]), cost = tonumber(ARGV[offset + 5])
    }
    local evaluate = algorithms[ARGV[offset + 1]]
    local state = decode(redis.call("GET", key))
    if state.expires <= actual then state = decode(false) end
    local now = max(actual, state.last)
    if rule.cost > rule.limit then
        local current = {0, rule.limit, 0, 0}
        if state.expires > actual then
            rule.cost = 1
            local _, before = evaluate(rule, actual, now, state)
            current = {0, before[2], 0, before[4]}
        end
        proposed[i], previous[i] = current, current
    else
        proposed[i], previous[i] = evaluate(rule, actual, now, state)
        if proposed[i][1] == 1 then
            values[i] = encode(state)
            ttl[i] = string.format("%.0f", state.expires - actual)
        end
    end
    if proposed[i][1] == 0 then all_allowed = false end
end

local output = {}
if all_allowed then
    if #KEYS > 1 then
        for i, key in ipairs(KEYS) do
            if not redis.acl_check_cmd("SET", key, values[i], "PX", ttl[i]) then
                return redis.error_reply("RATELIMITER_WRITE_DENIED")
            end
        end
    end
    for i, key in ipairs(KEYS) do
        redis.call("SET", key, values[i], "PX", ttl[i])
    end
end
for _, decision in ipairs(all_allowed and proposed or previous) do
    for _, field in ipairs(decision) do output[#output + 1] = field end
end
return output
