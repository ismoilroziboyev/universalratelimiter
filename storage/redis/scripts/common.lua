local algorithms = {}
local ceil, floor, max, min = math.ceil, math.floor, math.max, math.min

local function result(remaining, retry, reset)
    return {retry == 0 and 1 or 0, remaining, retry, reset}
end

local function decode(value)
    if not value then
        return {last = 0, expires = 0, start = 0, count = 0, previous = 0, value = 0}
    end
    local stored = cjson.decode(value)
    return {
        last = tonumber(stored[1]), expires = tonumber(stored[2]),
        start = tonumber(stored[3]), count = stored[4], previous = stored[5],
        value = tonumber(stored[6]), log = stored[7]
    }
end

local function encode(state)
    -- Preserve full precision across Lua/Redis round trips.
    return cjson.encode({
        string.format("%.0f", state.last), string.format("%.0f", state.expires),
        string.format("%.0f", state.start), state.count, state.previous,
        string.format("%.17g", state.value), state.log
    })
end
