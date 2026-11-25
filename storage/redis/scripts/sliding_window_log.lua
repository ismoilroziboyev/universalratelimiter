algorithms.sliding_window_log = function(rule, actual, now, state)
    local log = state.log or {}
    local first = 1
    while first <= #log and tonumber(log[first][1]) <= now - rule.window do
        state.count = state.count - log[first][2]
        first = first + 1
    end
    local reset = first <= #log and tonumber(log[#log][1]) + rule.window or 0
    local remaining = rule.limit - state.count
    if remaining < rule.cost then
        local available = remaining
        for i = first, #log do
            available = available + log[i][2]
            if available >= rule.cost then
                local denied = result(remaining, tonumber(log[i][1]) + rule.window - actual, reset)
                return denied, denied
            end
        end
    end
    if first > 1 then
        local active = {}
        for i = first, #log do active[#active + 1] = log[i] end
        log = active
    end
    log[#log + 1] = {string.format("%.0f", now), rule.cost}
    state.log = log
    state.count = state.count + rule.cost
    state.last, state.expires = now, now + rule.window
    return result(remaining - rule.cost, 0, state.expires), result(remaining, 0, reset)
end
