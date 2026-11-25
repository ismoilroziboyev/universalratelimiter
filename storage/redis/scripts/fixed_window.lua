algorithms.fixed_window = function(rule, actual, now, state)
    local start = now - now % rule.window
    if state.start ~= start then state.count = 0 end
    local reset = start + rule.window
    local remaining = rule.limit - state.count
    if remaining < rule.cost then
        local denied = result(remaining, reset - actual, reset)
        return denied, denied
    end
    state.start, state.count = start, state.count + rule.cost
    state.last, state.expires = now, reset
    return result(remaining - rule.cost, 0, reset), result(remaining, 0, reset)
end
