local function counter_offset(count, budget, window)
    return ceil(((count - budget) * window) / count)
end

algorithms.sliding_window_counter = function(rule, actual, now, state)
    local start = now - now % rule.window
    if start ~= state.start then
        if start - state.start == rule.window then
            state.previous, state.count = state.count, 0
        else
            state.previous, state.count = 0, 0
        end
    end
    local offset = now - start
    local weighted = (state.previous * (rule.window - offset)) / rule.window
    local remaining = max(0, rule.limit - state.count - ceil(weighted))
    local reset = 0
    if state.count > 0 then
        reset = start + 2 * rule.window
    elseif state.previous > 0 then
        reset = start + rule.window
    end
    if remaining < rule.cost then
        local budget = rule.limit - rule.cost
        local ready
        if state.count <= budget then
            ready = start + counter_offset(state.previous, budget - state.count, rule.window)
        else
            ready = start + rule.window + counter_offset(state.count, budget, rule.window)
        end
        local denied = result(remaining, max(now + 1, ready) - actual, reset)
        return denied, denied
    end
    state.start, state.count = start, state.count + rule.cost
    state.last, state.expires = now, start + 2 * rule.window
    return result(remaining - rule.cost, 0, state.expires), result(remaining, 0, reset)
end
