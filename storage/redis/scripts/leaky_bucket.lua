algorithms.leaky_bucket = function(rule, actual, now, state)
    local drained = (now - state.last) * rule.rate
    local level = max(0, state.value - drained)
    local available = rule.limit - level
    local reset = level > 0 and now + ceil(level / rule.rate) or 0
    if available < rule.cost then
        local retry = now - actual + max(1, ceil((rule.cost - available) / rule.rate))
        local denied = result(floor(available), retry, reset)
        return denied, denied
    end
    state.value = level + rule.cost
    state.last = now
    state.expires = now + max(1, ceil(state.value / rule.rate))
    return result(floor(rule.limit - state.value), 0, state.expires), result(floor(available), 0, reset)
end
