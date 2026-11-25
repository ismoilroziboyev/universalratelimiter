algorithms.token_bucket = function(rule, actual, now, state)
    local tokens = rule.limit
    if state.expires > actual then
        local refilled = (now - state.last) * rule.rate
        tokens = min(rule.limit, state.value + refilled)
    end
    local reset = now + ceil((rule.limit - tokens) / rule.rate)
    if tokens < rule.cost then
        local retry = now - actual + max(1, ceil((rule.cost - tokens) / rule.rate))
        local denied = result(floor(tokens), retry, reset)
        return denied, denied
    end
    local before = result(floor(tokens), 0, reset)
    state.value = tokens - rule.cost
    state.last = now
    state.expires = now + max(1, ceil((rule.limit - state.value) / rule.rate))
    return result(floor(state.value), 0, state.expires), before
end
