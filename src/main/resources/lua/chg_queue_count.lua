---
--- KEYS[1]: chgpKey
--- KEYS[2]: busyKey
--- ARGV[1]: chgp
---

local count = 0
local c = redis.call('GET', KEYS[1]);
if c ~= 'nil' then
    count = c
    local isBusy = redis.call('ZRANK', KEYS[2], ARGV[1])
    if isBusy ~= 'nil' then
        count = count - 1
    end
end
return count
