---
--- KEYS[1]: busyKey
--- KEYS[2]: readyKey
--- ARGV[1]: currentTimeMillis
--- ARGV[2]: CHG_PREFIX
---

-- List all busy chg which has timeout time before current time
local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local moved = 0
for _, key in ipairs(res) do
    -- Remove chg from busyKey
    redis.call('ZREM', KEYS[1], key)
    local chgpKey = ARGV[2] .. key
    -- Decrement queue count from chgpKey because when chg is busy one is added to queue count to avoid deleteion
    if redis.call('DECR', chgpKey) <= 0 then
        -- If queue count is zero there is no need for chg, remove it.
        redis.call('DEL', chgpKey)
    else
        -- Otherwise add chg to readyKey
        redis.call('RPUSH', KEYS[2], key)
    end
    moved = moved + 1
end
return moved