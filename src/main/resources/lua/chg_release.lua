---
--- KEYS[1]: busyKey
--- KEYS[2]: waitKey
--- KEYS[3]: chgpKey
--- ARGV[1]: readyTime
--- ARGV[2]: chgp
---

if redis.call('EXISTS', KEYS[3]) == 0 then
    return
end

-- Remove chg from busyKey
redis.call('ZREM', KEYS[1], ARGV[2])
-- Decrement queue count from chgpKey because when chg is busy one is added to queue count to avoid deleteion
if redis.call('DECR', KEYS[3]) <= 0 then
    -- If queue count is zero there is no need for chg, remove it.
    redis.call('DEL', KEYS[3])
else
    -- Otherwise add chg to waitKey
    redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
end
