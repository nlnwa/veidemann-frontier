---
--- KEYS[1]: fromQueueKey
--- KEYS[2]: toQueueKey
--- ARGV[1]: currentTimeMillis
--- ARGV[2]: CHG_PREFIX
---

local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local moved = 0
for _, key in ipairs(res) do
    redis.call('ZREM', KEYS[1], key)
    redis.call('RPUSH', KEYS[2], key)
    redis.call('DECR', ARGV[2] .. key)
    moved = moved + 1
end
return moved