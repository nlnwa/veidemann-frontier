---
--- KEYS[1]: busyKey
--- KEYS[2]: chgpKey
--- ARGV[1]: chgp
--- ARGV[2]: busyExpireTime
---

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
local queueCount = redis.call('GET', KEYS[2])

--- Increment the queue count while CHG is busy to ensure URI removal while busy does not remove CHG
redis.call('INCR', KEYS[2])
return queueCount