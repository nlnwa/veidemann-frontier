---
--- KEYS[1]: busyKey
--- KEYS[2]: chgpKey
--- ARGV[1]: chgp
--- ARGV[2]: busyExpireTime
---

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return redis.call('HGETALL', KEYS[2])
