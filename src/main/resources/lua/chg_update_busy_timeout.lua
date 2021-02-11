---
--- KEYS[1]: busyKey
--- ARGV[1]: TimeoutTimeMillis
--- ARGV[2]: CHG
---

redis.call('ZADD', KEYS[1], 'XX', ARGV[1], ARGV[2])
return redis.call('ZSCORE', KEYS[1], ARGV[2])
