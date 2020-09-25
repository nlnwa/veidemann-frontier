---
--- KEYS[1]: ueIdKey
--- KEYS[2]: uchgKey
---
--- ARGV[1]: ueIdVal
--- ARGV[2]: weight
--- ARGV[3]: eid
---

redis.call('ZADD', KEYS[1], 0, ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
