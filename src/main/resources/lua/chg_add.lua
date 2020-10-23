---
--- KEYS[1]: chgKey
--- KEYS[2]: waitKey
--- KEYS[3]: crawlExecutionIdCountKey
--- KEYS[4]: queueCountTotalKey
--- ARGV[1]: nextFetchTimeFieldName
--- ARGV[2]: nextReadyTime
--- ARGV[3]: uriCountFieldName
--- ARGV[4]: crawlExecutionId
---

local changes = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])

--- Increment chg queue count
redis.call('HINCRBY', KEYS[1], ARGV[3], 1)

--- Increment crawl execution queue count
redis.call('HINCRBY', KEYS[3], ARGV[4], 1)

--- Increment total queue count
redis.call('INCR', KEYS[4])

--- If new chg was created, queue it.
if changes > 0 then
    redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])
end
