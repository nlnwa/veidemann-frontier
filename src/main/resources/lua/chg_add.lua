---
--- KEYS[1]: chgKey
--- KEYS[2]: waitKey
--- KEYS[3]: crawlExecutionIdCountKey
--- KEYS[4]: queueCountTotalKey
--- ARGV[1]: nextReadyTime
--- ARGV[2]: crawlExecutionId
--- ARGV[3]: chg
---

--- Increment chg queue count
local queueCount = redis.call('INCR', KEYS[1])

--- Increment crawl execution queue count
redis.call('HINCRBY', KEYS[3], ARGV[2], 1)

--- Increment total queue count
redis.call('INCR', KEYS[4])

--- If new chg was created, queue it.
if queueCount == 1 then
    redis.call('ZADD', KEYS[2], ARGV[1], ARGV[3])
end
