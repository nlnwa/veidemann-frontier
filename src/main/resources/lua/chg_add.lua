---
--- Add CrawlHostGroup or increment queue count if it already exists
--- If attemptToSetAsBusy is true then set CrawlHostGroup as busy if current state is ready
---

local chgKey = KEYS[1]
local waitKey = KEYS[2]
local crawlExecutionIdCountKey = KEYS[3]
local queueCountTotalKey = KEYS[4]

local nextReadyTime = ARGV[1]
local crawlExecutionId = ARGV[2]
local chgId = ARGV[3]

local chgExists = redis.call('EXISTS', chgKey)

--- Increment chg queue count
local queueCount = redis.call('HINCRBY', chgKey, "qc", 1)

--- Increment crawl execution queue count
redis.call('HINCRBY', crawlExecutionIdCountKey, crawlExecutionId, 1)

--- Increment total queue count
redis.call('INCR', queueCountTotalKey)

if chgExists == 0 then
    -- If new chg was created, queue it.
    redis.call('ZADD', waitKey, nextReadyTime, chgId)
end

return queueCount
