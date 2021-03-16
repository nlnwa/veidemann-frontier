---
--- Add CrawlHostGroup or increment queue count if it already exists
--- If attemptToSetAsBusy is true then set CrawlHostGroup as busy if current state is ready
---

local chgKey = KEYS[1]
local waitKey = KEYS[2]
local busyKey = KEYS[3]
local readyKey = KEYS[4]
local crawlExecutionIdCountKey = KEYS[5]
local queueCountTotalKey = KEYS[6]

local nextReadyTime = ARGV[1]
local crawlExecutionId = ARGV[2]
local chgId = ARGV[3]
local attemptToSetAsBusy = ARGV[4]
local busyExpireTime = ARGV[5]

local chgExists = redis.call('EXISTS', chgKey)

--- Increment chg queue count
local queueCount = redis.call('HINCRBY', chgKey, "qc", 1)

--- Increment crawl execution queue count
redis.call('HINCRBY', crawlExecutionIdCountKey, crawlExecutionId, 1)

--- Increment total queue count
redis.call('INCR', queueCountTotalKey)

if attemptToSetAsBusy == 'true' then
    -- Caller want chg to be busy immediately

    -- If a new chg was created, it is safe to add as busy
    if queueCount == 1 then
        redis.call('ZADD', busyKey, busyExpireTime, chgId)
        return 'true'
    end

    -- Check if chg is ready and can be added to busy
    local isReady = redis.call('ZRANK', readyKey, chgId)
    if isReady ~= 'nil' then
        -- Remove from ready
        redis.call('LREM', readyKey, 0, chgId)
        -- Add chg to busy and return queue count
        redis.call('ZADD', busyKey, busyExpireTime, chgId)
        return 'true'
    end
else
    if chgExists == 0 then
        -- If new chg was created, queue it.
        redis.call('ZADD', waitKey, nextReadyTime, chgId)
    end
end

return 'false'
