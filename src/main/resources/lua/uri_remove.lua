local ueIdKey = KEYS[1]
local uchgKey = KEYS[2]
local chgKey = KEYS[3]
local crawlExecutionIdCountKey = KEYS[4]
local queueCountTotalKey = KEYS[5]
local uriremovequeuekey = KEYS[6]

local ueIdVal = ARGV[1]
local eid = ARGV[2]
local uriId = ARGV[3]

local removed = redis.call('ZREM', ueIdKey, ueIdVal)
if removed <= 0 then
    redis.log(redis.LOG_WARNING, "REM", ueIdKey, ueIdVal, removed)
end

if removed > 0 then
    -- Decrement CHG queue count
    redis.call('HINCRBY', chgKey, "qc", -1)

    -- Decrement crawl execution queue count
    local remaining_uri_count = redis.call('HINCRBY', crawlExecutionIdCountKey, eid, -1)
    if remaining_uri_count <= 0 then
        redis.call('HDEL', crawlExecutionIdCountKey, eid)
    end
    -- Decrement total queue count
    redis.call('DECR', queueCountTotalKey);

    -- Add uri to remove queue
    if uriId ~= '' then
        redis.call('RPUSH', uriremovequeuekey, uriId)
    end

    if redis.call('EXISTS', ueIdKey) == 0 then
        redis.call('ZREM', uchgKey, eid)
    end
end

return removed
