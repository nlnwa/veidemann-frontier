local busyKey = KEYS[1]
local waitKey = KEYS[2]
local chgKey = KEYS[3]
local sessionKey = KEYS[4]

local waitTime = ARGV[1]
local chgId = ARGV[2]
local sessionToken = ARGV[3]
local isTimeout = ARGV[4] == "true"

local isBusy = redis.call('ZRANK', busyKey, chgId)
local storedSessionToken = ""
local st = redis.call('HGET', chgKey, "st")
if st then
    storedSessionToken = st
end

-- Only busy or expired chg's can be released
if not isTimeout and not isBusy then
    error("Trying to release chg '" .. chgId .. "' which was not busy")
end
if isTimeout and isBusy then
    error("Trying to release chg '" .. chgId .. "' caused by timeout, but chg was busy")
end
if storedSessionToken ~= sessionToken then
    error("Trying to release chg '" .. chgId .. "' with sessionToken '" .. sessionToken .. "', but chg's sessionToken was '" .. storedSessionToken .. "'")
end

local queueCount = redis.call('HGET', chgKey, "qc")

-- Remove chg from busyKey
if isTimeout or redis.call('ZREM', busyKey, chgId) == 1 then
    -- Remove session token
    local sessionToken = redis.call('HGET', chgKey, "st")
    if sessionToken then
        redis.call('HDEL', sessionKey, sessionToken)
    end

    -- Check queue count from chgKey.
    if (not queueCount) or (tonumber(queueCount) <= 0) then
        -- If queue count is zero there is no need for chg, remove it.
        redis.call('DEL', chgKey)
    else
        -- Otherwise add chg to waitKey.
        redis.call('ZADD', waitKey, waitTime, chgId)
        -- Remove unnecessary data from chg.
        redis.call('HDEL', chgKey, "df", "rd", "mi", "ma", "mr", "u", "st", "ts")
    end
end

return queueCount