local busyKey = KEYS[1]
local chgKey = KEYS[2]

local expiresTimeMillis = ARGV[1]
local chgId = ARGV[2]
local sessionToken = ARGV[3]

local isBusy = redis.call('ZRANK', busyKey, chgId)
local storedSessionToken = ""
local st = redis.call('HGET', chgKey, "st")
if st then
    storedSessionToken = st
end

-- Only busy chg's can be released
if not isBusy then
    redis.log(redis.LOG_WARNING, "Trying to update timeout for chg '" .. chgId .. "' which was not busy")
    return false
end
if storedSessionToken ~= sessionToken then
    redis.log(redis.LOG_WARNING, "Trying to update timeout for chg '" .. chgId .. "' with sessionToken '" .. sessionToken .. "', but chg's sessionToken was '" .. storedSessionToken .. "'")
    return false
end

return redis.call('ZADD', busyKey, 'XX', expiresTimeMillis, chgId)
