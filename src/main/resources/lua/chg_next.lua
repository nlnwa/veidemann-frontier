local busyKey = KEYS[1]
local chgKey = KEYS[2]

local chgId = ARGV[1]
local expiresTimeMillis = ARGV[2]

local isBusy = redis.call('ZRANK', busyKey, chgId)
if isBusy then
    error("CHG was already busy")
end

redis.call('ZADD', busyKey, expiresTimeMillis, chgId)
local queueCount = redis.call('HGET', chgKey, "qc")
return queueCount
