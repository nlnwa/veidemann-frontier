local busyKey = KEYS[1]
local sessionKey = KEYS[2]
local currentTimeMillis = ARGV[1]

-- List all busy chg which has timeout time before current time
local res = redis.call('ZRANGEBYSCORE', busyKey, 0, currentTimeMillis)
if next(res) ~= nil then
    -- Remove from session table
    redis.call('HDEL', sessionKey, unpack(res))
end

return res
