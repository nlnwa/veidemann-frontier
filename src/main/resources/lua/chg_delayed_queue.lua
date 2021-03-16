local fromQueueKey = KEYS[1]
local toQueueKey = KEYS[2]
local currentTimeMillis = ARGV[1]

local res = redis.call('ZRANGEBYSCORE', fromQueueKey, 0, currentTimeMillis)
local moved = 0
for _, key in ipairs(res) do
    redis.call('ZREM', fromQueueKey, key)
    redis.call('RPUSH', toQueueKey, key)
    moved = moved + 1
end
return moved
