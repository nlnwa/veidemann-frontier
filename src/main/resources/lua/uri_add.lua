local ueIdKey = KEYS[1]
local uchgKey = KEYS[2]

local ueIdVal = ARGV[1]
local weight = tonumber(ARGV[2])
local eid = ARGV[3]

redis.call('ZADD', ueIdKey, 0, ueIdVal)
redis.call('ZADD', uchgKey, weight, eid)
