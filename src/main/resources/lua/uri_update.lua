local ueIdKey = KEYS[1]

local ueIdOldVal = ARGV[1]
local ueIdNewVal = ARGV[2]

local removed = redis.call('ZREM', ueIdKey, ueIdOldVal)
if removed > 0 then
    redis.call('ZADD', ueIdKey, 0, ueIdNewVal)
end
