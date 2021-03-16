local chgKey = KEYS[1]

local count = 0
-- Get count from chgKey
local c = redis.call('HGET', chgKey, "qc");
if c ~= 'nil' then
    count = c
end
return count
