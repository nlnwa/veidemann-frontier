---
--- KEYS[1]: ueIdKey
--- KEYS[2]: uchgKey
--- KEYS[3]: chgpKey
--- KEYS[4]: readyKey
--- KEYS[5]: busyKey
--- KEYS[6]: waitKey
--- KEYS[7]: eidcKey
--- KEYS[8]: Queue total count key
--- KEYS[9]: uri remove queue key
---
--- ARGV[1]: ueIdVal
--- ARGV[2]: eid
--- ARGV[3]: chgp
--- ARGV[4]: uri id
---

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    -- Decrement CHG queue count
    if redis.call('HINCRBY', KEYS[3], 'c', -1) == 0 then
        redis.call('DEL', KEYS[3])
        redis.call('LREM', KEYS[4], 0, ARGV[3])
        redis.call('ZREM', KEYS[5], ARGV[3])
        redis.call('ZREM', KEYS[6], ARGV[3])
    end
    -- Decrement crawl execution queue count
    local remaining_uri_count = redis.call('HINCRBY', KEYS[7], ARGV[2], -1)
    if remaining_uri_count == 0 then
        redis.call('HDEL', KEYS[7], ARGV[2])
    end
    -- Decrement total queue count
    redis.call('DECR', KEYS[8]);
end

-- Add uri to remove queue
if ARGV[4] ~= '' then
    redis.call('RPUSH', KEYS[9], ARGV[4])
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('ZREM', KEYS[2], ARGV[2])
end

return removed
