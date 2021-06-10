local jobExecutionKey = KEYS[1]
local oldState = ARGV[1]
local newState = ARGV[2]
local documentsCrawled = ARGV[3]
local documentsDenied = ARGV[4]
local documentsFailed = ARGV[5]
local documentsOutOfScope = ARGV[6]
local documentsRetried = ARGV[7]
local urisCrawled = ARGV[8]
local bytesCrawled = ARGV[9]

if newState == 'CREATED' or redis.call('EXISTS', jobExecutionKey) == 1 then
    -- Update states
    if oldState ~= 'UNDEFINED' then
        redis.call('HINCRBY', jobExecutionKey, oldState, -1)
    end

    if newState ~= 'UNDEFINED' then
        redis.call('HINCRBY', jobExecutionKey, newState, 1)
    end

    -- Update stats
    redis.call('HINCRBY', jobExecutionKey, "documentsCrawled", documentsCrawled)
    redis.call('HINCRBY', jobExecutionKey, "documentsDenied", documentsDenied)
    redis.call('HINCRBY', jobExecutionKey, "documentsFailed", documentsFailed)
    redis.call('HINCRBY', jobExecutionKey, "documentsOutOfScope", documentsOutOfScope)
    redis.call('HINCRBY', jobExecutionKey, "documentsRetried", documentsRetried)
    redis.call('HINCRBY', jobExecutionKey, "urisCrawled", urisCrawled)
    redis.call('HINCRBY', jobExecutionKey, "bytesCrawled", bytesCrawled)

    local running = 0
    if redis.call('HEXISTS', jobExecutionKey, 'UNDEFINED') == 1 then
        running = running + tonumber(redis.call('HGET', jobExecutionKey, 'UNDEFINED'))
    end
    if redis.call('HEXISTS', jobExecutionKey, 'CREATED') == 1 then
        running = running + tonumber(redis.call('HGET', jobExecutionKey, 'CREATED'))
    end
    if redis.call('HEXISTS', jobExecutionKey, 'FETCHING') == 1 then
        running = running + tonumber(redis.call('HGET', jobExecutionKey, 'FETCHING'))
    end
    if redis.call('HEXISTS', jobExecutionKey, 'SLEEPING') == 1 then
        running = running + tonumber(redis.call('HGET', jobExecutionKey, 'SLEEPING'))
    end

    return running
end

return -1
