---
--- KEYS[1]: jobExecutionKey
--- ARGV[1]: oldState
--- ARGV[2]: newState
--- ARGV[3]: documentsCrawled
--- ARGV[4]: documentsDenied
--- ARGV[5]: documentsFailed
--- ARGV[6]: documentsOutOfScope
--- ARGV[7]: documentsRetried
--- ARGV[8]: urisCrawled
--- ARGV[9]: bytesCrawled
---

-- Update states
if ARGV[1] ~= 'UNDEFINED' then
    redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
end

if ARGV[2] ~= 'UNDEFINED' then
    redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
end

-- Update stats
redis.call('HINCRBY', KEYS[1], "documentsCrawled", ARGV[3])
redis.call('HINCRBY', KEYS[1], "documentsDenied", ARGV[4])
redis.call('HINCRBY', KEYS[1], "documentsFailed", ARGV[5])
redis.call('HINCRBY', KEYS[1], "documentsOutOfScope", ARGV[6])
redis.call('HINCRBY', KEYS[1], "documentsRetried", ARGV[7])
redis.call('HINCRBY', KEYS[1], "urisCrawled", ARGV[8])
redis.call('HINCRBY', KEYS[1], "bytesCrawled", ARGV[9])
