-- Sliding Window Rate Limiter --

-- inputs
local key       = KEYS[1]
local window_ms = tonumber(ARGV[1])
local limit     = tonumber(ARGV[2])
local member    = ARGV[3]

-- fetch atomic server time in milliseconds to prevent client clock skew
local t      = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local cutoff = now_ms - window_ms

-- evict requests outside the active window and get current count
redis.call('ZREMRANGEBYSCORE', key, 0, '(' .. cutoff)
local count = redis.call('ZCARD', key)

-- evaluate rate limit and record request if allowed
local allowed = 0
if count < limit then
  allowed = 1
  redis.call('ZADD', key, now_ms, member)
  count = count + 1
end

-- calculate retry delay based on the oldest request in the window
local ttl_ms = window_ms
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest ~= nil and oldest[2] ~= nil then
  ttl_ms = math.max(0, (tonumber(oldest[2]) + window_ms) - now_ms)
end

-- set TTL for automatic key cleanup and return results
redis.call('PEXPIRE', key, window_ms)
return { allowed, count, ttl_ms }
