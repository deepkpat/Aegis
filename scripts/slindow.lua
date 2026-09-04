-- Slinding Window Rate Limter --

-- inputs --
local key       = KEYS[1]
local now_ms    = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local member    = ARGV[4]
local cutoff    = now_ms - window_ms

-- drop expired entries and count survivors --
redis.call('ZREMRANGEBYSCORE', key, 0, cutoff)
local count = redis.call('ZCARD', key)

-- admit only when under the limit --
local allowed = 0
if count < limit then
  allowed = 1
  redis.call('ZADD', key, now_ms, member)
  count = count + 1
end

-- time until the oldest entry slides out (for retry-after) --
local ttl_ms = window_ms
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest ~= nil and oldest[2] ~= nil then
  ttl_ms = (tonumber(oldest[2]) + window_ms) - now_ms
  if ttl_ms < 0 then
    ttl_ms = 0
  end
end

-- auto clean idle keys, then report --
redis.call('PEXPIRE', key, window_ms)
return { allowed, count, ttl_ms }
