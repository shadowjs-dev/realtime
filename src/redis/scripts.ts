/**
 * The Lua scripts of the Redis adapter, and the SHA-1 that names them on the server. Every script touches only keys of one
 * channel (one hash slot) and reads the clock from the server (`TIME`), so servers with skewed clocks agree.
 */

/** A script with its source and the SHA-1 `EVALSHA` uses. */
export interface Script {
  readonly source: string;
  readonly sha: string;
}

function rotateLeft(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

/**
 * SHA-1 of an ASCII or UTF-8 text, as lowercase hex. Pure TypeScript, so the adapter needs neither `node:crypto` nor a
 * round trip to `SCRIPT LOAD`.
 */
export function sha1(text: string): string {
  const input = new TextEncoder().encode(text);
  const total = ((input.length + 9 + 63) >> 6) << 6;
  const padded = new Uint8Array(total);
  padded.set(input);
  padded[input.length] = 0x80;
  const view = new DataView(padded.buffer);
  const bits = input.length * 8;
  view.setUint32(total - 8, Math.floor(bits / 2 ** 32));
  view.setUint32(total - 4, bits >>> 0);
  let h0 = 0x67452301;
  let h1 = 0xefcdab89;
  let h2 = 0x98badcfe;
  let h3 = 0x10325476;
  let h4 = 0xc3d2e1f0;
  const w = new Uint32Array(80);
  for (let offset = 0; offset < total; offset += 64) {
    for (let i = 0; i < 16; i += 1) w[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 80; i += 1)
      w[i] = rotateLeft(
        (w[i - 3] as number) ^ (w[i - 8] as number) ^ (w[i - 14] as number) ^ (w[i - 16] as number),
        1,
      );
    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;
    for (let i = 0; i < 80; i += 1) {
      let f: number;
      let k: number;
      if (i < 20) {
        f = (b & c) | (~b & d);
        k = 0x5a827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ed9eba1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8f1bbcdc;
      } else {
        f = b ^ c ^ d;
        k = 0xca62c1d6;
      }
      const next = (rotateLeft(a, 5) + (f >>> 0) + e + k + (w[i] as number)) >>> 0;
      e = d;
      d = c;
      c = rotateLeft(b, 30);
      b = a;
      a = next;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
  }
  return [h0, h1, h2, h3, h4].map((word) => word.toString(16).padStart(8, "0")).join("");
}

function script(source: string): Script {
  return { source, sha: sha1(source) };
}

/** Lua helpers shared by the scripts that read the clock. */
const CLOCK = `
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function num(n) return string.format('%.0f', n) end
`;

/**
 * Publish. KEYS: meta hash, log zset (score = seq, member = the message), time zset (score = accepted-at, member = zero-padded
 * seq), optional idempotency key. ARGV: history, historyTtlMs, idempotencyTtlMs, channelTtlMs, topic, message tail.
 * Returns `{seq, duplicate}`.
 */
export const PUBLISH: Script = script(`${CLOCK}
local meta, log, tix, idem = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
if idem then
  local prev = redis.call('GET', idem)
  if prev then return {tonumber(prev), 1} end
end
local history = tonumber(ARGV[1])
local histTtl = tonumber(ARGV[2])
local idemTtl = tonumber(ARGV[3])
local chanTtl = tonumber(ARGV[4])
local at = now_ms()
local seq = redis.call('HINCRBY', meta, 's', 1)
if history > 0 then
  local newest = redis.call('ZREVRANGE', tix, 0, 0, 'WITHSCORES')
  if newest[2] and tonumber(newest[2]) > at then at = tonumber(newest[2]) end
end
local msg = '{"s":' .. num(seq) .. ',"a":' .. num(at) .. ',' .. ARGV[6]
if history > 0 then
  local pad = string.format('%016.0f', seq)
  redis.call('ZADD', log, seq, msg)
  redis.call('ZADD', tix, at, pad)
  local n = redis.call('ZCARD', log)
  if n ~= redis.call('ZCARD', tix) then
    redis.call('DEL', log, tix)
    redis.call('ZADD', log, seq, msg)
    redis.call('ZADD', tix, at, pad)
  else
    local drop = n - history
    local old = redis.call('ZCOUNT', tix, '-inf', '(' .. num(at - histTtl))
    if old > drop then drop = old end
    if drop > 0 then
      redis.call('ZREMRANGEBYRANK', log, 0, drop - 1)
      redis.call('ZREMRANGEBYRANK', tix, 0, drop - 1)
    end
  end
  redis.call('PEXPIRE', log, histTtl)
  redis.call('PEXPIRE', tix, histTtl)
else
  redis.call('DEL', log, tix)
end
redis.call('HSET', meta, 't', ARGV[2])
local keep = chanTtl
if histTtl > keep then keep = histTtl end
if idem and idemTtl > keep then keep = idemTtl end
redis.call('PEXPIRE', meta, keep)
if idem then redis.call('SET', idem, num(seq), 'PX', idemTtl) end
redis.call('PUBLISH', ARGV[5], msg)
return {seq, 0}
`);

/**
 * History. KEYS: meta, log, time. ARGV: after, limit. Returns `{head, oldest, message...}`; a message older than the channel's
 * history lifetime is not returned even when no publish has trimmed it yet.
 */
export const HISTORY: Script = script(`${CLOCK}
local meta, log, tix = KEYS[1], KEYS[2], KEYS[3]
local head = tonumber(redis.call('HGET', meta, 's') or '0')
local ttl = redis.call('HGET', meta, 't')
local low = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local skip = 0
if ttl then
  skip = redis.call('ZCOUNT', tix, '-inf', '(' .. num(now_ms() - tonumber(ttl)))
end
local oldest = 0
local first = redis.call('ZRANGE', tix, skip, skip)
if first[1] then oldest = tonumber(first[1]) end
if skip > 0 then
  local expired = tonumber(redis.call('ZRANGE', tix, skip - 1, skip - 1)[1])
  if expired and expired > low then low = expired end
end
local out = {head, oldest}
if limit > 0 and oldest > 0 then
  local rows = redis.call('ZRANGEBYSCORE', log, '(' .. num(low), '+inf', 'LIMIT', 0, limit)
  for i = 1, #rows do out[#out + 1] = rows[i] end
end
return out
`);

/**
 * Keeps the numbering of a channel that has listeners alive. KEYS: meta. ARGV: channelTtlMs. Only ever extends the lifetime.
 */
export const KEEPALIVE: Script = script(`
local ms = tonumber(ARGV[1])
local left = redis.call('PTTL', KEYS[1])
if left >= 0 and left < ms then redis.call('PEXPIRE', KEYS[1], ms) end
return 1
`);

/** Lua: extends the lifetime of both presence keys to `keep` milliseconds, never shortens it. */
const EXTEND = `
local function extend(keep)
  for i = 1, 2 do
    local left = redis.call('PTTL', KEYS[i])
    if left == -1 or (left >= 0 and left < keep) then redis.call('PEXPIRE', KEYS[i], keep) end
  end
end
`;

/**
 * Presence set. KEYS: members hash (id -> stored), expiry zset (id -> expires). ARGV: id, version, ttlMs, topic, stored, member
 * JSON, graceMs. Returns 0 for an ignored older version, else 1. Publishes join (new) or set (higher version).
 */
export const PRESENCE_SET: Script = script(`${CLOCK}${EXTEND}
local id, version, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], id)
local op = nil
if cur then
  local known = tonumber(string.match(cur, '^{"v":(%d+),'))
  if known and known > version then return 0 end
  if known == nil or known < version then op = 'set' end
else
  op = 'join'
end
redis.call('HSET', KEYS[1], id, ARGV[5])
redis.call('ZADD', KEYS[2], num(now_ms() + ttl), id)
extend(ttl + tonumber(ARGV[7]))
if op then redis.call('PUBLISH', ARGV[4], '{"o":"' .. op .. '","m":' .. ARGV[6] .. '}') end
return 1
`);

/**
 * Presence remove. KEYS: hash, zset. ARGV: id, topic. Returns 1 only for the call that removed the member (which alone
 * publishes the leave), else 0.
 */
export const PRESENCE_REMOVE: Script = script(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if not cur then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
local member = string.match(cur, '^{"v":%d+,"m":(.*)}$')
if member then redis.call('PUBLISH', ARGV[2], '{"o":"leave","m":' .. member .. '}') end
return 1
`);

/**
 * Presence touch. KEYS: hash, zset. ARGV: ttlMs, graceMs, id... Refreshes the expiry of the members that exist and returns the
 * ids that do not.
 */
export const PRESENCE_TOUCH: Script = script(`${CLOCK}${EXTEND}
local ttl = tonumber(ARGV[1])
local expires = num(now_ms() + ttl)
local missing = {}
local found = false
for i = 3, #ARGV do
  if redis.call('HEXISTS', KEYS[1], ARGV[i]) == 1 then
    redis.call('ZADD', KEYS[2], expires, ARGV[i])
    found = true
  else
    missing[#missing + 1] = ARGV[i]
  end
end
if found then extend(ttl + tonumber(ARGV[2])) end
return missing
`);

/**
 * Presence reap. KEYS: hash, zset. ARGV: topic, limit. Removes up to `limit` expired members, publishing one leave for each,
 * and returns their stored values.
 */
export const PRESENCE_REAP: Script = script(`${CLOCK}
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', num(now_ms()), 'LIMIT', 0, tonumber(ARGV[2]))
local out = {}
for i = 1, #ids do
  local cur = redis.call('HGET', KEYS[1], ids[i])
  redis.call('ZREM', KEYS[2], ids[i])
  if cur then
    redis.call('HDEL', KEYS[1], ids[i])
    out[#out + 1] = cur
    local member = string.match(cur, '^{"v":%d+,"m":(.*)}$')
    if member then redis.call('PUBLISH', ARGV[1], '{"o":"leave","m":' .. member .. '}') end
  end
end
return out
`);

/** Presence list. KEYS: hash, zset. Returns the stored values of the members that have not expired. */
export const PRESENCE_LIST: Script = script(`${CLOCK}
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '(' .. num(now_ms()), '+inf')
local out = {}
local step = 400
for from = 1, #ids, step do
  local to = from + step - 1
  if to > #ids then to = #ids end
  local values = redis.call('HMGET', KEYS[1], unpack(ids, from, to))
  for i = 1, #values do
    if values[i] then out[#out + 1] = values[i] end
  end
end
return out
`);
