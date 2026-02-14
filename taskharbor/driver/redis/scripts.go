package redis

import (
	"context"
	"errors"
	"strconv"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/redis/go-redis/v9"
)

// Lua returns numbers as int64 sometimes — normalize for the 0/1 checks
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// enqueue: atomically HSET job hash and RPUSH ready or ZADD scheduled (single script = atomic).
// KEYS[1]=prefix, ARGV: id, queue, type, payload, run_at_nano, timeout_nano, created_at_nano, attempts, max_attempts, last_error, failed_at_nano, idempotency_key
const scriptEnqueue = `
local prefix = KEYS[1]
local id = ARGV[1]
local queue = ARGV[2]
local job_key = prefix .. ":job:" .. id
local ready_key = prefix .. ":queue:" .. queue .. ":ready"
local sched_key = prefix .. ":queue:" .. queue .. ":scheduled"
local run_at_nano = ARGV[5]
redis.call('HSET', job_key,
  'type', ARGV[3],
  'queue', queue,
  'payload', ARGV[4],
  'run_at_nano', run_at_nano,
  'timeout_nano', ARGV[6],
  'created_at_nano', ARGV[7],
  'attempts', ARGV[8],
  'max_attempts', ARGV[9],
  'last_error', ARGV[10],
  'failed_at_nano', ARGV[11],
  'idempotency_key', ARGV[12],
  'status', 'ready',
  'lease_token', '',
  'lease_expires_at_nano', '0'
)
if run_at_nano == '0' or run_at_nano == '' then
  redis.call('RPUSH', ready_key, id)
else
  redis.call('ZADD', sched_key, run_at_nano, id)
end
return 1
`

func (d *Driver) runEnqueueScript(ctx context.Context, rec *driver.JobRecord, runAtNano, createdAtNano, failedAtNano int64) error {
	keys := []string{d.opts.prefix}
	args := []interface{}{
		rec.ID,
		rec.Queue,
		rec.Type,
		string(rec.Payload),
		strconv.FormatInt(runAtNano, 10),
		strconv.FormatInt(rec.Timeout.Nanoseconds(), 10),
		strconv.FormatInt(createdAtNano, 10),
		rec.Attempts,
		rec.MaxAttempts,
		rec.LastError,
		strconv.FormatInt(failedAtNano, 10),
		rec.IdempotencyKey,
	}
	_, err := d.client.Eval(ctx, scriptEnqueue, keys, args...).Result()
	return err
}

// reserve: reclaim expired -> promote due scheduled -> LPOP one -> set lease + add to inflight. returns id or nil
const scriptReserve = `
local prefix = KEYS[1]
local queue = ARGV[1]
local now = tonumber(ARGV[2])
local token = ARGV[3]
local exp = tonumber(ARGV[4])
local ready_key = prefix .. ":queue:" .. queue .. ":ready"
local sched_key = prefix .. ":queue:" .. queue .. ":scheduled"
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"

-- reclaim expired inflight -> put back on ready
local expired = redis.call('ZRANGEBYSCORE', inflight_key, 0, now)
for _, id in ipairs(expired) do
  local job_key = prefix .. ":job:" .. id
  local status = redis.call('HGET', job_key, 'status')
  if status == 'inflight' then
    redis.call('ZREM', inflight_key, id)
    redis.call('HSET', job_key, 'status', 'ready', 'run_at_nano', '0', 'lease_token', '', 'lease_expires_at_nano', '0')
    redis.call('RPUSH', ready_key, id)
  end
end

-- promote due scheduled -> ready
local due = redis.call('ZRANGEBYSCORE', sched_key, 0, now)
for _, id in ipairs(due) do
  redis.call('ZREM', sched_key, id)
  local job_key = prefix .. ":job:" .. id
  redis.call('HSET', job_key, 'run_at_nano', '0')
  redis.call('RPUSH', ready_key, id)
end

-- pop one and lease it
local id = redis.call('LPOP', ready_key)
if id == false or id == nil then
  return nil
end

local job_key = prefix .. ":job:" .. id
redis.call('HSET', job_key, 'status', 'inflight', 'lease_token', token, 'lease_expires_at_nano', ARGV[4])
redis.call('ZADD', inflight_key, exp, id)
return id
`

func (d *Driver) runReserveScript(ctx context.Context, queue string, nowNano int64, token string, expiresAtNano int64) (string, error) {
	keys := []string{d.opts.prefix}
	args := []interface{}{
		queue,
		strconv.FormatInt(nowNano, 10),
		token,
		strconv.FormatInt(expiresAtNano, 10),
	}
	v, err := d.client.Eval(ctx, scriptReserve, keys, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	if v == nil {
		return "", nil
	}
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	default:
		return "", nil
	}
}

// extend: update hash + bump score in inflight zset so reclaim sees new expiry
const scriptExtendLease = `
local job_key = KEYS[1]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local new_exp = tonumber(ARGV[3])
local prefix = ARGV[4]
local id = ARGV[5]
local status = redis.call('HGET', job_key, 'status')
local db_tok = redis.call('HGET', job_key, 'lease_token')
local db_exp = redis.call('HGET', job_key, 'lease_expires_at_nano')
if status ~= 'inflight' or db_tok == false or db_exp == false then
  return 0
end
if db_tok ~= token then
  return 0
end
if tonumber(db_exp) <= now then
  return 0
end
redis.call('HSET', job_key, 'lease_expires_at_nano', tostring(new_exp))
local queue = redis.call('HGET', job_key, 'queue')
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"
redis.call('ZADD', inflight_key, new_exp, id)
return 1
`

func (d *Driver) runExtendLeaseScript(ctx context.Context, id, token string, nowNano int64, newExpiresAtNano int64) (bool, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []interface{}{
		token,
		strconv.FormatInt(nowNano, 10),
		newExpiresAtNano,
		d.opts.prefix,
		id,
	}
	v, err := d.client.Eval(ctx, scriptExtendLease, keys, args...).Result()
	if err != nil {
		return false, err
	}
	n, _ := toInt64(v)
	return n == 1, nil
}

// ack: check inflight + token + not expired, then mark job done (status=done, clear lease) and ZREM from inflight.
// We keep the job hash for inspect/done parity with postgres; we do not DEL.
// Returns: 1 = success, 2 = not inflight, 3 = token mismatch, 4 = expired.
const scriptAck = `
local job_key = KEYS[1]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local prefix = ARGV[3]
local id = ARGV[4]
local status = redis.call('HGET', job_key, 'status')
local db_tok = redis.call('HGET', job_key, 'lease_token')
local db_exp = redis.call('HGET', job_key, 'lease_expires_at_nano')
if status ~= 'inflight' or db_tok == false or db_exp == false then
  return 2
end
if db_tok ~= token then
  return 3
end
if tonumber(db_exp) <= now then
  return 4
end
local queue = redis.call('HGET', job_key, 'queue')
redis.call('HSET', job_key, 'status', 'done', 'lease_token', '', 'lease_expires_at_nano', '0')
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"
redis.call('ZREM', inflight_key, id)
return 1
`

// runAckScript returns (1=success, 2=not inflight, 3=token mismatch, 4=expired), or error.
func (d *Driver) runAckScript(ctx context.Context, id, token string, nowNano int64) (int64, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []interface{}{token, strconv.FormatInt(nowNano, 10), d.opts.prefix, id}
	v, err := d.client.Eval(ctx, scriptAck, keys, args...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := toInt64(v)
	if !ok {
		return 0, nil
	}
	return n, nil
}

// retry: back to ready/scheduled with new attempts/last_error, clear lease, ZREM from inflight
const scriptRetry = `
local job_key = KEYS[1]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local run_at_nano = tonumber(ARGV[3])
local attempts = ARGV[4]
local last_error = ARGV[5]
local failed_at_nano = ARGV[6]
local prefix = ARGV[7]
local id = ARGV[8]
local status = redis.call('HGET', job_key, 'status')
local db_tok = redis.call('HGET', job_key, 'lease_token')
local db_exp = redis.call('HGET', job_key, 'lease_expires_at_nano')
if status ~= 'inflight' or db_tok == false or db_exp == false then
  return 0
end
if db_tok ~= token then
  return 0
end
if tonumber(db_exp) <= now then
  return 0
end
local queue = redis.call('HGET', job_key, 'queue')
redis.call('HSET', job_key, 'status', 'ready', 'run_at_nano', tostring(run_at_nano), 'attempts', attempts, 'last_error', last_error, 'failed_at_nano', failed_at_nano, 'lease_token', '', 'lease_expires_at_nano', '0')
redis.call('ZREM', prefix .. ":queue:" .. queue .. ":inflight", id)
local ready_key = prefix .. ":queue:" .. queue .. ":ready"
local sched_key = prefix .. ":queue:" .. queue .. ":scheduled"
if run_at_nano == 0 then
  redis.call('RPUSH', ready_key, id)
else
  redis.call('ZADD', sched_key, run_at_nano, id)
end
return 1
`

func (d *Driver) runRetryScript(ctx context.Context, id, token string, nowNano, runAtNano int64, attempts int, lastError string, failedAtNano int64) (bool, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []interface{}{
		token,
		strconv.FormatInt(nowNano, 10),
		runAtNano,
		attempts,
		lastError,
		strconv.FormatInt(failedAtNano, 10),
		d.opts.prefix,
		id,
	}
	v, err := d.client.Eval(ctx, scriptRetry, keys, args...).Result()
	if err != nil {
		return false, err
	}
	n, _ := toInt64(v)
	return n == 1, nil
}

// fail: status=dlq, set reason + failed_at, clear lease, ZREM inflight, RPUSH dlq list
const scriptFail = `
local job_key = KEYS[1]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local reason = ARGV[3]
local failed_at_nano = tonumber(ARGV[4])
local prefix = ARGV[5]
local id = ARGV[6]
local status = redis.call('HGET', job_key, 'status')
local db_tok = redis.call('HGET', job_key, 'lease_token')
local db_exp = redis.call('HGET', job_key, 'lease_expires_at_nano')
if status ~= 'inflight' or db_tok == false or db_exp == false then
  return 0
end
if db_tok ~= token then
  return 0
end
if tonumber(db_exp) <= now then
  return 0
end
local queue = redis.call('HGET', job_key, 'queue')
redis.call('HSET', job_key, 'status', 'dlq', 'lease_token', '', 'lease_expires_at_nano', '0', 'dlq_reason', reason, 'dlq_failed_at_nano', tostring(failed_at_nano))
redis.call('ZREM', prefix .. ":queue:" .. queue .. ":inflight", id)
redis.call('RPUSH', prefix .. ":queue:" .. queue .. ":dlq", id)
return 1
`

func (d *Driver) runFailScript(ctx context.Context, id, token string, nowNano int64, reason string, failedAtNano int64) (bool, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []interface{}{
		token,
		strconv.FormatInt(nowNano, 10),
		reason,
		failedAtNano,
		d.opts.prefix,
		id,
	}
	v, err := d.client.Eval(ctx, scriptFail, keys, args...).Result()
	if err != nil {
		return false, err
	}
	n, _ := toInt64(v)
	return n == 1, nil
}
