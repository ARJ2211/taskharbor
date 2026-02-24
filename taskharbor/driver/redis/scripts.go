package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/redis/go-redis/v9"
)

// Scheduled ZSET representation (key fix for the conformance timing tests):
//   - score  = runAt Unix seconds (fits safely in Redis ZSET double score)
//   - member = "%09d:<jobID>" where %09d is nanoseconds within the second
// This lets us do nanosecond-accurate due checks inside Lua for the current second.
//
// Why: the conformance tests check due-1ns vs due (often within the same millisecond).
// Millisecond scores can't represent that boundary; nanosecond scores lose precision
// because Redis ZSET scores are doubles.

func schedMember(sub int64, id string) string {
	if sub < 0 {
		sub = 0
	}
	if sub > 999_999_999 {
		sub = 999_999_999
	}
	return fmt.Sprintf("%09d:%s", sub, id)
}

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

// enqueue: atomically HSET job hash and RPUSH ready or ZADD scheduled.
// KEYS[1]=prefix
// ARGV:
//
//	1  id
//	2  queue
//	3  type
//	4  payload
//	5  run_at_nano (string)
//	6  timeout_nano
//	7  created_at_nano
//	8  attempts
//	9  max_attempts
//	10 last_error
//	11 failed_at_nano
//	12 idempotency_key
//	13 run_at_sec (string)
//	14 run_at_member ("%09d:<id>")
const scriptEnqueue = `
local prefix = KEYS[1]
local id = ARGV[1]
local queue = ARGV[2]

local job_key   = prefix .. ":job:" .. id
local ready_key = prefix .. ":queue:" .. queue .. ":ready"
local sched_key = prefix .. ":queue:" .. queue .. ":scheduled"

-- idempotency mapping
local idem = ARGV[12]
local idem_key = nil
if idem ~= nil and idem ~= '' then
  idem_key = prefix .. ":idem:" .. queue .. ":" .. idem
  local existing = redis.call('GET', idem_key)
  if existing and existing ~= false and existing ~= '' then
    return {existing, 1}
  end
end

local run_at_nano = ARGV[5]
local status = "ready"
if run_at_nano ~= '0' and run_at_nano ~= '' then
  status = "scheduled"
end

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
  'idempotency_key', idem,
  'status', status,
  'lease_token', '',
  'lease_expires_at_nano', '0'
)

if status == 'ready' then
  redis.call('RPUSH', ready_key, id)
else
  local run_at_sec = tonumber(ARGV[13])
  local run_at_member = ARGV[14]
  redis.call('ZADD', sched_key, run_at_sec, run_at_member)
end

if idem_key ~= nil then
  redis.call('SET', idem_key, id)
end

return {id, 0}
`

func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

func (d *Driver) runEnqueueScript(
	ctx context.Context,
	rec *driver.JobRecord,
	runAtNano int64,
	runAtSec int64,
	runAtMember string,
	createdAtNano int64,
	failedAtNano int64,
) (string, bool, error) {
	keys := []string{d.opts.prefix}

	idem := strings.TrimSpace(rec.IdempotencyKey)

	args := []any{
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
		idem,
		strconv.FormatInt(runAtSec, 10),
		runAtMember,
	}

	res, err := d.client.Eval(ctx, scriptEnqueue, keys, args...).Result()
	if err != nil {
		return "", false, err
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return "", false, errors.New("enqueue script returned unexpected shape")
	}

	jobID, ok := asString(arr[0])
	if !ok {
		return "", false, errors.New("enqueue script returned non-string job id")
	}

	n, _ := toInt64(arr[1])
	existed := n == 1

	return jobID, existed, nil
}

// reserve: reclaim expired -> promote due scheduled -> LPOP one -> set lease + add to inflight.
// Scheduled promotion rules:
//   - all items with score < now_sec are due
//   - items with score == now_sec are due iff member nanos <= now_sub
const scriptReserve = `
local prefix = KEYS[1]
local queue = ARGV[1]

local now_sec = tonumber(ARGV[2])
local now_sub = tonumber(ARGV[3])
local now_ms  = tonumber(ARGV[4])

local token    = ARGV[5]
local exp_ms   = tonumber(ARGV[6])
local exp_nano = ARGV[7]

local ready_key    = prefix .. ":queue:" .. queue .. ":ready"
local sched_key    = prefix .. ":queue:" .. queue .. ":scheduled"
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"

local function member_to_id(m)
  local colon = string.find(m, ':')
  if colon then
    return string.sub(m, colon + 1)
  end
  return m
end

local function promote(m)
  redis.call('ZREM', sched_key, m)
  local id = member_to_id(m)
  local job_key = prefix .. ":job:" .. id
  local status = redis.call('HGET', job_key, 'status')
  if status == 'scheduled' then
    redis.call('HSET', job_key, 'status', 'ready', 'run_at_nano', '0')
    redis.call('RPUSH', ready_key, id)
  end
end

-- reclaim expired inflight -> ready
local expired = redis.call('ZRANGEBYSCORE', inflight_key, 0, now_ms)
for _, id in ipairs(expired) do
  local job_key = prefix .. ":job:" .. id
  local status = redis.call('HGET', job_key, 'status')

  redis.call('ZREM', inflight_key, id)

  if status == 'inflight' then
    redis.call('HSET', job_key,
      'status', 'ready',
      'run_at_nano', '0',
      'lease_token', '',
      'lease_expires_at_nano', '0'
    )
    redis.call('RPUSH', ready_key, id)
  end
end

-- promote scheduled strictly before this second
local prev_max = now_sec - 1
if prev_max >= 0 then
  local due_prev = redis.call('ZRANGEBYSCORE', sched_key, 0, prev_max)
  for _, m in ipairs(due_prev) do
    promote(m)
  end
end

-- promote scheduled in this second up to now_sub
local due_this = redis.call('ZRANGEBYSCORE', sched_key, now_sec, now_sec)
for _, m in ipairs(due_this) do
  local sub = tonumber(string.sub(m, 1, 9)) or 0
  if sub <= now_sub then
    promote(m)
  end
end

-- pop one and lease it
local id = redis.call('LPOP', ready_key)
if not id then
  return nil
end

local job_key = prefix .. ":job:" .. id
redis.call('HSET', job_key,
  'status', 'inflight',
  'lease_token', token,
  'lease_expires_at_nano', exp_nano,
  'run_at_nano', '0'
)
redis.call('ZADD', inflight_key, exp_ms, id)

return id
`

func (d *Driver) runReserveScript(
	ctx context.Context,
	queue string,
	nowSec int64,
	nowSub int64,
	nowMs int64,
	token string,
	expMs int64,
	expNano int64,
) (string, error) {
	keys := []string{d.opts.prefix}
	args := []any{
		queue,
		strconv.FormatInt(nowSec, 10),
		strconv.FormatInt(nowSub, 10),
		strconv.FormatInt(nowMs, 10),
		token,
		strconv.FormatInt(expMs, 10),
		strconv.FormatInt(expNano, 10),
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
local new_exp_nano = ARGV[3]
local new_exp_ms = tonumber(ARGV[4])
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
redis.call('HSET', job_key, 'lease_expires_at_nano', new_exp_nano)
local queue = redis.call('HGET', job_key, 'queue')
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"
redis.call('ZADD', inflight_key, new_exp_ms, id)
return 1
`

func (d *Driver) runExtendLeaseScript(ctx context.Context, id, token string, nowNano int64, newExpiresAtNano int64, newExpiresAtMs int64) (bool, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}

	args := []any{
		token,
		strconv.FormatInt(nowNano, 10),
		strconv.FormatInt(newExpiresAtNano, 10),
		strconv.FormatInt(newExpiresAtMs, 10),
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

// ack: check inflight + token + not expired, then mark job done and ZREM from inflight.
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
redis.call('RPUSH', prefix .. ":queue:" .. queue .. ":done", id)
local inflight_key = prefix .. ":queue:" .. queue .. ":inflight"
redis.call('ZREM', inflight_key, id)
return 1
`

func (d *Driver) runAckScript(ctx context.Context, id, token string, nowNano int64) (int64, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []any{token, strconv.FormatInt(nowNano, 10), d.opts.prefix, id}
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

// retry: back to ready/scheduled with new attempts/last_error, clear lease, ZREM inflight.
// Scheduled entries use (run_at_sec, run_at_member).
const scriptRetry = `
local job_key = KEYS[1]

local token = ARGV[1]
local now = tonumber(ARGV[2])

local run_at_nano = ARGV[3]
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

local new_status = 'ready'
if run_at_nano ~= '0' and run_at_nano ~= '' then
  new_status = 'scheduled'
end

redis.call('HSET', job_key,
  'status', new_status,
  'run_at_nano', run_at_nano,
  'attempts', attempts,
  'last_error', last_error,
  'failed_at_nano', failed_at_nano,
  'lease_token', '',
  'lease_expires_at_nano', '0'
)

redis.call('ZREM', prefix .. ":queue:" .. queue .. ":inflight", id)

local ready_key = prefix .. ":queue:" .. queue .. ":ready"
local sched_key = prefix .. ":queue:" .. queue .. ":scheduled"

if new_status == 'ready' then
  redis.call('RPUSH', ready_key, id)
else
  local run_at_sec = tonumber(ARGV[9])
  local run_at_member = ARGV[10]
  redis.call('ZADD', sched_key, run_at_sec, run_at_member)
end

return 1
`

func (d *Driver) runRetryScript(
	ctx context.Context,
	id, token string,
	nowNano int64,
	runAtNano int64,
	runAtSec int64,
	runAtMember string,
	attempts int,
	lastError string,
	failedAtNano int64,
) (bool, error) {
	jobKey := d.keyJob(id)
	keys := []string{jobKey}
	args := []any{
		token,
		strconv.FormatInt(nowNano, 10),
		strconv.FormatInt(runAtNano, 10),
		attempts,
		lastError,
		strconv.FormatInt(failedAtNano, 10),
		d.opts.prefix,
		id,
		strconv.FormatInt(runAtSec, 10),
		runAtMember,
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
	args := []any{
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

const scriptRequeueDLQ = `
local prefix = KEYS[1]
local id = ARGV[1]
local now_nano = tonumber(ARGV[2])

local queue_guard = ARGV[3]      -- '' means ignore
local run_at_nano = ARGV[4]      -- string
local run_at_sec = tonumber(ARGV[5])
local run_at_member = ARGV[6]
local reset = tonumber(ARGV[7]) -- 0/1

local job_key = prefix .. ":job:" .. id
if redis.call('EXISTS', job_key) == 0 then
  return 0
end

local status = redis.call('HGET', job_key, 'status')
if status ~= 'dlq' then
  return -1
end

local queue = redis.call('HGET', job_key, 'queue')
if queue_guard ~= nil and queue_guard ~= '' and queue_guard ~= queue then
  return -2
end

local dlq_key = prefix .. ":queue:" .. queue .. ":dlq"
redis.call('LREM', dlq_key, 0, id)

local new_status = 'ready'
if run_at_nano ~= nil and run_at_nano ~= '' and run_at_nano ~= '0' then
  if tonumber(run_at_nano) > now_nano then
    new_status = 'scheduled'
  else
    run_at_nano = '0'
  end
else
  run_at_nano = '0'
end

if reset == 1 then
  redis.call('HSET', job_key,
    'status', new_status,
    'run_at_nano', run_at_nano,
    'attempts', '0',
    'last_error', '',
    'failed_at_nano', '0',
    'dlq_reason', '',
    'dlq_failed_at_nano', '0',
    'lease_token', '',
    'lease_expires_at_nano', '0'
  )
else
  redis.call('HSET', job_key,
    'status', new_status,
    'run_at_nano', run_at_nano,
    'dlq_reason', '',
    'dlq_failed_at_nano', '0',
    'lease_token', '',
    'lease_expires_at_nano', '0'
  )
end

if new_status == 'ready' then
  redis.call('RPUSH', prefix .. ":queue:" .. queue .. ":ready", id)
else
  redis.call('ZADD', prefix .. ":queue:" .. queue .. ":scheduled", run_at_sec, run_at_member)
end

return 1
`

func (d *Driver) runRequeueDLQScript(
	ctx context.Context,
	id string,
	nowNano int64,
	queueGuard string,
	runAtNano int64,
	runAtSec int64,
	runAtMember string,
	resetAttempts bool,
) (int64, error) {
	keys := []string{d.opts.prefix}
	reset := int64(0)
	if resetAttempts {
		reset = 1
	}
	args := []any{
		id,
		strconv.FormatInt(nowNano, 10),
		queueGuard,
		strconv.FormatInt(runAtNano, 10),
		strconv.FormatInt(runAtSec, 10),
		runAtMember,
		strconv.FormatInt(reset, 10),
	}

	v, err := d.client.Eval(ctx, scriptRequeueDLQ, keys, args...).Result()
	if err != nil {
		return 0, err
	}
	n, _ := toInt64(v)
	return n, nil
}

// Silence unused import warnings if you temporarily comment-out scripts during refactors.
var _ = errors.New
