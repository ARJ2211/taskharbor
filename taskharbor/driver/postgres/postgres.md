## SQL schema draft

```sql
-- Table: th_jobs
-- Purpose: single-table model for jobs + leases + DLQ, mirroring memory driver semantics.

CREATE TABLE IF NOT EXISTS th_jobs (
  id               TEXT PRIMARY KEY,
  type             TEXT NOT NULL,
  queue            TEXT NOT NULL,
  payload          BYTEA NOT NULL,

  -- Scheduling: RunAt == zero in Go means runnable now. We store that as NULL.
  run_at           TIMESTAMPTZ NULL,

  -- Duration stored as nanoseconds (time.Duration is int64 nanos).
  timeout_nanos    BIGINT NOT NULL DEFAULT 0 CHECK (timeout_nanos >= 0),

  created_at       TIMESTAMPTZ NOT NULL,

  attempts         INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts     INT NOT NULL DEFAULT 0 CHECK (max_attempts >= 0),

  last_error       TEXT NOT NULL DEFAULT '',
  failed_at        TIMESTAMPTZ NULL,

  -- State machine:
  -- ready: eligible when due (run_at NULL or run_at <= now)
  -- inflight: leased, not eligible unless lease is expired (reclaim)
  -- dlq: terminal DLQ
  -- done: terminal success (optional; could delete instead)
  status           TEXT NOT NULL DEFAULT 'ready'
                   CHECK (status IN ('ready','inflight','dlq','done')),

  lease_token      TEXT NULL,
  lease_expires_at TIMESTAMPTZ NULL,

  dlq_reason       TEXT NULL,
  dlq_failed_at    TIMESTAMPTZ NULL,

  -- Present in milestone 5 for storage; uniqueness/enforcement is milestone 6.
  idempotency_key  TEXT NULL,

  -- Consistency constraints
  CONSTRAINT th_jobs_lease_pair_chk
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),

  CONSTRAINT th_jobs_inflight_requires_lease_chk
    CHECK (status <> 'inflight' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),

  CONSTRAINT th_jobs_dlq_requires_metadata_chk
    CHECK (status <> 'dlq' OR (dlq_failed_at IS NOT NULL))
);

-- No indexes in milestone 5 (correctness first).
-- Indexes for reserve performance + lease reclaim land in milestone 6.
```

## Mapping rules (Go <-> Postgres)

### 1) time.Time zero values

- JobRecord.RunAt.IsZero() maps to run_at = NULL
- RetryUpdate.RunAt.IsZero() maps to run_at = NULL
- FailedAt.IsZero() maps to failed_at = NULL
- Lease.ExpiresAt is always non-zero when inflight; stored as lease_expires_at

### 2) status model

- ready
    - not leased (lease_token/lease_expires_at are NULL)
    - may be due now or scheduled later based on run_at
- inflight
    - leased; lease fields are present
- dlq
    - terminal dead-lettered job; dlq_failed_at set, dlq_reason set by Fail
- done
    - terminal success; set by Ack (alternative is DELETE on ack)

### 3) runnable query rules (used by Reserve)

A job is runnable if:

- queue matches
- status is ready, OR status is inflight with an expired lease (reclaim case)
- and it is due:
    - run_at IS NULL OR run_at <= now

Important parity detail with memory driver:

- When reclaiming an expired inflight job, memory sets rec.RunAt = zero to run immediately.
- Postgres Reserve must do the same by setting run_at = NULL when it steals an expired inflight lease.

### 4) state transitions (high level)

- Enqueue
    - status = ready
    - lease fields NULL
    - dlq fields NULL
    - run_at NULL if RunAt is zero, else set run_at
- Reserve
    - atomically pick a runnable row (including reclaiming expired inflight)
    - set status = inflight
    - set lease_token, lease_expires_at = now + leaseFor
    - if reclaiming expired inflight, also set run_at = NULL
- ExtendLease
    - requires inflight + matching token + not expired
    - updates lease_expires_at = now + leaseFor
- Ack
    - requires inflight + matching token + not expired
    - sets status = done and clears lease fields (or delete row)
- Retry
    - requires inflight + matching token + not expired
    - clears lease fields, status = ready
    - updates run_at (NULL if zero) and Attempts/LastError/FailedAt from RetryUpdate
- Fail
    - requires inflight + matching token + not expired
    - clears lease fields
    - status = dlq
    - dlq_reason = reason, dlq_failed_at = now.UTC()

## Scope note (Milestone 5 vs Milestone 6)

- Milestone 5 includes schema + migrations + correct driver behavior.
- Milestone 6 adds hardening: idempotency uniqueness, performance indexes, and fully idempotent ack/fail semantics.
