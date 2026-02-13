-- 003_milestone6_indexes.sql
-- Milestone 6: indexes to support fast reserve + lease reclaim

-- Ready jobs runnable immediately (run_at IS NULL)
CREATE INDEX IF NOT EXISTS th_jobs_ready_immediate_idx
  ON th_jobs (queue, created_at, id)
  WHERE status = 'ready' AND run_at IS NULL;

-- Ready jobs scheduled (filter by run_at <= now)
CREATE INDEX IF NOT EXISTS th_jobs_ready_scheduled_idx
  ON th_jobs (queue, run_at, created_at, id)
  WHERE status = 'ready' AND run_at IS NOT NULL;

-- Inflight jobs eligible for reclaim (lease_expires_at <= now)
CREATE INDEX IF NOT EXISTS th_jobs_inflight_expired_idx
  ON th_jobs (queue, lease_expires_at, created_at, id)
  WHERE status = 'inflight';

-- Query for psql to analyze the indexes
-- EXPLAIN (ANALYZE, BUFFERS)
-- WITH cte AS (
--   SELECT id
--   FROM th_jobs
--   WHERE queue = 'default'
--     AND status NOT IN ('done','dlq')
--     AND (
--       (status = 'ready' AND (run_at IS NULL OR run_at <= now()))
--       OR
--       (status = 'inflight' AND lease_expires_at <= now())
--     )
--   ORDER BY
--     CASE
--       WHEN status = 'ready' AND run_at IS NULL THEN 0
--       WHEN status = 'inflight' AND lease_expires_at <= now() THEN 1
--       ELSE 2
--     END,
--     created_at ASC,
--     run_at ASC NULLS LAST,
--     id ASC
--   FOR UPDATE SKIP LOCKED
--   LIMIT 1
-- )
-- UPDATE th_jobs j
-- SET status='inflight', lease_token='x', lease_expires_at=now() + interval '30 seconds'
-- FROM cte
-- WHERE j.id = cte.id
-- RETURNING j.id;
