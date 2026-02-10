-- 002_milestone6.sql
-- Added constraint on the idempotency key to avoid dedups

ALTER TABLE th_jobs
  ADD CONSTRAINT th_jobs_queue_idempotency_key_uniq UNIQUE (queue, idempotency_key);