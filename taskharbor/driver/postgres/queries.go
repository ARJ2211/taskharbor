package postgres

const QEnqueue = `
INSERT INTO th_jobs (
	id,
	type,
	queue,
	payload,
	run_at,
	timeout_nanos,
	created_at,
	attempts,
	max_attempts,
	last_error,
	failed_at,
	status,
	idempotency_key
) VALUES (
	$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
)
`
