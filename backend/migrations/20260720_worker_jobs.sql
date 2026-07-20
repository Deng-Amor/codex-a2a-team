CREATE TABLE IF NOT EXISTS worker_jobs (
  id VARCHAR(80) PRIMARY KEY,
  workflow_id VARCHAR(40) NOT NULL,
  task_id VARCHAR(80) UNIQUE NOT NULL,
  job_type VARCHAR(40) NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'queued',
  attempt_id INTEGER NOT NULL DEFAULT 0,
  lease_version INTEGER NOT NULL DEFAULT 0,
  worker_id VARCHAR(120) NOT NULL DEFAULT '',
  lease_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  callback_id VARCHAR(120) NOT NULL DEFAULT '',
  result_payload TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_worker_jobs_status_lease ON worker_jobs (status, lease_expires_at);

