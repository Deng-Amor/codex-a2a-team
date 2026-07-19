-- Stage-one LangGraph business-ledger schema.
-- The application records this version in schema_migrations during startup.
-- LangGraph's own checkpoint tables are owned by PostgresSaver.setup().

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS engine VARCHAR(40) NOT NULL DEFAULT 'python_legacy';
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS graph_version VARCHAR(40) NOT NULL DEFAULT 'stage1';
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS state_schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS thread_id VARCHAR(80) NOT NULL DEFAULT '';
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS next_event_sequence INTEGER NOT NULL DEFAULT 0;

ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS attempt_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS lease_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS worker_session_id VARCHAR(120) NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS workflow_task_attempts (
  id VARCHAR(80) PRIMARY KEY,
  workflow_id VARCHAR(40) NOT NULL,
  task_id VARCHAR(80) NOT NULL,
  attempt_number INTEGER NOT NULL,
  lease_version INTEGER NOT NULL DEFAULT 0,
  status VARCHAR(30) NOT NULL DEFAULT 'recorded',
  worker_session_id VARCHAR(120) NOT NULL DEFAULT '',
  idempotency_key VARCHAR(120) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TIMESTAMPTZ,
  CONSTRAINT uq_task_attempt_number UNIQUE (task_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS workflow_events (
  id BIGSERIAL PRIMARY KEY,
  workflow_id VARCHAR(40) NOT NULL,
  sequence INTEGER NOT NULL,
  event_type VARCHAR(80) NOT NULL,
  task_id VARCHAR(80) NOT NULL DEFAULT '',
  payload TEXT NOT NULL DEFAULT '{}',
  idempotency_key VARCHAR(120) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_workflow_event_sequence UNIQUE (workflow_id, sequence)
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id VARCHAR(80) PRIMARY KEY,
  workflow_id VARCHAR(40) NOT NULL,
  workflow_event_id BIGINT NOT NULL UNIQUE,
  event_type VARCHAR(80) NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  status VARCHAR(30) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_defects (
  id VARCHAR(80) PRIMARY KEY,
  workflow_id VARCHAR(40) NOT NULL,
  task_id VARCHAR(80) NOT NULL DEFAULT '',
  owner_agent VARCHAR(80) NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'open',
  content TEXT NOT NULL,
  content_hash VARCHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_workflow_defect_hash UNIQUE (workflow_id, content_hash)
);
