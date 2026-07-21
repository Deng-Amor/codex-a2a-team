"""PostgreSQL integration coverage for the lease-based Worker API.

Run only against an isolated database, for example:
  $env:TEST_DATABASE_URL = $env:DATABASE_URL
  python -m unittest discover -s tests -p test_worker_jobs.py -v
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


class WorkerJobIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_url = os.environ.get("TEST_DATABASE_URL")
        if not base_url:
            raise unittest.SkipTest("set TEST_DATABASE_URL to run PostgreSQL Worker integration tests")
        cls.database_name = "a2a_worker_test_" + uuid4().hex[:10]
        cls.admin_engine = create_engine(base_url, isolation_level="AUTOCOMMIT")
        with cls.admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{cls.database_name}"'))
        os.environ["DATABASE_URL"] = base_url.rsplit("/", 1)[0] + "/" + cls.database_name

        from fastapi.testclient import TestClient
        from app.main import Local, Task, WorkerJob, app, engine

        cls.Local, cls.Task, cls.WorkerJob, cls.app_engine = Local, Task, WorkerJob, engine
        cls.client_context = TestClient(app)
        cls.client = cls.client_context.__enter__()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "client_context"):
            cls.client_context.__exit__(None, None, None)
        if hasattr(cls, "app_engine"):
            cls.app_engine.dispose()
        if hasattr(cls, "admin_engine"):
            cls.admin_engine.dispose()
            cleanup_engine = create_engine(os.environ["TEST_DATABASE_URL"], isolation_level="AUTOCOMMIT")
            with cleanup_engine.connect() as connection:
                connection.execute(text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :name AND pid <> pg_backend_pid()"), {"name": cls.database_name})
                connection.execute(text(f'DROP DATABASE IF EXISTS "{cls.database_name}"'))
            cleanup_engine.dispose()

    def create_started_job(self, label):
        workflow = self.client.post("/api/workflows", json={"title": label, "request": "实现后端 REST API"})
        self.assertEqual(workflow.status_code, 200, workflow.text)
        workflow_id = workflow.json()["id"]
        task_id = f"{workflow_id}_team_lead"
        started = self.client.post(f"/api/workflows/{workflow_id}/tasks/{task_id}/start", json={"idempotency_key": f"{label}-start"})
        self.assertEqual(started.status_code, 200, started.text)
        return workflow_id, task_id, started.json()["job_id"]

    def claim(self, workflow_id, job_id, worker_id="worker-a", agent_key="team-lead"):
        runtime_id = "runtime-" + worker_id
        registered = self.client.post("/api/agent-runtimes/register", json={
            "runtime_id": runtime_id, "agent_key": agent_key, "adapter": "codex",
            "worker_id": worker_id, "session_ref": "test-session-" + worker_id,
        })
        self.assertEqual(registered.status_code, 200, registered.text)
        response = self.client.post("/api/worker/jobs/claim", json={
            "worker_id": worker_id, "runtime_id": runtime_id, "job_id": job_id, "workflow_id": workflow_id,
        })
        self.assertEqual(response.status_code, 200, response.text)
        return runtime_id, response.json()["job"]

    def test_success_callback_is_idempotent_and_releases_contract_audit(self):
        workflow_id, task_id, job_id = self.create_started_job("idempotent-callback")
        runtime_id, lease = self.claim(workflow_id, job_id)
        callback = {"worker_id": "worker-a", "attempt_id": lease["attempt_id"], "lease_version": lease["lease_version"],
                    "runtime_id": runtime_id, "callback_id": "callback-1", "outcome": "succeeded", "evidence": "test evidence"}
        first = self.client.post(f"/api/worker/jobs/{job_id}/callback", json=callback)
        duplicate = self.client.post(f"/api/worker/jobs/{job_id}/callback", json=callback)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(duplicate.status_code, 200, duplicate.text)
        self.assertEqual(duplicate.json()["status"], "succeeded")
        tasks = self.client.get(f"/api/workflows/{workflow_id}").json()["tasks"]
        statuses = {task["stage"]: task["status"] for task in tasks}
        self.assertEqual(statuses["contract_audit"], "ready")
        self.assertEqual(self.client.get(f"/api/workflows/{workflow_id}/tasks/{task_id}").json()["status"], "passed")

    def test_expired_lease_requeues_and_rejects_old_worker(self):
        _, _, job_id = self.create_started_job("expired-lease")
        old_runtime, old_lease = self.claim("", job_id, "worker-old")
        with self.Local() as session:
            job = session.get(self.WorkerJob, job_id)
            job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            session.commit()
        self.assertEqual(self.client.post("/api/worker/jobs/reap-expired").json()["requeued"], 1)
        new_runtime, new_lease = self.claim("", job_id, "worker-new")
        stale = {"worker_id": "worker-old", "attempt_id": old_lease["attempt_id"], "lease_version": old_lease["lease_version"],
                 "runtime_id": old_runtime, "callback_id": "stale", "outcome": "succeeded", "evidence": "must not write"}
        self.assertEqual(self.client.post(f"/api/worker/jobs/{job_id}/callback", json=stale).status_code, 409)
        current = {"worker_id": "worker-new", "attempt_id": new_lease["attempt_id"], "lease_version": new_lease["lease_version"],
                   "runtime_id": new_runtime, "callback_id": "current", "outcome": "succeeded", "evidence": "new lease evidence"}
        self.assertEqual(self.client.post(f"/api/worker/jobs/{job_id}/callback", json=current).status_code, 200)

    def test_failed_callback_returns_task_to_ready_and_is_idempotent(self):
        workflow_id, task_id, job_id = self.create_started_job("failed-callback")
        runtime_id, lease = self.claim(workflow_id, job_id, "worker-failure")
        callback = {"worker_id": "worker-failure", "attempt_id": lease["attempt_id"], "lease_version": lease["lease_version"],
                    "runtime_id": runtime_id, "callback_id": "failure-1", "outcome": "failed", "error": "intentional test failure"}
        self.assertEqual(self.client.post(f"/api/worker/jobs/{job_id}/callback", json=callback).status_code, 200)
        self.assertEqual(self.client.post(f"/api/worker/jobs/{job_id}/callback", json=callback).status_code, 200)
        task = self.client.get(f"/api/workflows/{workflow_id}/tasks/{task_id}").json()
        self.assertEqual(task["status"], "ready")

    def test_a2a_flow_validation_uses_the_minimal_dag(self):
        workflow = self.client.post("/api/workflows", json={"title": "flow validation", "request": "验证 A2A 流程与 Dashboard 状态"})
        self.assertEqual(workflow.status_code, 200, workflow.text)
        detail = self.client.get(f"/api/workflows/{workflow.json()['id']}").json()
        self.assertEqual({task["stage"] for task in detail["tasks"]}, {"team_lead", "workflow_validation", "acceptance"})

    def test_prd_uses_document_review_without_contract_audit(self):
        workflow = self.client.post("/api/workflows", json={"title": "prd", "request": "编写 A2A 流程 PRD 产品需求文档"})
        self.assertEqual(workflow.status_code, 200, workflow.text)
        detail = self.client.get(f"/api/workflows/{workflow.json()['id']}").json()
        self.assertEqual({task["stage"] for task in detail["tasks"]}, {"team_lead", "document_review", "acceptance"})

    def test_targeted_claim_does_not_lease_another_workflow_job(self):
        first_workflow, _, first_job = self.create_started_job("targeted-claim-first")
        second_workflow, _, second_job = self.create_started_job("targeted-claim-second")
        _, claimed = self.claim(second_workflow, second_job, "targeted-worker")
        self.assertEqual(claimed["id"], second_job)
        with self.Local() as session:
            first = session.get(self.WorkerJob, first_job)
            self.assertEqual(first.workflow_id, first_workflow)
            self.assertEqual(first.status, "queued")

    def test_rejection_queues_replan_and_reopens_only_selected_stages(self):
        workflow = self.client.post("/api/workflows", json={"title": "replan", "request": "实现前端和后端 REST API"}).json()
        workflow_id = workflow["id"]
        with self.Local() as session:
            tasks = session.query(self.Task).filter_by(workflow_id=workflow_id).all()
            for task in tasks:
                task.status = "passed"
            acceptance = next(task for task in tasks if task.stage_key == "acceptance")
            acceptance.status = "acceptance_pending_human"
            session.commit()
        rejected = self.client.post(f"/api/workflows/{workflow_id}/acceptance/decision", json={
            "decision": "REJECT", "actor": "test human", "reason": "frontend needs a correction", "idempotency_key": "replan-reject"})
        self.assertEqual(rejected.status_code, 200, rejected.text)
        replan_job_id = rejected.json()["replan_job_id"]
        self.assertTrue(replan_job_id)
        runtime_id, lease = self.claim(workflow_id, replan_job_id, "replan-worker")
        self.assertEqual(lease["id"], replan_job_id)
        callback = {"worker_id": "replan-worker", "attempt_id": lease["attempt_id"], "lease_version": lease["lease_version"],
                    "runtime_id": runtime_id, "callback_id": "replan-callback", "outcome": "succeeded", "evidence": "replan reviewed rejection",
                    "replan": {"affected_stages": ["frontend"], "summary": "fix the frontend issue and rerun its downstream gates"}}
        completed = self.client.post(f"/api/worker/jobs/{replan_job_id}/callback", json=callback)
        self.assertEqual(completed.status_code, 200, completed.text)
        self.assertEqual(completed.json()["task"]["replan"]["reopened_stages"], ["acceptance", "audit", "frontend", "test"])
        tasks = self.client.get(f"/api/workflows/{workflow_id}").json()["tasks"]
        statuses = {task["stage"]: task["status"] for task in tasks}
        self.assertEqual(statuses["frontend"], "ready")
        self.assertEqual(statuses["backend"], "passed")
        self.assertEqual(statuses["audit"], "blocked")
        self.assertEqual(statuses["test"], "blocked")
        self.assertEqual(statuses["acceptance"], "blocked")

    def test_runtime_must_match_the_job_role(self):
        workflow_id, _, job_id = self.create_started_job("role-gate")
        registered = self.client.post("/api/agent-runtimes/register", json={
            "runtime_id": "runtime-frontend", "agent_key": "frontend-agent", "adapter": "codex", "worker_id": "frontend-worker"})
        self.assertEqual(registered.status_code, 200, registered.text)
        claimed = self.client.post("/api/worker/jobs/claim", json={
            "runtime_id": "runtime-frontend", "worker_id": "frontend-worker", "workflow_id": workflow_id, "job_id": job_id})
        self.assertEqual(claimed.status_code, 200, claimed.text)
        self.assertIsNone(claimed.json()["job"])

    def test_busy_runtime_cannot_be_re_registered_and_exposes_workflow_binding(self):
        workflow_id, _, job_id = self.create_started_job("runtime-binding")
        runtime_id, _ = self.claim(workflow_id, job_id, "bound-worker")
        replaced = self.client.post("/api/agent-runtimes/register", json={
            "runtime_id": runtime_id, "agent_key": "frontend-agent", "adapter": "workbuddy", "worker_id": "bound-worker"})
        self.assertEqual(replaced.status_code, 409, replaced.text)
        runtime = next(item for item in self.client.get("/api/agent-runtimes").json() if item["id"] == runtime_id)
        self.assertEqual(runtime["current_workflow_id"], workflow_id)
        self.assertEqual(runtime["state"], "working")


if __name__ == "__main__":
    unittest.main()
