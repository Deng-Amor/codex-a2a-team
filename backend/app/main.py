import os
import json
import hashlib
import logging
from pathlib import Path
from threading import Event, Thread
from uuid import uuid4
from datetime import datetime, timezone, timedelta

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import DateTime, String, UniqueConstraint, create_engine, select, text as sql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from langgraph.checkpoint.postgres import PostgresSaver

from app.langgraph_loop import GRAPH_VERSION, build_stage_one_graph, new_state

env_file = Path(__file__).resolve().parents[2] / ".env"
for line in env_file.read_text(encoding="utf-8").splitlines() if env_file.exists() else []:
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.strip():
        os.environ.setdefault(key.strip(), value.strip())
engine = create_engine(os.environ["DATABASE_URL"])
Local = sessionmaker(bind=engine)
checkpointer_context = None
checkpointer = None
runtime_reaper_stop = Event()
runtime_reaper_thread = None


class Base(DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = "agents"
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(80), unique=True)
    name: Mapped[str] = mapped_column(String(80))
    role: Mapped[str] = mapped_column(String(80))
    capabilities: Mapped[str] = mapped_column(String, default="")


class Stage(Base):
    __tablename__ = "workflow_stages"
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(80), unique=True)
    agent_key: Mapped[str] = mapped_column(String(80))
    depends_on: Mapped[str] = mapped_column(String, default="")


class Workflow(Base):
    __tablename__ = "workflows"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    request: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String(30), default="running")
    context_summary: Mapped[str] = mapped_column(String, default="")
    # A workflow never changes engines while it is running.  The LangGraph
    # checkpointer is runtime state; these fields are the durable audit view.
    engine: Mapped[str] = mapped_column(String(40), default="langgraph_v1")
    graph_version: Mapped[str] = mapped_column(String(40), default="stage1")
    state_schema_version: Mapped[int] = mapped_column(default=1)
    thread_id: Mapped[str] = mapped_column(String(80), default="")
    next_event_sequence: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    last_activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))


class AgentRuntime(Base):
    """A real, heartbeat-backed execution instance; never a planned DAG node."""
    __tablename__ = "agent_runtimes"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    agent_key: Mapped[str] = mapped_column(String(80))
    adapter: Mapped[str] = mapped_column(String(40))
    worker_id: Mapped[str] = mapped_column(String(120), unique=True)
    state: Mapped[str] = mapped_column(String(30), default="idle")
    session_ref: Mapped[str] = mapped_column(String(200), default="")
    worktree_path: Mapped[str] = mapped_column(String, default="")
    current_job_id: Mapped[str] = mapped_column(String(80), default="")
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"), onupdate=lambda: datetime.now(timezone.utc))


class Task(Base):
    __tablename__ = "workflow_tasks"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    stage_key: Mapped[str] = mapped_column(String(80))
    agent_key: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(30))
    depends_on: Mapped[str] = mapped_column(String, default="")
    iterations: Mapped[int] = mapped_column(default=0)
    instructions: Mapped[str] = mapped_column(String, default="")
    plan: Mapped[str] = mapped_column(String, default="")
    artifacts: Mapped[str] = mapped_column(String, default="")
    execution_log: Mapped[str] = mapped_column(String, default="")
    handoff_summary: Mapped[str] = mapped_column(String, default="")
    attempt_id: Mapped[int] = mapped_column(default=0)
    lease_version: Mapped[int] = mapped_column(default=0)
    worker_session_id: Mapped[str] = mapped_column(String(120), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"), onupdate=lambda: datetime.now(timezone.utc))


class Message(Base):
    __tablename__ = "a2a_messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    task_id: Mapped[str] = mapped_column(String(80))
    from_agent: Mapped[str] = mapped_column(String(80))
    to_agent: Mapped[str] = mapped_column(String(80))
    text: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String(20), default="handoff")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))


class TaskAttempt(Base):
    __tablename__ = "workflow_task_attempts"
    __table_args__ = (UniqueConstraint("task_id", "attempt_number", name="uq_task_attempt_number"),
                      UniqueConstraint("idempotency_key", name="uq_task_attempt_idempotency"))
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    task_id: Mapped[str] = mapped_column(String(80))
    attempt_number: Mapped[int] = mapped_column()
    lease_version: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(30), default="recorded")
    worker_session_id: Mapped[str] = mapped_column(String(120), default="")
    runtime_id: Mapped[str] = mapped_column(String(80), default="")
    external_session_id: Mapped[str] = mapped_column(String(200), default="")
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WorkerJob(Base):
    __tablename__ = "worker_jobs"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    task_id: Mapped[str] = mapped_column(String(80), unique=True)
    job_type: Mapped[str] = mapped_column(String(40))
    required_agent_key: Mapped[str] = mapped_column(String(80), default="")
    runtime_id: Mapped[str] = mapped_column(String(80), default="")
    status: Mapped[str] = mapped_column(String(30), default="queued")
    attempt_id: Mapped[int] = mapped_column(default=0)
    lease_version: Mapped[int] = mapped_column(default=0)
    worker_id: Mapped[str] = mapped_column(String(120), default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    callback_id: Mapped[str] = mapped_column(String(120), default="")
    result_payload: Mapped[str] = mapped_column(String, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"), onupdate=lambda: datetime.now(timezone.utc))


class WorkflowEvent(Base):
    __tablename__ = "workflow_events"
    __table_args__ = (UniqueConstraint("workflow_id", "sequence", name="uq_workflow_event_sequence"),
                      UniqueConstraint("idempotency_key", name="uq_workflow_event_idempotency"))
    id: Mapped[int] = mapped_column(primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    sequence: Mapped[int] = mapped_column()
    event_type: Mapped[str] = mapped_column(String(80))
    task_id: Mapped[str] = mapped_column(String(80), default="")
    payload: Mapped[str] = mapped_column(String, default="{}")
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    workflow_event_id: Mapped[int] = mapped_column(unique=True)
    event_type: Mapped[str] = mapped_column(String(80))
    payload: Mapped[str] = mapped_column(String, default="{}")
    status: Mapped[str] = mapped_column(String(30), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))


class Defect(Base):
    __tablename__ = "workflow_defects"
    __table_args__ = (UniqueConstraint("workflow_id", "content_hash", name="uq_workflow_defect_hash"),)
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    task_id: Mapped[str] = mapped_column(String(80), default="")
    owner_agent: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(30), default="open")
    content: Mapped[str] = mapped_column(String)
    content_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"), onupdate=lambda: datetime.now(timezone.utc))


DEFAULT_AGENTS = [
    ("team-lead", "Team Lead", "lead"),
    ("task-decomposer", "任务拆分", "planner"),
    ("architecture-agent", "架构设计", "architect"),
    ("product-agent", "产品验收", "product"),
    ("frontend-agent", "前端开发", "frontend"),
    ("backend-agent", "后端开发", "backend"),
    ("audit-agent", "代码审计", "auditor"),
    ("test-agent", "测试验证", "tester"),
    ("deployment-agent", "部署发布", "deployer"),
]
DEFAULT_STAGES = [
    ("decompose", "task-decomposer", ""),
    ("architecture", "architecture-agent", "decompose"),
    ("product", "product-agent", "architecture"),
    ("frontend", "frontend-agent", "product"),
    ("backend", "backend-agent", "product"),
    ("audit", "audit-agent", "frontend,backend"),
    ("test", "test-agent", "audit"),
    ("document_review", "product-agent", "team_lead"),
    ("acceptance", "product-agent", "test"),
    ("deploy", "deployment-agent", "acceptance"),
]
FRONTEND_ONLY = [
    ("decompose", "task-decomposer", ""),
    ("frontend", "frontend-agent", "decompose"),
    ("audit", "audit-agent", "frontend"),
    ("test", "test-agent", "audit"),
    ("acceptance", "product-agent", "test"),
]

# Team Lead owns the contract.  No implementation task may run before its audit.
LEAD_GATE = [
    ("team_lead", "team-lead", ""),
    ("contract_audit", "audit-agent", "team_lead"),
]
TEAM_FULL = [
    ("frontend", "frontend-agent", "contract_audit"),
    ("backend", "backend-agent", "contract_audit"),
    ("audit", "audit-agent", "frontend,backend"),
    ("test", "test-agent", "audit"),
    ("acceptance", "product-agent", "test"),
]
TEAM_FRONTEND_ONLY = [
    ("frontend", "frontend-agent", "contract_audit"),
    ("audit", "audit-agent", "frontend"),
    ("test", "test-agent", "audit"),
    ("acceptance", "product-agent", "test"),
]
TEAM_BACKEND_ONLY = [
    ("backend", "backend-agent", "contract_audit"),
    ("audit", "audit-agent", "backend"),
    ("test", "test-agent", "audit"),
    ("acceptance", "product-agent", "test"),
]
TEAM_WORKFLOW_VALIDATION = [
    ("workflow_validation", "test-agent", "team_lead"),
    ("acceptance", "product-agent", "workflow_validation"),
]
TEAM_DOCUMENTATION = [
    ("document_review", "product-agent", "team_lead"),
    ("acceptance", "product-agent", "document_review"),
]
MAX_TASK_ITERATIONS = 3
MAX_REPEAT_MESSAGES = 3
RECENT_MESSAGES = 12
WORKER_JOB_TYPES = {"team_lead", "contract_audit", "frontend", "backend", "audit", "test", "workflow_validation", "document_review"}
WORKER_LEASE_SECONDS = 60
AGENT_RUNTIME_TTL_SECONDS = 90

# There is no Alembic project yet. Keep the compatibility migration explicit
# and idempotent until the project adopts a real migration revision chain.
SCHEMA_MIGRATIONS = {
    "20260720_langgraph_stage1": (
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS engine VARCHAR(40) NOT NULL DEFAULT 'python_legacy'",
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS graph_version VARCHAR(40) NOT NULL DEFAULT 'stage1'",
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS state_schema_version INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS thread_id VARCHAR(80) NOT NULL DEFAULT ''",
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS next_event_sequence INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS attempt_id INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS lease_version INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS worker_session_id VARCHAR(120) NOT NULL DEFAULT ''",
    ),
    "20260720_workflow_created_at": (
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ),
    "20260720_workflow_created_at_backfill": (
        "UPDATE workflows SET created_at = COALESCE((SELECT MIN(created_at) FROM workflow_events WHERE workflow_events.workflow_id = workflows.id), created_at)",
    ),
    "20260720_workflow_last_activity": (
        "ALTER TABLE workflows ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "UPDATE workflows SET last_activity_at = COALESCE((SELECT MAX(created_at) FROM workflow_events WHERE workflow_events.workflow_id = workflows.id), created_at)",
    ),
    "20260721_agent_runtime": (
        "ALTER TABLE workflow_task_attempts ADD COLUMN IF NOT EXISTS runtime_id VARCHAR(80) NOT NULL DEFAULT ''",
        "ALTER TABLE workflow_task_attempts ADD COLUMN IF NOT EXISTS external_session_id VARCHAR(200) NOT NULL DEFAULT ''",
        "ALTER TABLE worker_jobs ADD COLUMN IF NOT EXISTS required_agent_key VARCHAR(80) NOT NULL DEFAULT ''",
        "ALTER TABLE worker_jobs ADD COLUMN IF NOT EXISTS runtime_id VARCHAR(80) NOT NULL DEFAULT ''",
        "UPDATE worker_jobs AS job SET required_agent_key = task.agent_key FROM workflow_tasks AS task WHERE job.task_id = task.id AND job.required_agent_key = ''",
    ),
}


def route_for(request: str):
    normalized = request.replace(" ", "")
    is_documentation = any(flag in normalized.lower() for flag in ("prd", "产品需求文档", "需求文档", "流程文档", "文档评审"))
    if is_documentation:
        return "documentation", TEAM_DOCUMENTATION
    is_flow_validation = "a2a" in normalized.lower() and any(flag in normalized for flag in ("验证", "自检", "流程测试", "流程验收"))
    if is_flow_validation:
        return "workflow_validation", TEAM_WORKFLOW_VALIDATION
    has_frontend = "前端" in normalized or "前后端" in normalized
    frontend_only = has_frontend and any(flag in normalized for flag in ("后端数据不修改", "后端不修改", "接口不变", "数据库不修改"))
    backend_only = "后端" in normalized and not has_frontend
    if frontend_only:
        return "frontend_only", TEAM_FRONTEND_ONLY
    if backend_only:
        return "backend_only", TEAM_BACKEND_ONLY
    return "full", TEAM_FULL


def decode(value: str):
    return json.loads(value) if value else []


def payload_hash(value: str):
    return hashlib.sha256(value.encode()).hexdigest()


def event_response(session: Session, workflow_id: str, idempotency_key: str):
    if not idempotency_key:
        return None
    event = session.scalar(select(WorkflowEvent).where(
        WorkflowEvent.workflow_id == workflow_id, WorkflowEvent.idempotency_key == idempotency_key))
    if not event:
        return None
    return decode(event.payload).get("response")


def append_event(session: Session, workflow: Workflow, event_type: str, *, task_id="", payload=None, idempotency_key=""):
    """Write the audit event and its outbox record in the business transaction."""
    locked = session.execute(select(Workflow).where(Workflow.id == workflow.id).with_for_update()).scalar_one()
    locked.next_event_sequence += 1
    locked.last_activity_at = datetime.now(timezone.utc)
    event = WorkflowEvent(workflow_id=workflow.id, sequence=locked.next_event_sequence, event_type=event_type,
                          task_id=task_id, payload=json.dumps(payload or {}, ensure_ascii=False),
                          idempotency_key=idempotency_key or f"event_{uuid4().hex}")
    session.add(event)
    session.flush()
    session.add(OutboxEvent(id="outbox_" + uuid4().hex, workflow_id=workflow.id, workflow_event_id=event.id,
                            event_type=event_type, payload=event.payload))
    return event


def task_data(item: Task, details=False):
    data = {"id": item.id, "stage": item.stage_key, "agent": item.agent_key, "status": item.status,
            "depends_on": item.depends_on.split(",") if item.depends_on else [], "iterations": item.iterations,
            "attempt_id": item.attempt_id, "lease_version": item.lease_version,
            "updated_at": item.updated_at.isoformat()}
    if details:
        data["detail"] = {"instructions": item.instructions, "plan": decode(item.plan), "artifacts": decode(item.artifacts),
                          "execution_log": decode(item.execution_log), "handoff_summary": item.handoff_summary}
    return data


def runtime_is_live(runtime: AgentRuntime) -> bool:
    return runtime.last_heartbeat_at >= datetime.now(timezone.utc) - timedelta(seconds=AGENT_RUNTIME_TTL_SECONDS)


def runtime_data(session: Session, runtime: AgentRuntime):
    live = runtime_is_live(runtime)
    job = session.get(WorkerJob, runtime.current_job_id) if runtime.current_job_id else None
    return {"id": runtime.id, "agent_key": runtime.agent_key, "adapter": runtime.adapter,
            "worker_id": runtime.worker_id, "state": runtime.state if live else "offline",
            "session_ref": runtime.session_ref, "worktree_path": runtime.worktree_path,
            "current_job_id": runtime.current_job_id,
            "current_workflow_id": job.workflow_id if job else "",
            "last_heartbeat_at": runtime.last_heartbeat_at.isoformat(),
            "live": live, "updated_at": runtime.updated_at.isoformat()}


def reap_expired(session: Session) -> int:
    now = datetime.now(timezone.utc)
    jobs = session.scalars(select(WorkerJob).where(WorkerJob.status.in_(("leased", "running")),
                                                   WorkerJob.lease_expires_at < now)).all()
    for job in jobs:
        runtime = session.get(AgentRuntime, job.runtime_id) if job.runtime_id else None
        if runtime and runtime.current_job_id == job.id:
            runtime.state, runtime.current_job_id = "idle", ""
        job.status, job.worker_id, job.runtime_id, job.lease_expires_at = "queued", "", "", None
    if jobs:
        session.commit()
    return len(jobs)


def runtime_reaper_loop():
    while not runtime_reaper_stop.wait(10):
        try:
            with Local() as session:
                reap_expired(session)
        except Exception:
            logging.getLogger(__name__).exception("agent runtime reaper failed")


def queue_worker_attempt(session: Session, workflow: Workflow, task: Task, *, event_type: str, detail: str):
    """Create exactly one executable attempt for a task that has been reopened."""
    if task.stage_key not in WORKER_JOB_TYPES:
        return None
    job = session.scalar(select(WorkerJob).where(WorkerJob.task_id == task.id))
    if job and job.status in {"queued", "leased", "running"} and job.attempt_id == task.attempt_id:
        return job

    task.attempt_id += 1
    if not job:
        job = WorkerJob(id="job_" + uuid4().hex, workflow_id=workflow.id, task_id=task.id,
                        job_type=task.stage_key, required_agent_key=task.agent_key)
        session.add(job)
    job.status, job.attempt_id, job.worker_id, job.callback_id = "queued", task.attempt_id, "", ""
    job.required_agent_key, job.runtime_id = task.agent_key, ""
    job.lease_expires_at = job.heartbeat_at = None
    job.result_payload = "{}"
    session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow.id, task_id=task.id,
                            attempt_number=task.attempt_id, lease_version=task.lease_version, status="queued",
                            worker_session_id="", idempotency_key=f"{event_type}:{task.id}:{task.attempt_id}"))
    append_event(session, workflow, event_type, task_id=task.id,
                 payload={"attempt_id": task.attempt_id, "job_id": job.id, "detail": detail},
                 idempotency_key=f"{event_type}:{task.id}:{task.attempt_id}")
    return job


def queue_repair_regression(session: Session, workflow: Workflow, owner: Task, content: str, source: Task | None = None):
    """Reopen one owner and only its downstream gates for a defect regression."""
    owner.status = "repairing"
    owner.instructions = f"正在修复：{content}"
    owner_log = decode(owner.execution_log)
    owner_log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "缺陷已接收", "detail": owner.instructions})
    owner.execution_log = json.dumps(owner_log, ensure_ascii=False)
    repair_job = queue_worker_attempt(session, workflow, owner, event_type="task.repair.queued", detail=content)
    if repair_job:
        owner_log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "修复任务已排队",
                          "detail": f"修复 attempt {owner.attempt_id} 已创建，Worker Job：{repair_job.id}"})
        owner.execution_log = json.dumps(owner_log, ensure_ascii=False)

    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow.id)).all()
    reopen = {owner.stage_key}
    changed = True
    while changed:
        changed = False
        for item in tasks:
            dependencies = set(filter(None, item.depends_on.split(",")))
            if item.stage_key not in reopen and dependencies & reopen:
                reopen.add(item.stage_key)
                changed = True
    for item in tasks:
        if item.stage_key == owner.stage_key or item.stage_key not in reopen:
            continue
        item.status = "blocked"
        log = decode(item.execution_log)
        log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "等待回归", "detail": f"等待 {owner.stage_key} 修复缺陷后重新执行。"})
        item.execution_log = json.dumps(log, ensure_ascii=False)
    sender = source.agent_key if source else "dashboard"
    session.add(Message(workflow_id=workflow.id, task_id=owner.id, from_agent=sender,
                        to_agent=owner.agent_key, kind="challenge",
                        text=f"驳回/缺陷回派：{content}；已重新发布给 {owner.agent_key}，修复 Job 已排队。"))


def audit_inputs_ready(session: Session, workflow_id: str, audit_task: Task, by_stage: dict[str, Task]) -> bool:
    """The persisted audit dependencies define this iteration's fan-in."""
    selected = [by_stage[key] for key in audit_task.depends_on.split(",") if key]
    if not selected or any(task.status != "passed" for task in selected):
        return False
    if session.scalar(select(Defect.id).where(Defect.workflow_id == workflow_id,
                                              Defect.owner_agent.in_([task.agent_key for task in selected]),
                                              Defect.status.in_(("open", "assigned", "reopened")))):
        return False
    for task in selected:
        evidence = [item for item in decode(task.execution_log) if item.get("event") == "交付证据"]
        if not any(item.get("attempt_id") == task.attempt_id for item in evidence):
            return False
    return True


def task_detail(stage: str, workflow: Workflow, route: str):
    contract = [
        {"operation": "GET /api/v1/resources", "purpose": "分页查询资源", "response": '{"items": [{"id": "string", "name": "string", "status": "active"}], "total": 0}'},
        {"operation": "GET /api/v1/resources/{id}", "purpose": "读取资源详情", "response": '{"id": "string", "name": "string", "status": "active", "updated_at": "ISO-8601"}'},
        {"operation": "POST /api/v1/resources", "purpose": "创建资源", "request": '{"name": "string"}', "response": "201 + resource"},
        {"operation": "PATCH /api/v1/resources/{id}", "purpose": "更新资源", "request": '{"name": "string", "status": "active|inactive"}', "response": "200 + resource"},
    ]
    lead_detail = ("澄清需求并输出可审计的实施边界与 REST API Contract；开发任务在方案审计通过前不得启动。",
                   ["确认范围：" + workflow.request, "定义前后端并行边界与验收条件", "输出 REST API Contract，等待方案审计"],
                   [{"name": "REST API Contract", "type": "api_contract", "content": contract}, {"name": "验收标准", "type": "acceptance", "content": ["接口字段与状态枚举一致", "前端可使用 Mock 完成独立开发", "真实 API 接入后通过集成测试"]}],
                   "将方案、接口契约及风险交给方案审计 Agent。")
    if route == "documentation":
        lead_detail = ("梳理文档目标、读者、范围、流程、角色和验收标准，产出可供团队直接阅读的 PRD。",
                       ["确认文档读者与使用场景：" + workflow.request, "整理主流程、异常分支和职责边界", "输出 PRD 草案，等待文档评审"],
                       [{"name": "PRD 草案", "type": "document", "content": ["产品目标", "流程与角色", "边界与验收标准"]}],
                       "将 PRD 草案交给文档评审 Agent。")
    details = {
        "team_lead": lead_detail,
        "contract_audit": ("审查 Team Lead 的需求边界、API 风格、字段完整性和前后端并行可行性。",
                           ["检查 REST 命名、状态码和错误响应", "检查请求/响应字段是否足够前端 Mock", "通过后释放开发任务"],
                           [{"name": "Contract 审核清单", "type": "review", "content": ["路径使用复数资源名", "写操作返回明确状态码", "字段、枚举、错误场景可实现"]}],
                           "通过后向前端和后端发送已审核 Contract。"),
        "document_review": ("审阅 PRD 或流程文档的目标、范围、角色、流程、边界和验收标准，确保同事可直接理解和使用。",
                            ["核对文档是否说明目标读者和业务背景", "核对流程、角色、异常分支与验收标准", "输出可读性和缺漏审阅结论"],
                            [{"name": "文档评审清单", "type": "review", "content": ["目标与范围明确", "流程与角色完整", "异常与验收可执行", "术语与示例可理解"]}],
                            "通过后将文档评审结论交给人工验收。"),
        "frontend": ("按审核通过的 Contract 使用 Mock 开发页面；真实 API 可用后完成 Mock 到 API 的切换。",
                     ["依据 Contract 定义前端类型和 Mock", "实现页面、加载、空态和错误态", "接入真实 API 并回归"],
                     [{"name": "前端交付", "type": "implementation", "content": ["页面与交互", "Mock 数据适配器", "API 接入检查"]}],
                     "提交改动文件、Mock/真实 API 切换说明给代码审计。"),
        "backend": ("按审核通过的 Contract 实现 API、校验、持久化和服务端测试。",
                    ["实现路由与请求校验", "实现 PostgreSQL 持久化", "输出示例响应并执行 API 测试"],
                    [{"name": "后端交付", "type": "implementation", "content": ["REST 路由", "数据库迁移", "API 测试结果"]}],
                    "交付 API 地址、字段约束、示例响应和测试结果给前端及审计。"),
        "audit": ("审查代码、接口契约一致性、边界处理与可维护性；问题退回责任开发 Agent。",
                  ["检查 Contract 一致性", "检查输入验证、错误处理和测试", "输出通过或缺陷清单"],
                  [{"name": "审计结果", "type": "review", "content": ["Contract 一致性", "输入校验", "错误处理", "可访问性基础"]}],
                  "通过后将审计结论交给测试验证。"),
        "test": ("执行功能、接口集成和回归测试；失败时按归属回派前端或后端。",
                 ["准备关键验收用例", "验证 Mock 切换真实 API", "记录失败复现与回归结论"],
                 [{"name": "测试清单", "type": "test_plan", "content": ["正常流程", "空数据", "接口错误", "前后端集成"]}],
                 "交付通过的测试报告，或将可复现缺陷退回开发。"),
        "acceptance": ("按需求和验收标准进行产品验收，确认用户可完成目标流程。",
                       ["核对需求范围", "核对测试证据", "给出验收或退回结论"],
                       [{"name": "验收清单", "type": "acceptance", "content": ["需求覆盖", "交互可用", "关键数据正确"]}],
                       "验收通过后交给发布节点；不通过则回到 Team Lead 协调。"),
    }
    instructions, plan, artifacts, handoff = details.get(stage, ("完成本节点的已分配工作。", ["执行任务", "记录结果"], [], "向下一节点交付结果。"))
    return {"instructions": instructions, "plan": plan, "artifacts": artifacts,
            "execution_log": [{"at": datetime.now(timezone.utc).isoformat(), "event": "任务已创建", "detail": f"路由：{route}；等待执行。"}], "handoff_summary": handoff}


def refresh_summary(workflow: Workflow, messages: list[Message]):
    older = messages[:-RECENT_MESSAGES]
    if older:
        workflow.context_summary = f"已压缩 {len(older)} 条早期上下文；最近摘要：{older[-1].from_agent}→{older[-1].to_agent}：{older[-1].text[:120]}"

app = FastAPI(title="A2A Control Plane")
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:20002", "http://localhost:20002"], allow_methods=["*"], allow_headers=["*"])


def db():
    with Local() as session:
        yield session


@app.on_event("startup")
def boot():
    global checkpointer_context, checkpointer, runtime_reaper_thread
    Base.metadata.create_all(engine)
    with Local() as session:
        session.execute(sql("CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(80) PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"))
        applied = set(session.scalars(sql("SELECT version FROM schema_migrations")).all())
        for version, statements in SCHEMA_MIGRATIONS.items():
            if version not in applied:
                for statement in statements:
                    session.execute(sql(statement))
                session.execute(sql("INSERT INTO schema_migrations (version) VALUES (:version)"), {"version": version})
        # Existing rows predate the graph runtime. New workflows explicitly set
        # their own thread_id and engine in create_workflow().
        session.execute(sql("UPDATE workflows SET engine = 'python_legacy' WHERE thread_id = ''"))
        session.execute(sql("ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"))
        session.execute(sql("ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS iterations INTEGER NOT NULL DEFAULT 0"))
        for column in ("instructions", "plan", "artifacts", "execution_log", "handoff_summary"):
            session.execute(sql(f"ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS {column} TEXT NOT NULL DEFAULT ''"))
        session.execute(sql("ALTER TABLE workflows ADD COLUMN IF NOT EXISTS context_summary TEXT NOT NULL DEFAULT ''"))
        session.execute(sql("ALTER TABLE a2a_messages ADD COLUMN IF NOT EXISTS kind VARCHAR(20) NOT NULL DEFAULT 'handoff'"))
        existing_agents = {item.key for item in session.scalars(select(Agent))}
        existing_stages = {item.key for item in session.scalars(select(Stage))}
        session.add_all(Agent(key=key, name=name, role=role) for key, name, role in DEFAULT_AGENTS if key not in existing_agents)
        session.add_all(Stage(key=key, agent_key=agent, depends_on=deps) for key, agent, deps in DEFAULT_STAGES if key not in existing_stages)
        session.flush()
        for task in session.scalars(select(Task).where(Task.instructions == "")):
            workflow = session.get(Workflow, task.workflow_id)
            if workflow:
                detail = task_detail(task.stage_key, workflow, route_for(workflow.request)[0])
                task.instructions = detail["instructions"]
                task.plan = json.dumps(detail["plan"], ensure_ascii=False)
                task.artifacts = json.dumps(detail["artifacts"], ensure_ascii=False)
                task.execution_log = json.dumps(detail["execution_log"], ensure_ascii=False)
                task.handoff_summary = detail["handoff_summary"]
        for workflow in session.scalars(select(Workflow).where(Workflow.status == "running")):
            acceptance = stage_task(session, workflow.id, "acceptance")
            lead = stage_task(session, workflow.id, "team_lead")
            if not acceptance or not lead or acceptance.status != "blocked" or not lead.instructions.startswith("验收驳回："):
                continue
            existing_job = session.scalar(select(WorkerJob).where(WorkerJob.task_id == lead.id))
            if lead.status in {"running", "repairing"} or (existing_job and existing_job.status in {"queued", "leased", "running"}):
                continue
            lead.status, lead.worker_session_id = "queued", ""
            lead.attempt_id += 1
            if not existing_job:
                existing_job = WorkerJob(id="job_" + uuid4().hex, workflow_id=workflow.id, task_id=lead.id,
                                         job_type="team_lead", required_agent_key=lead.agent_key,
                                         attempt_id=lead.attempt_id, status="queued")
                session.add(existing_job)
            else:
                existing_job.status, existing_job.attempt_id, existing_job.worker_id, existing_job.callback_id = "queued", lead.attempt_id, "", ""
                existing_job.required_agent_key, existing_job.runtime_id = lead.agent_key, ""
                existing_job.lease_expires_at = existing_job.heartbeat_at = None
                existing_job.result_payload = "{}"
            session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow.id, task_id=lead.id,
                                    attempt_number=lead.attempt_id, lease_version=lead.lease_version, status="queued",
                                    worker_session_id="", runtime_id="", external_session_id="",
                                    idempotency_key=f"replan_{lead.id}_{lead.attempt_id}"))
        # A previous process may have stopped after a defect moved an owner to
        # repairing but before its worker was queued. Recover that durable state.
        for task in session.scalars(select(Task).where(Task.status == "repairing")):
            workflow = session.get(Workflow, task.workflow_id)
            defect = session.scalar(select(Defect).where(Defect.workflow_id == task.workflow_id,
                                                          Defect.owner_agent == task.agent_key,
                                                          Defect.status.in_(("open", "assigned", "reopened"))).order_by(Defect.created_at))
            if workflow and defect:
                queue_worker_attempt(session, workflow, task, event_type="task.repair.recovered", detail=defect.content)
        for task in session.scalars(select(Task).where(Task.status == "ready", Task.stage_key.in_(WORKER_JOB_TYPES))):
            workflow = session.get(Workflow, task.workflow_id)
            if workflow and workflow.status == "running":
                queue_worker_attempt(session, workflow, task, event_type="task.queued", detail="依赖已满足，等待 Worker 领取。")
        session.commit()
    checkpoint_url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    checkpointer_context = PostgresSaver.from_conn_string(checkpoint_url)
    checkpointer = checkpointer_context.__enter__()
    checkpointer.setup()
    runtime_reaper_stop.clear()
    if not runtime_reaper_thread or not runtime_reaper_thread.is_alive():
        runtime_reaper_thread = Thread(target=runtime_reaper_loop, name="agent-runtime-reaper", daemon=True)
        runtime_reaper_thread.start()


@app.on_event("shutdown")
def close_checkpointer():
    global checkpointer_context, checkpointer, runtime_reaper_thread
    runtime_reaper_stop.set()
    if runtime_reaper_thread and runtime_reaper_thread.is_alive():
        runtime_reaper_thread.join(timeout=2)
    runtime_reaper_thread = None
    if checkpointer_context:
        checkpointer_context.__exit__(None, None, None)
        checkpointer_context = None
        checkpointer = None


@app.get("/api/agents")
def agents(session: Session = Depends(db)):
    return [{"key": item.key, "name": item.name, "role": item.role, "capabilities": item.capabilities} for item in session.scalars(select(Agent))]


@app.post("/api/agents")
def add_agent(agent: dict, session: Session = Depends(db)):
    item = Agent(**agent)
    session.add(item)
    session.commit()
    return {"key": item.key, "name": item.name, "role": item.role}


@app.get("/api/agent-runtimes")
def agent_runtimes(session: Session = Depends(db)):
    return [runtime_data(session, item) for item in session.scalars(select(AgentRuntime).order_by(AgentRuntime.updated_at.desc()))]


@app.post("/api/agent-runtimes/register")
def register_agent_runtime(payload: dict, session: Session = Depends(db)):
    runtime_id = str(payload.get("runtime_id", "")).strip()
    agent_key = str(payload.get("agent_key", "")).strip()
    adapter = str(payload.get("adapter", "")).strip()
    worker_id = str(payload.get("worker_id", "")).strip()
    if not runtime_id or not agent_key or not adapter or not worker_id:
        raise HTTPException(422, "runtime_id, agent_key, adapter, and worker_id are required")
    if not session.scalar(select(Agent).where(Agent.key == agent_key)):
        raise HTTPException(422, "agent_key is not registered")
    runtime = session.get(AgentRuntime, runtime_id)
    if runtime and runtime.worker_id != worker_id:
        raise HTTPException(409, "runtime_id is already owned by another worker")
    if runtime and runtime.current_job_id:
        raise HTTPException(409, "busy runtime must use its heartbeat endpoint, not register")
    duplicate_worker = session.scalar(select(AgentRuntime).where(AgentRuntime.worker_id == worker_id,
                                                                  AgentRuntime.id != runtime_id))
    if duplicate_worker:
        raise HTTPException(409, "worker_id is already registered by another runtime")
    if not runtime:
        runtime = AgentRuntime(id=runtime_id, agent_key=agent_key, adapter=adapter, worker_id=worker_id)
        session.add(runtime)
    runtime.agent_key, runtime.adapter = agent_key, adapter
    runtime.session_ref = str(payload.get("session_ref", runtime.session_ref)).strip()
    runtime.worktree_path = str(payload.get("worktree_path", runtime.worktree_path)).strip()
    runtime.state, runtime.last_heartbeat_at = "idle", datetime.now(timezone.utc)
    session.commit()
    return runtime_data(session, runtime)


@app.post("/api/agent-runtimes/{runtime_id}/heartbeat")
def runtime_heartbeat(runtime_id: str, payload: dict, session: Session = Depends(db)):
    runtime = session.get(AgentRuntime, runtime_id)
    if not runtime or runtime.worker_id != str(payload.get("worker_id", "")).strip():
        raise HTTPException(409, "unknown runtime or worker")
    runtime.last_heartbeat_at = datetime.now(timezone.utc)
    if payload.get("session_ref") is not None:
        runtime.session_ref = str(payload["session_ref"]).strip()
    if payload.get("worktree_path") is not None:
        runtime.worktree_path = str(payload["worktree_path"]).strip()
    session.commit()
    return runtime_data(session, runtime)


@app.get("/api/workflow-stages")
def stages(session: Session = Depends(db)):
    return [{"key": item.key, "agent_key": item.agent_key, "depends_on": item.depends_on.split(",") if item.depends_on else []} for item in session.scalars(select(Stage))]


@app.post("/api/workflow-stages")
def add_stage(stage: dict, session: Session = Depends(db)):
    item = Stage(key=stage["key"], agent_key=stage["agent_key"], depends_on=",".join(stage.get("depends_on", [])))
    session.add(item)
    session.commit()
    return {"key": item.key}


@app.post("/api/workflows")
def create_workflow(payload: dict, session: Session = Depends(db)):
    if not payload.get("title") or not payload.get("request"):
        raise HTTPException(422, "title and request are required")
    engine_name = payload.get("engine", "langgraph_v1")
    if engine_name not in {"langgraph_v1", "node_legacy"}:
        raise HTTPException(422, "unsupported engine")
    workflow_id = "wf_" + uuid4().hex[:8]
    route, stages = route_for(payload["request"])
    context_summary = "等待 Team Lead 产出 PRD 草案。" if route == "documentation" else "等待 Team Lead 产出方案与 REST API Contract。"
    workflow = Workflow(id=workflow_id, thread_id=workflow_id, engine=engine_name,
                        graph_version=payload.get("graph_version", GRAPH_VERSION), state_schema_version=1,
                        title=payload["title"], request=payload["request"], context_summary=context_summary)
    session.add(workflow)
    lead_gate = [LEAD_GATE[0]] if route in {"workflow_validation", "documentation"} else LEAD_GATE
    for key, agent, dependencies in lead_gate + stages:
        detail = task_detail(key, workflow, route)
        session.add(Task(id=f"{workflow_id}_{key}", workflow_id=workflow_id, stage_key=key, agent_key=agent,
                         status="ready" if not dependencies else "blocked", depends_on=dependencies,
                         instructions=detail["instructions"], plan=json.dumps(detail["plan"], ensure_ascii=False),
                         artifacts=json.dumps(detail["artifacts"], ensure_ascii=False), execution_log=json.dumps(detail["execution_log"], ensure_ascii=False),
                         handoff_summary=detail["handoff_summary"]))
    summary = "仅激活前端、审计、测试与验收链路。" if route == "frontend_only" else "需求已确认，等待 Team Lead 编排。"
    session.add(Message(workflow_id=workflow_id, task_id=f"{workflow_id}_team_lead", from_agent="codex", to_agent="team-lead", text=summary, kind="handoff"))
    session.flush()
    append_event(session, workflow, "workflow.created", payload={"response": {"id": workflow_id, "status": "running", "route": route, "gate": "contract_audit"}, "engine": engine_name})
    session.commit()
    return {"id": workflow_id, "status": "running", "route": route, "gate": "contract_audit", "engine": engine_name, "thread_id": workflow_id}


@app.get("/api/workflows")
def workflows(session: Session = Depends(db)):
    return [{"id": item.id, "title": item.title, "status": item.status, "engine": item.engine,
             "graph_version": item.graph_version, "thread_id": item.thread_id, "created_at": item.created_at.isoformat(),
             "last_activity_at": item.last_activity_at.isoformat()}
            for item in session.scalars(select(Workflow).where(Workflow.status != "invalidated").order_by(Workflow.last_activity_at.desc()))]


@app.get("/api/workflows/{workflow_id}")
def get_workflow(workflow_id: str, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    messages = session.scalars(select(Message).where(Message.workflow_id == workflow_id).order_by(Message.id.desc())).all()
    return {"id": workflow.id, "title": workflow.title, "status": workflow.status, "engine": workflow.engine,
            "graph_version": workflow.graph_version, "state_schema_version": workflow.state_schema_version, "thread_id": workflow.thread_id,
            "context_summary": workflow.context_summary,
            "tasks": [task_data(item, details=True) for item in tasks],
            "messages": [{"id": item.id, "task_id": item.task_id, "from": item.from_agent, "to": item.to_agent, "text": item.text, "kind": item.kind, "created_at": item.created_at.isoformat()} for item in messages]}


@app.get("/api/workflows/{workflow_id}/tasks/{task_id}")
def get_task(workflow_id: str, task_id: str, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    return task_data(task, details=True)


@app.get("/api/workflows/{workflow_id}/context")
def context(workflow_id: str, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    messages = session.scalars(select(Message).where(Message.workflow_id == workflow_id).order_by(Message.id.desc()).limit(RECENT_MESSAGES)).all()
    return {"summary": workflow.context_summary, "recent_messages": [{"from": item.from_agent, "to": item.to_agent, "text": item.text, "kind": item.kind} for item in reversed(messages)]}


def stage_task(session: Session, workflow_id: str, stage_key: str):
    return session.scalar(select(Task).where(Task.workflow_id == workflow_id, Task.stage_key == stage_key))


def persist_stage_one_result(session: Session, workflow: Workflow, result: dict, *, simulate_workers: bool = False):
    """Project pure LangGraph events into the dashboard's business ledger."""
    task_by_node = {"team_lead": "team_lead", "contract_audit": "contract_audit", "frontend": "frontend", "audit": "audit"}
    for item in result.get("events", []):
        # The graph's frontend/audit nodes are a deterministic loop test until
        # the lease-based Codex Worker lands.  Never fake a real delivery.
        if not simulate_workers and item["node"] in {"frontend", "audit"}:
            continue
        duplicate = session.scalar(select(WorkflowEvent.id).where(WorkflowEvent.workflow_id == workflow.id,
                                                                   WorkflowEvent.idempotency_key == item["idempotency_key"]))
        if duplicate:
            continue
        stage_key = task_by_node.get(item["node"], "")
        task = stage_task(session, workflow.id, stage_key) if stage_key else None
        if item["node"] == "audit" and item["event"] == "defects":
            defect_key = payload_hash(f"frontend\n{item['detail']}")
            defect = session.scalar(select(Defect).where(Defect.workflow_id == workflow.id, Defect.content_hash == defect_key))
            if not defect:
                session.add(Defect(id="defect_" + uuid4().hex, workflow_id=workflow.id,
                                   task_id=task.id if task else "", owner_agent="frontend-agent",
                                   status="open", content=item["detail"], content_hash=defect_key))
        if item["node"] == "frontend" and item["event"] == "repair":
            for defect in session.scalars(select(Defect).where(Defect.workflow_id == workflow.id,
                                                                Defect.owner_agent == "frontend-agent",
                                                                Defect.status.in_(("open", "assigned", "reopened")))):
                defect.status = "fixed"
        if item["node"] == "audit" and item["event"] == "pass":
            for defect in session.scalars(select(Defect).where(Defect.workflow_id == workflow.id, Defect.status == "fixed")):
                defect.status = "verified"
        if task:
            if item["event"] in {"contract_ready", "passed", "pass", "implement", "repair"}:
                task.status = "passed"
                task.iterations = max(task.iterations, 1)
            elif item["event"] == "defects":
                task.status = "defects"
            elif item["event"] == "escalated":
                task.status = "waiting_human"
        session.add(Message(workflow_id=workflow.id, task_id=task.id if task else "", from_agent=item["node"],
                            to_agent="dashboard", text=item["detail"], kind="handoff"))
        append_event(session, workflow, f"graph.{item['event']}", task_id=task.id if task else "",
                     payload={"graph_event": item}, idempotency_key=item["idempotency_key"])
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow.id)).all()
    by_stage = {item.stage_key: item for item in tasks}
    for task in tasks:
        dependencies = task.depends_on.split(",") if task.depends_on else []
        if task.status == "blocked" and all(by_stage[dependency].status == "passed" for dependency in dependencies):
            task.status = "ready"
    # Stage one only covers the contract/frontend repair loop.  A PASS here
    # releases downstream work; it must not falsely mark the full workflow done.
    workflow.status = "waiting_human" if result.get("workflow_status") == "waiting_human" else "running"
    workflow.context_summary = "LangGraph 阶段一已运行；详情请查看节点事件与缺陷账本。"


@app.post("/api/workflows/{workflow_id}/langgraph/stage-one/run")
def run_stage_one(workflow_id: str, payload: dict | None = None, session: Session = Depends(db)):
    """Run the deterministic Stage-one graph and persist its audit projection.

    This is intentionally a short-task graph only.  Codex/Git worktree jobs will
    enter through the interrupt + lease executor in the next migration stage.
    """
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    if workflow.engine != "langgraph_v1":
        raise HTTPException(409, "workflow is pinned to a legacy engine")
    if not checkpointer:
        raise HTTPException(503, "LangGraph checkpointer is not ready")
    payload = payload or {}
    run_id = str(payload.get("run_id", "run_" + uuid4().hex[:8]))
    idempotency_key = str(payload.get("idempotency_key", f"{workflow_id}:{run_id}:complete"))
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    state = new_state(workflow.id, workflow.request, run_id)
    state["contract_audit_outcomes"] = list(payload.get("contract_audit_outcomes", []))
    state["audit_outcomes"] = list(payload.get("audit_outcomes", []))
    state["max_task_iterations"] = int(payload.get("max_task_iterations", 3))
    graph = build_stage_one_graph(checkpointer=checkpointer)
    result = graph.invoke(state, {"configurable": {"thread_id": workflow.thread_id, "checkpoint_ns": f"stage1:{run_id}"}})
    simulate_workers = bool(payload.get("simulate_workers", False))
    persist_stage_one_result(session, workflow, result, simulate_workers=simulate_workers)
    response = {"workflow_id": workflow.id, "run_id": run_id, "status": workflow.status,
                "stage_one_status": result.get("workflow_status") if simulate_workers else "contract_ready",
                "contract_revision": result.get("contract_revision", 0),
                "frontend_iterations": result.get("frontend_iterations", 0),
                "events": len(result.get("events", []))}
    append_event(session, workflow, "graph.run.completed", payload={"response": response}, idempotency_key=idempotency_key)
    session.commit()
    return response


@app.get("/api/workflows/{workflow_id}/events")
def workflow_events(workflow_id: str, session: Session = Depends(db)):
    if not session.get(Workflow, workflow_id):
        raise HTTPException(404, "workflow not found")
    return [{"sequence": item.sequence, "type": item.event_type, "task_id": item.task_id,
             "payload": decode(item.payload), "created_at": item.created_at.isoformat()}
            for item in session.scalars(select(WorkflowEvent).where(WorkflowEvent.workflow_id == workflow_id).order_by(WorkflowEvent.sequence))]


@app.get("/api/workflows/{workflow_id}/tasks/{task_id}/attempts")
def task_attempts(workflow_id: str, task_id: str, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    return [{"id": item.id, "attempt_number": item.attempt_number, "lease_version": item.lease_version,
             "status": item.status, "worker_session_id": item.worker_session_id,
             "created_at": item.created_at.isoformat(), "finished_at": item.finished_at.isoformat() if item.finished_at else None}
            for item in session.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id).order_by(TaskAttempt.attempt_number))]


@app.get("/api/workflows/{workflow_id}/defects")
def defects(workflow_id: str, session: Session = Depends(db)):
    if not session.get(Workflow, workflow_id):
        raise HTTPException(404, "workflow not found")
    return [{"id": item.id, "task_id": item.task_id, "owner_agent": item.owner_agent, "status": item.status,
             "content": item.content, "created_at": item.created_at.isoformat(), "updated_at": item.updated_at.isoformat()}
            for item in session.scalars(select(Defect).where(Defect.workflow_id == workflow_id).order_by(Defect.created_at))]


@app.post("/api/workflows/{workflow_id}/defects")
def add_defect(workflow_id: str, payload: dict, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    owner = str(payload.get("owner_agent", "")).strip()
    content = str(payload.get("content", "")).strip()
    if not owner or not content:
        raise HTTPException(422, "owner_agent and content are required")
    task_id = str(payload.get("task_id", "")).strip()
    source_task = None
    if task_id and (not (source_task := session.get(Task, task_id)) or source_task.workflow_id != workflow_id):
        raise HTTPException(404, "task not found")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    content_digest = payload_hash(f"{owner}\n{content}")
    existing = session.scalar(select(Defect).where(Defect.workflow_id == workflow_id, Defect.content_hash == content_digest))
    if existing:
        return {"id": existing.id, "status": existing.status, "duplicate": True}
    owner_task = session.scalar(select(Task).where(Task.workflow_id == workflow_id, Task.agent_key == owner))
    if not owner_task:
        raise HTTPException(422, "owner_agent does not have a task in this workflow")
    defect = Defect(id="defect_" + uuid4().hex, workflow_id=workflow_id, task_id=task_id, owner_agent=owner,
                    content=content, content_hash=content_digest)
    session.add(defect)
    session.flush()
    queue_repair_regression(session, workflow, owner_task, content, source_task)
    response = {"id": defect.id, "status": defect.status, "owner_task_id": owner_task.id, "owner_status": owner_task.status}
    append_event(session, workflow, "defect.opened", task_id=owner_task.id,
                 payload={"response": response, "owner_agent": owner, "content": content}, idempotency_key=idempotency_key)
    session.commit()
    return response


@app.post("/api/workflows/{workflow_id}/defects/{defect_id}/transition")
def transition_defect(workflow_id: str, defect_id: str, payload: dict, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    defect = session.get(Defect, defect_id)
    if not workflow or not defect or defect.workflow_id != workflow_id:
        raise HTTPException(404, "defect not found")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    target = str(payload.get("status", "")).strip()
    allowed = {"open": {"assigned", "cancelled"}, "assigned": {"fixed", "open"},
               "fixed": {"verified", "reopened"}, "reopened": {"assigned", "cancelled"}}
    if target not in allowed.get(defect.status, set()):
        raise HTTPException(409, f"invalid defect transition: {defect.status} -> {target}")
    defect.status = target
    response = {"id": defect.id, "status": defect.status}
    owner_task = session.scalar(select(Task).where(Task.workflow_id == workflow_id, Task.agent_key == defect.owner_agent))
    if owner_task and target in {"fixed", "verified"}:
        log = decode(owner_task.execution_log)
        detail = "缺陷修复已交付，等待审计复验。" if target == "fixed" else "缺陷已通过回归验证。"
        log.append({"at": datetime.now(timezone.utc).isoformat(), "event": f"缺陷{target}", "detail": detail})
        owner_task.execution_log = json.dumps(log, ensure_ascii=False)
    append_event(session, workflow, f"defect.{target}", task_id=defect.task_id,
                 payload={"response": response, "owner_agent": defect.owner_agent}, idempotency_key=idempotency_key)
    session.commit()
    return response


@app.post("/api/workflows/{workflow_id}/messages")
def send_message(workflow_id: str, payload: dict, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    required = ("from", "to", "text")
    if not workflow:
        raise HTTPException(404, "workflow not found")
    if any(not str(payload.get(key, "")).strip() for key in required):
        raise HTTPException(422, "from, to and text are required")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    kind = payload.get("kind", "message")
    if kind not in {"handoff", "question", "reply", "challenge", "approval", "message"}:
        raise HTTPException(422, "unsupported message kind")
    duplicate_count = len(session.scalars(select(Message).where(
        Message.workflow_id == workflow_id, Message.from_agent == payload["from"], Message.to_agent == payload["to"],
        Message.text == payload["text"], Message.kind == kind)).all())
    if duplicate_count >= MAX_REPEAT_MESSAGES:
        raise HTTPException(429, "repeated message limit reached")
    task_id = payload.get("task_id", "")
    if task_id:
        task = session.get(Task, task_id)
        if not task or task.workflow_id != workflow_id:
            raise HTTPException(404, "task not found")
    item = Message(workflow_id=workflow_id, task_id=task_id, from_agent=payload["from"], to_agent=payload["to"], text=payload["text"], kind=kind)
    session.add(item)
    session.flush()
    all_messages = session.scalars(select(Message).where(Message.workflow_id == workflow_id).order_by(Message.id)).all()
    refresh_summary(workflow, all_messages)
    response = {"id": item.id, "kind": item.kind}
    append_event(session, workflow, "message.sent", task_id=task_id, payload={"response": response, "kind": kind}, idempotency_key=idempotency_key)
    session.commit()
    return response


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/start")
def start_task(workflow_id: str, task_id: str, payload: dict | None = None, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    payload = payload or {}
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    if task.status not in {"ready", "repairing"}:
        raise HTTPException(409, "task is not ready")
    task.status = "queued"
    task.attempt_id += 1
    task.worker_session_id = ""
    job = session.scalar(select(WorkerJob).where(WorkerJob.task_id == task.id))
    if task.stage_key in WORKER_JOB_TYPES:
        if not job:
            job = WorkerJob(id="job_" + uuid4().hex, workflow_id=workflow_id, task_id=task.id, job_type=task.stage_key,
                            required_agent_key=task.agent_key, attempt_id=task.attempt_id, status="queued")
            session.add(job)
        else:
            job.status, job.attempt_id, job.worker_id, job.callback_id = "queued", task.attempt_id, "", ""
            job.required_agent_key, job.runtime_id = task.agent_key, ""
            job.lease_expires_at = job.heartbeat_at = None
    response = {"id": task.id, "status": task.status, "attempt_id": task.attempt_id, "job_id": job.id if job else "",
                "required_agent_key": task.agent_key}
    session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow_id, task_id=task.id,
                            attempt_number=task.attempt_id, lease_version=task.lease_version, status="queued",
                            worker_session_id="", runtime_id="", external_session_id="",
                            idempotency_key=idempotency_key or f"start_{task.id}_{task.attempt_id}"))
    append_event(session, session.get(Workflow, workflow_id), "task.started", task_id=task.id,
                 payload={"response": response}, idempotency_key=idempotency_key)
    session.commit()
    return response


def active_lease(job: WorkerJob, worker_id: str, attempt_id: int, lease_version: int):
    return (job.worker_id == worker_id and job.attempt_id == attempt_id and job.lease_version == lease_version
            and job.status in {"leased", "running"} and job.lease_expires_at and job.lease_expires_at > datetime.now(timezone.utc))


def apply_replan(session: Session, workflow: Workflow, selected_stages: list[str], summary: str):
    """Reopen only the stages selected by Team Lead and their dependent gates."""
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow.id)).all()
    by_stage = {task.stage_key: task for task in tasks}
    valid_stages = set(by_stage) - {"team_lead", "acceptance"}
    invalid = set(selected_stages) - valid_stages
    if invalid or not selected_stages:
        raise HTTPException(422, "replan requires one or more valid affected stages")
    reopened = set(selected_stages)
    changed = True
    while changed:
        changed = False
        for task in tasks:
            dependencies = set(filter(None, task.depends_on.split(",")))
            if task.stage_key not in reopened and dependencies & reopened:
                reopened.add(task.stage_key)
                changed = True
    reopened.discard("team_lead")
    for stage in reopened:
        task = by_stage[stage]
        task.status = "blocked"
        task.instructions = f"验收驳回后的重规划：{summary}"
        log = decode(task.execution_log)
        log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "重规划已选中", "detail": summary})
        task.execution_log = json.dumps(log, ensure_ascii=False)
    for stage in reopened:
        task = by_stage[stage]
        dependencies = task.depends_on.split(",") if task.depends_on else []
        if all(by_stage[dependency].status == "passed" for dependency in dependencies):
            task.status = "acceptance_pending_human" if stage == "acceptance" else "ready"
    response = {"selected_stages": sorted(set(selected_stages)), "reopened_stages": sorted(reopened)}
    append_event(session, workflow, "workflow.replanned", payload={"response": response, "summary": summary},
                 idempotency_key=f"{workflow.id}:replan:{workflow.next_event_sequence + 1}")
    for stage in sorted(reopened):
        task = by_stage[stage]
        session.add(Message(workflow_id=workflow.id, task_id=task.id, from_agent="team-lead", to_agent=task.agent_key,
                            text=f"验收驳回后重规划：{summary}；{stage} 已重新排队。", kind="handoff"))
    return response


@app.post("/api/worker/jobs/claim")
def claim_job(payload: dict, session: Session = Depends(db)):
    worker_id = str(payload.get("worker_id", "")).strip()
    runtime_id = str(payload.get("runtime_id", "")).strip()
    if not worker_id or not runtime_id:
        raise HTTPException(422, "worker_id and runtime_id are required")
    runtime = session.execute(select(AgentRuntime).where(AgentRuntime.id == runtime_id).with_for_update()).scalar_one_or_none()
    if not runtime or runtime.worker_id != worker_id or not runtime_is_live(runtime):
        raise HTTPException(409, "runtime is unknown, owned by another worker, or offline")
    if runtime.state == "working" or runtime.current_job_id:
        raise HTTPException(409, "runtime already owns a job")
    job_id = str(payload.get("job_id", "")).strip()
    workflow_id = str(payload.get("workflow_id", "")).strip()
    statement = select(WorkerJob).where(WorkerJob.status == "queued", WorkerJob.required_agent_key == runtime.agent_key)
    if job_id:
        statement = statement.where(WorkerJob.id == job_id)
    if workflow_id:
        statement = statement.where(WorkerJob.workflow_id == workflow_id)
    job = session.scalar(statement.order_by(WorkerJob.created_at).with_for_update(skip_locked=True))
    if not job:
        return {"job": None}
    if job.job_type not in WORKER_JOB_TYPES:
        raise HTTPException(409, "job type is not executable")
    job.worker_id, job.runtime_id, job.lease_version, job.status = worker_id, runtime.id, job.lease_version + 1, "leased"
    job.heartbeat_at = datetime.now(timezone.utc)
    job.lease_expires_at = job.heartbeat_at + timedelta(seconds=WORKER_LEASE_SECONDS)
    task = session.get(Task, job.task_id)
    task.status, task.lease_version = "running", job.lease_version
    task.worker_session_id = runtime.session_ref or worker_id
    runtime.state, runtime.current_job_id, runtime.last_heartbeat_at = "working", job.id, job.heartbeat_at
    attempt = session.scalar(select(TaskAttempt).where(TaskAttempt.task_id == task.id,
                                                        TaskAttempt.attempt_number == job.attempt_id))
    if attempt:
        attempt.status, attempt.lease_version = "running", job.lease_version
        attempt.worker_session_id, attempt.runtime_id, attempt.external_session_id = worker_id, runtime.id, runtime.session_ref
    session.commit()
    return {"job": {"id": job.id, "workflow_id": job.workflow_id, "task_id": job.task_id, "job_type": job.job_type,
                    "required_agent_key": job.required_agent_key, "attempt_id": job.attempt_id,
                    "lease_version": job.lease_version, "instructions": task.instructions}}


@app.post("/api/worker/jobs/{job_id}/heartbeat")
def heartbeat(job_id: str, payload: dict, session: Session = Depends(db)):
    job = session.get(WorkerJob, job_id)
    runtime_id = str(payload.get("runtime_id", "")).strip()
    runtime = session.get(AgentRuntime, runtime_id) if runtime_id else None
    if (not job or not runtime or job.runtime_id != runtime_id or runtime.worker_id != str(payload.get("worker_id", ""))
            or not active_lease(job, runtime.worker_id, int(payload.get("attempt_id", -1)), int(payload.get("lease_version", -1)))):
        raise HTTPException(409, "stale or expired lease")
    job.status, job.heartbeat_at = "running", datetime.now(timezone.utc)
    job.lease_expires_at = job.heartbeat_at + timedelta(seconds=WORKER_LEASE_SECONDS)
    runtime.last_heartbeat_at = job.heartbeat_at
    session.commit()
    return {"job_id": job.id, "lease_expires_at": job.lease_expires_at.isoformat()}


@app.post("/api/worker/jobs/reap-expired")
def reap_expired_jobs(session: Session = Depends(db)):
    return {"requeued": reap_expired(session)}


@app.post("/api/worker/jobs/{job_id}/callback")
def worker_callback(job_id: str, payload: dict, session: Session = Depends(db)):
    job = session.get(WorkerJob, job_id)
    worker_id = str(payload.get("worker_id", ""))
    runtime_id = str(payload.get("runtime_id", "")).strip()
    runtime = session.get(AgentRuntime, runtime_id) if runtime_id else None
    attempt_id, lease_version = int(payload.get("attempt_id", -1)), int(payload.get("lease_version", -1))
    callback_id = str(payload.get("callback_id", "")).strip()
    if not job or not callback_id:
        raise HTTPException(409, "stale, expired, or invalid callback")
    if job.callback_id == callback_id:
        recorded = decode(job.result_payload)
        if (recorded.get("worker_id") == worker_id and recorded.get("attempt_id") == attempt_id
                and recorded.get("lease_version") == lease_version):
            task = session.get(Task, job.task_id)
            return {"job_id": job.id, "status": job.status,
                    "task": {"id": task.id, "status": task.status, "attempt_id": task.attempt_id} if task else None}
        raise HTTPException(409, "callback payload does not match recorded result")
    if (not runtime or runtime.worker_id != worker_id or job.runtime_id != runtime_id
            or not active_lease(job, worker_id, attempt_id, lease_version)):
        raise HTTPException(409, "stale, expired, or invalid callback")
    if job.callback_id:
        raise HTTPException(409, "job already has a callback")
    outcome = str(payload.get("outcome", "")).lower()
    evidence = str(payload.get("evidence", "")).strip()
    if outcome not in {"succeeded", "failed"} or (outcome == "succeeded" and not evidence):
        raise HTTPException(422, "callback requires succeeded|failed and success evidence")
    job.callback_id, job.result_payload = callback_id, json.dumps(payload, ensure_ascii=False)
    task = session.get(Task, job.task_id)
    task.worker_session_id = runtime.session_ref or worker_id
    replan = payload.get("replan") if job.job_type == "team_lead" and task.instructions.startswith("验收驳回：") else None
    if replan is not None and (not isinstance(replan, dict) or not isinstance(replan.get("affected_stages"), list)
                               or not str(replan.get("summary", "")).strip()):
        raise HTTPException(422, "replan callback requires affected_stages and summary")
    if outcome == "failed":
        job.status = "failed"
        task.status = "repairing" if task.status == "repairing" else "ready"
        runtime.state, runtime.current_job_id = "idle", ""
        attempt = session.scalar(select(TaskAttempt).where(TaskAttempt.task_id == task.id, TaskAttempt.attempt_number == task.attempt_id))
        if attempt:
            attempt.status, attempt.finished_at = "failed", datetime.now(timezone.utc)
        append_event(session, session.get(Workflow, job.workflow_id), "job.failed", task_id=task.id,
                     payload={"response": {"job_id": job.id, "status": job.status}, "error": str(payload.get("error", "worker failed"))},
                     idempotency_key=f"job:{job.id}:callback:{callback_id}")
        session.commit()
        return {"job_id": job.id, "status": "failed"}
    job.status = "callback_pending"
    result = complete_task(job.workflow_id, job.task_id, {"worker_session_id": worker_id, "evidence": evidence,
                                                           "idempotency_key": f"job:{job.id}:callback:{callback_id}"}, session,
                           commit=False)
    if replan is not None:
        result["replan"] = apply_replan(session, session.get(Workflow, job.workflow_id),
                                         [str(stage) for stage in replan["affected_stages"]], str(replan["summary"]).strip())
    job.status, job.lease_expires_at = "succeeded", None
    runtime.state, runtime.current_job_id = "idle", ""
    session.commit()
    return {"job_id": job.id, "status": "succeeded", "task": result}


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/complete")
def complete(workflow_id: str, task_id: str, payload: dict | None = None, session: Session = Depends(db)):
    return complete_task(workflow_id, task_id, payload, session)


def complete_task(workflow_id: str, task_id: str, payload: dict | None, session: Session, *, commit: bool = True):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    payload = payload or {}
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    if task.status not in {"ready", "running", "repairing"}:
        raise HTTPException(409, "task is not ready")
    evidence = str(payload.get("evidence", "")).strip()
    if not evidence:
        raise HTTPException(422, "evidence is required before a task can be delivered")
    if task.iterations >= MAX_TASK_ITERATIONS:
        raise HTTPException(409, "task iteration limit reached")
    was_repairing = task.status == "repairing"
    task.status = "passed"
    task.iterations += 1
    task.worker_session_id = str(payload.get("worker_session_id", ""))
    log = decode(task.execution_log)
    log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "交付证据", "detail": evidence,
                "attempt_id": task.attempt_id})
    if was_repairing:
        task.instructions = "缺陷修复已交付，等待审计复验。"
        log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "回归已排队", "detail": "修复已交付；下游审计与测试将按依赖重新执行。"})
        for defect in session.scalars(select(Defect).where(Defect.workflow_id == workflow_id,
                                                           Defect.owner_agent == task.agent_key,
                                                           Defect.status.in_(("open", "assigned", "reopened")))):
            defect.status = "fixed"
        append_event(session, session.get(Workflow, workflow_id), "defect.fixed", task_id=task.id,
                     payload={"owner_agent": task.agent_key, "attempt_id": task.attempt_id},
                     idempotency_key=f"defect.fixed:{task.id}:{task.attempt_id}")
    task.execution_log = json.dumps(log, ensure_ascii=False)
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    by_stage = {item.stage_key: item for item in tasks}
    for item in tasks:
        dependencies = item.depends_on.split(",") if item.depends_on else []
        eligible = all(by_stage[dependency].status == "passed" for dependency in dependencies)
        if item.status == "blocked" and eligible and (item.stage_key != "audit" or audit_inputs_ready(session, workflow_id, item, by_stage)):
            item.status = "acceptance_pending_human" if item.stage_key == "acceptance" else "ready"
            text = "测试已通过，等待人工验收决定。" if item.stage_key == "acceptance" else f"{task.stage_key} 已交付，{item.stage_key} 节点可以开始。"
            session.add(Message(workflow_id=workflow_id, task_id=task.id, from_agent=task.agent_key, to_agent=item.agent_key, text=text))
            if item.status == "ready":
                queue_worker_attempt(session, session.get(Workflow, workflow_id), item,
                                     event_type="task.queued", detail=f"{task.stage_key} 已交付，依赖已满足。")
    if all(item.status == "passed" for item in tasks):
        session.get(Workflow, workflow_id).status = "completed"
    response = {"id": task.id, "status": task.status, "attempt_id": task.attempt_id}
    attempt = session.scalar(select(TaskAttempt).where(TaskAttempt.task_id == task.id, TaskAttempt.attempt_number == task.attempt_id))
    if attempt:
        attempt.status, attempt.worker_session_id, attempt.finished_at = "passed", task.worker_session_id, datetime.now(timezone.utc)
    else:
        session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow_id, task_id=task.id,
                                attempt_number=task.attempt_id, lease_version=task.lease_version, status="passed",
                                worker_session_id=task.worker_session_id,
                                idempotency_key=idempotency_key or f"complete_{task.id}_{task.attempt_id}", finished_at=datetime.now(timezone.utc)))
    append_event(session, session.get(Workflow, workflow_id), "task.passed", task_id=task.id,
                 payload={"response": response, "attempt_id": task.attempt_id}, idempotency_key=idempotency_key)
    if commit:
        session.commit()
    return response


@app.post("/api/workflows/{workflow_id}/acceptance/decision")
def acceptance_decision(workflow_id: str, payload: dict, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    acceptance = stage_task(session, workflow_id, "acceptance")
    if not workflow or not acceptance:
        raise HTTPException(404, "acceptance task not found")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    decision = str(payload.get("decision", "")).upper()
    actor, reason = str(payload.get("actor", "")).strip(), str(payload.get("reason", "")).strip()
    if acceptance.status != "acceptance_pending_human" or decision not in {"PASS", "REJECT"} or not actor or not reason:
        raise HTTPException(422, "pending acceptance requires PASS|REJECT, actor, and reason")
    record = {"at": datetime.now(timezone.utc).isoformat(), "event": "人工验收", "decision": decision,
              "actor": actor, "detail": reason}
    log = decode(acceptance.execution_log)
    log.append(record)
    acceptance.execution_log = json.dumps(log, ensure_ascii=False)
    if decision == "PASS":
        acceptance.status = "passed"
        workflow.status = "completed"
        message = "人工验收通过，工作流已完成。"
    else:
        acceptance.status = "blocked"
        lead = stage_task(session, workflow_id, "team_lead")
        lead.status, lead.instructions = "queued", f"验收驳回：{reason}\n必须读取驳回理由，产出重规划摘要，并仅选择受影响节点重新执行。"
        lead.attempt_id += 1
        lead.worker_session_id = ""
        replan_job = session.scalar(select(WorkerJob).where(WorkerJob.task_id == lead.id))
        if not replan_job:
            replan_job = WorkerJob(id="job_" + uuid4().hex, workflow_id=workflow_id, task_id=lead.id,
                                   job_type="team_lead", required_agent_key=lead.agent_key,
                                   attempt_id=lead.attempt_id, status="queued")
            session.add(replan_job)
        else:
            replan_job.status, replan_job.attempt_id, replan_job.worker_id, replan_job.callback_id = "queued", lead.attempt_id, "", ""
            replan_job.required_agent_key, replan_job.runtime_id = lead.agent_key, ""
            replan_job.lease_expires_at = replan_job.heartbeat_at = None
            replan_job.result_payload = "{}"
        session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow_id, task_id=lead.id,
                                attempt_number=lead.attempt_id, lease_version=lead.lease_version, status="queued",
                                worker_session_id="", runtime_id="", external_session_id="",
                                idempotency_key=f"replan_{lead.id}_{lead.attempt_id}"))
        workflow.status = "running"
        message = f"人工验收驳回：{reason}；已创建 Team Lead 重规划 Job。"
    response = {"workflow_id": workflow_id, "decision": decision, "status": workflow.status, "actor": actor,
                "replan_job_id": replan_job.id if decision == "REJECT" else ""}
    session.add(Message(workflow_id=workflow_id, task_id=acceptance.id, from_agent=actor, to_agent="team-lead" if decision == "REJECT" else "dashboard", text=message, kind="approval"))
    append_event(session, workflow, "acceptance.decided", task_id=acceptance.id,
                 payload={"response": response, "reason": reason}, idempotency_key=idempotency_key)
    session.commit()
    return response


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/evidence")
def record_evidence(workflow_id: str, task_id: str, payload: dict, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    evidence = str(payload.get("evidence", "")).strip()
    if not evidence:
        raise HTTPException(422, "evidence is required")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    log = decode(task.execution_log)
    log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "交付证据", "detail": evidence})
    task.execution_log = json.dumps(log, ensure_ascii=False)
    response = {"id": task.id, "status": task.status}
    append_event(session, session.get(Workflow, workflow_id), "task.evidence.recorded", task_id=task.id,
                 payload={"response": response, "evidence": evidence}, idempotency_key=idempotency_key)
    session.commit()
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8010)
