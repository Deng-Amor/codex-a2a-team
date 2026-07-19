import os
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import DateTime, String, create_engine, select, text as sql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

for line in Path(".env").read_text().splitlines() if Path(".env").exists() else []:
    key, _, value = line.partition("=")
    os.environ.setdefault(key, value)
engine = create_engine(os.environ["DATABASE_URL"])
Local = sessionmaker(bind=engine)


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


class Task(Base):
    __tablename__ = "workflow_tasks"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    stage_key: Mapped[str] = mapped_column(String(80))
    agent_key: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(30))
    depends_on: Mapped[str] = mapped_column(String, default="")
    iterations: Mapped[int] = mapped_column(default=0)
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
MAX_TASK_ITERATIONS = 3
MAX_REPEAT_MESSAGES = 3
RECENT_MESSAGES = 12


def route_for(request: str):
    normalized = request.replace(" ", "")
    frontend_only = "前端" in normalized and any(flag in normalized for flag in ("后端数据不修改", "后端不修改", "接口不变", "数据库不修改"))
    backend_only = "后端" in normalized and "前端" not in normalized
    if frontend_only:
        return "frontend_only", TEAM_FRONTEND_ONLY
    if backend_only:
        return "backend_only", TEAM_BACKEND_ONLY
    return "full", TEAM_FULL


def task_data(item: Task):
    return {"id": item.id, "stage": item.stage_key, "agent": item.agent_key, "status": item.status,
            "depends_on": item.depends_on.split(",") if item.depends_on else [], "iterations": item.iterations,
            "updated_at": item.updated_at.isoformat()}


def refresh_summary(workflow: Workflow, messages: list[Message]):
    older = messages[:-RECENT_MESSAGES]
    if older:
        workflow.context_summary = f"已压缩 {len(older)} 条早期上下文；最近摘要：{older[-1].from_agent}→{older[-1].to_agent}：{older[-1].text[:120]}"

app = FastAPI(title="A2A Control Plane")
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])


def db():
    with Local() as session:
        yield session


@app.on_event("startup")
def boot():
    Base.metadata.create_all(engine)
    with Local() as session:
        session.execute(sql("ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"))
        session.execute(sql("ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS iterations INTEGER NOT NULL DEFAULT 0"))
        session.execute(sql("ALTER TABLE workflows ADD COLUMN IF NOT EXISTS context_summary TEXT NOT NULL DEFAULT ''"))
        session.execute(sql("ALTER TABLE a2a_messages ADD COLUMN IF NOT EXISTS kind VARCHAR(20) NOT NULL DEFAULT 'handoff'"))
        existing_agents = {item.key for item in session.scalars(select(Agent))}
        existing_stages = {item.key for item in session.scalars(select(Stage))}
        session.add_all(Agent(key=key, name=name, role=role) for key, name, role in DEFAULT_AGENTS if key not in existing_agents)
        session.add_all(Stage(key=key, agent_key=agent, depends_on=deps) for key, agent, deps in DEFAULT_STAGES if key not in existing_stages)
        session.commit()


@app.get("/api/agents")
def agents(session: Session = Depends(db)):
    return [{"key": item.key, "name": item.name, "role": item.role, "capabilities": item.capabilities} for item in session.scalars(select(Agent))]


@app.post("/api/agents")
def add_agent(agent: dict, session: Session = Depends(db)):
    item = Agent(**agent)
    session.add(item)
    session.commit()
    return {"key": item.key, "name": item.name, "role": item.role}


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
    workflow_id = "wf_" + uuid4().hex[:8]
    workflow = Workflow(id=workflow_id, title=payload["title"], request=payload["request"], context_summary="等待 Team Lead 产出方案与 REST API Contract。")
    session.add(workflow)
    route, stages = route_for(payload["request"])
    for key, agent, dependencies in LEAD_GATE + stages:
        session.add(Task(id=f"{workflow_id}_{key}", workflow_id=workflow_id, stage_key=key, agent_key=agent, status="ready" if not dependencies else "blocked", depends_on=dependencies))
    summary = "仅激活前端、审计、测试与验收链路。" if route == "frontend_only" else "需求已确认，等待 Team Lead 编排。"
    session.add(Message(workflow_id=workflow_id, task_id=f"{workflow_id}_team_lead", from_agent="codex", to_agent="team-lead", text=summary, kind="handoff"))
    session.commit()
    return {"id": workflow_id, "status": "running", "route": route, "gate": "contract_audit"}


@app.get("/api/workflows")
def workflows(session: Session = Depends(db)):
    return [{"id": item.id, "title": item.title, "status": item.status} for item in session.scalars(select(Workflow))]


@app.get("/api/workflows/{workflow_id}")
def get_workflow(workflow_id: str, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    messages = session.scalars(select(Message).where(Message.workflow_id == workflow_id).order_by(Message.id.desc())).all()
    return {"id": workflow.id, "title": workflow.title, "status": workflow.status, "context_summary": workflow.context_summary,
            "tasks": [task_data(item) for item in tasks],
            "messages": [{"id": item.id, "task_id": item.task_id, "from": item.from_agent, "to": item.to_agent, "text": item.text, "kind": item.kind, "created_at": item.created_at.isoformat()} for item in messages]}


@app.get("/api/workflows/{workflow_id}/context")
def context(workflow_id: str, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    messages = session.scalars(select(Message).where(Message.workflow_id == workflow_id).order_by(Message.id.desc()).limit(RECENT_MESSAGES)).all()
    return {"summary": workflow.context_summary, "recent_messages": [{"from": item.from_agent, "to": item.to_agent, "text": item.text, "kind": item.kind} for item in reversed(messages)]}


@app.post("/api/workflows/{workflow_id}/messages")
def send_message(workflow_id: str, payload: dict, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    required = ("from", "to", "text")
    if not workflow:
        raise HTTPException(404, "workflow not found")
    if any(not str(payload.get(key, "")).strip() for key in required):
        raise HTTPException(422, "from, to and text are required")
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
    session.commit()
    return {"id": item.id, "kind": item.kind}


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/complete")
def complete(workflow_id: str, task_id: str, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    if task.status != "ready":
        raise HTTPException(409, "task is not ready")
    if task.iterations >= MAX_TASK_ITERATIONS:
        raise HTTPException(409, "task iteration limit reached")
    task.status = "passed"
    task.iterations += 1
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    by_stage = {item.stage_key: item for item in tasks}
    for item in tasks:
        dependencies = item.depends_on.split(",") if item.depends_on else []
        if item.status == "blocked" and all(by_stage[dependency].status == "passed" for dependency in dependencies):
            item.status = "ready"
            session.add(Message(workflow_id=workflow_id, task_id=task.id, from_agent=task.agent_key, to_agent=item.agent_key, text=f"{task.stage_key} 已交付，{item.stage_key} 节点可以开始。"))
    if all(item.status == "passed" for item in tasks):
        session.get(Workflow, workflow_id).status = "completed"
    session.commit()
    return {"id": task.id, "status": task.status}
