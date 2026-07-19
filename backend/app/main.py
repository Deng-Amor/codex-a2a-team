import os
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import DateTime, String, create_engine, select, text
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


class Task(Base):
    __tablename__ = "workflow_tasks"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(40))
    stage_key: Mapped[str] = mapped_column(String(80))
    agent_key: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(30))
    depends_on: Mapped[str] = mapped_column(String, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"), onupdate=lambda: datetime.now(timezone.utc))


DEFAULT_AGENTS = [
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

app = FastAPI(title="A2A Control Plane")
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])


def db():
    with Local() as session:
        yield session


@app.on_event("startup")
def boot():
    Base.metadata.create_all(engine)
    with Local() as session:
        session.execute(text("ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"))
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
    workflow_id = "wf_" + uuid4().hex[:8]
    session.add(Workflow(id=workflow_id, title=payload["title"], request=payload["request"]))
    for stage in session.scalars(select(Stage)):
        session.add(Task(id=f"{workflow_id}_{stage.key}", workflow_id=workflow_id, stage_key=stage.key, agent_key=stage.agent_key, status="ready" if not stage.depends_on else "blocked", depends_on=stage.depends_on))
    session.commit()
    return {"id": workflow_id, "status": "running"}


@app.get("/api/workflows")
def workflows(session: Session = Depends(db)):
    return [{"id": item.id, "title": item.title, "status": item.status} for item in session.scalars(select(Workflow))]


@app.get("/api/workflows/{workflow_id}")
def get_workflow(workflow_id: str, session: Session = Depends(db)):
    workflow = session.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(404, "workflow not found")
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    return {"id": workflow.id, "title": workflow.title, "status": workflow.status, "tasks": [{"id": item.id, "stage": item.stage_key, "agent": item.agent_key, "status": item.status, "depends_on": item.depends_on.split(",") if item.depends_on else [], "updated_at": item.updated_at.isoformat()} for item in tasks]}


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/complete")
def complete(workflow_id: str, task_id: str, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    if task.status != "ready":
        raise HTTPException(409, "task is not ready")
    task.status = "passed"
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    by_stage = {item.stage_key: item for item in tasks}
    for item in tasks:
        dependencies = item.depends_on.split(",") if item.depends_on else []
        if item.status == "blocked" and all(by_stage[dependency].status == "passed" for dependency in dependencies):
            item.status = "ready"
    if all(item.status == "passed" for item in tasks):
        session.get(Workflow, workflow_id).status = "completed"
    session.commit()
    return {"id": task.id, "status": task.status}
