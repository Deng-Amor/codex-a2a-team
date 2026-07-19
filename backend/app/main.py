import os
import json
import hashlib
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import DateTime, String, UniqueConstraint, create_engine, select, text as sql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from langgraph.checkpoint.postgres import PostgresSaver

from app.langgraph_loop import GRAPH_VERSION, build_stage_one_graph, new_state

for line in Path(".env").read_text().splitlines() if Path(".env").exists() else []:
    key, _, value = line.partition("=")
    os.environ.setdefault(key, value)
engine = create_engine(os.environ["DATABASE_URL"])
Local = sessionmaker(bind=engine)
checkpointer_context = None
checkpointer = None


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
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=sql("CURRENT_TIMESTAMP"))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


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
}


def route_for(request: str):
    normalized = request.replace(" ", "")
    frontend_only = "前端" in normalized and any(flag in normalized for flag in ("后端数据不修改", "后端不修改", "接口不变", "数据库不修改"))
    backend_only = "后端" in normalized and "前端" not in normalized
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


def task_detail(stage: str, workflow: Workflow, route: str):
    contract = [
        {"operation": "GET /api/v1/resources", "purpose": "分页查询资源", "response": '{"items": [{"id": "string", "name": "string", "status": "active"}], "total": 0}'},
        {"operation": "GET /api/v1/resources/{id}", "purpose": "读取资源详情", "response": '{"id": "string", "name": "string", "status": "active", "updated_at": "ISO-8601"}'},
        {"operation": "POST /api/v1/resources", "purpose": "创建资源", "request": '{"name": "string"}', "response": "201 + resource"},
        {"operation": "PATCH /api/v1/resources/{id}", "purpose": "更新资源", "request": '{"name": "string", "status": "active|inactive"}', "response": "200 + resource"},
    ]
    details = {
        "team_lead": ("澄清需求并输出可审计的实施边界与 REST API Contract；开发任务在方案审计通过前不得启动。",
                      ["确认范围：" + workflow.request, "定义前后端并行边界与验收条件", "输出 REST API Contract，等待方案审计"],
                      [{"name": "REST API Contract", "type": "api_contract", "content": contract}, {"name": "验收标准", "type": "acceptance", "content": ["接口字段与状态枚举一致", "前端可使用 Mock 完成独立开发", "真实 API 接入后通过集成测试"]}],
                      "将方案、接口契约及风险交给方案审计 Agent。"),
        "contract_audit": ("审查 Team Lead 的需求边界、API 风格、字段完整性和前后端并行可行性。",
                           ["检查 REST 命名、状态码和错误响应", "检查请求/响应字段是否足够前端 Mock", "通过后释放开发任务"],
                           [{"name": "Contract 审核清单", "type": "review", "content": ["路径使用复数资源名", "写操作返回明确状态码", "字段、枚举、错误场景可实现"]}],
                           "通过后向前端和后端发送已审核 Contract。"),
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
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])


def db():
    with Local() as session:
        yield session


@app.on_event("startup")
def boot():
    global checkpointer_context, checkpointer
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
        session.commit()
    checkpoint_url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    checkpointer_context = PostgresSaver.from_conn_string(checkpoint_url)
    checkpointer = checkpointer_context.__enter__()
    checkpointer.setup()


@app.on_event("shutdown")
def close_checkpointer():
    global checkpointer_context, checkpointer
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
    workflow = Workflow(id=workflow_id, thread_id=workflow_id, engine=engine_name,
                        graph_version=payload.get("graph_version", GRAPH_VERSION), state_schema_version=1,
                        title=payload["title"], request=payload["request"], context_summary="等待 Team Lead 产出方案与 REST API Contract。")
    session.add(workflow)
    route, stages = route_for(payload["request"])
    for key, agent, dependencies in LEAD_GATE + stages:
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
             "graph_version": item.graph_version, "thread_id": item.thread_id} for item in session.scalars(select(Workflow))]


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


def persist_stage_one_result(session: Session, workflow: Workflow, result: dict):
    """Project pure LangGraph events into the dashboard's business ledger."""
    task_by_node = {"team_lead": "team_lead", "contract_audit": "contract_audit", "frontend": "frontend", "audit": "audit"}
    for item in result.get("events", []):
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
    persist_stage_one_result(session, workflow, result)
    response = {"workflow_id": workflow.id, "run_id": run_id, "status": workflow.status,
                "stage_one_status": result.get("workflow_status"),
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
    if task_id and (not (task := session.get(Task, task_id)) or task.workflow_id != workflow_id):
        raise HTTPException(404, "task not found")
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    content_digest = payload_hash(f"{owner}\n{content}")
    existing = session.scalar(select(Defect).where(Defect.workflow_id == workflow_id, Defect.content_hash == content_digest))
    if existing:
        return {"id": existing.id, "status": existing.status, "duplicate": True}
    defect = Defect(id="defect_" + uuid4().hex, workflow_id=workflow_id, task_id=task_id, owner_agent=owner,
                    content=content, content_hash=content_digest)
    session.add(defect)
    response = {"id": defect.id, "status": defect.status}
    append_event(session, workflow, "defect.opened", task_id=task_id, payload={"response": response, "owner_agent": owner}, idempotency_key=idempotency_key)
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


@app.post("/api/workflows/{workflow_id}/tasks/{task_id}/complete")
def complete(workflow_id: str, task_id: str, payload: dict | None = None, session: Session = Depends(db)):
    task = session.get(Task, task_id)
    if not task or task.workflow_id != workflow_id:
        raise HTTPException(404, "task not found")
    payload = payload or {}
    idempotency_key = str(payload.get("idempotency_key", "")).strip()
    previous = event_response(session, workflow_id, idempotency_key)
    if previous is not None:
        return previous
    if task.status != "ready":
        raise HTTPException(409, "task is not ready")
    if task.iterations >= MAX_TASK_ITERATIONS:
        raise HTTPException(409, "task iteration limit reached")
    task.status = "passed"
    task.iterations += 1
    task.attempt_id += 1
    task.worker_session_id = str(payload.get("worker_session_id", ""))
    log = decode(task.execution_log)
    log.append({"at": datetime.now(timezone.utc).isoformat(), "event": "节点已交付", "detail": task.handoff_summary})
    task.execution_log = json.dumps(log, ensure_ascii=False)
    tasks = session.scalars(select(Task).where(Task.workflow_id == workflow_id)).all()
    by_stage = {item.stage_key: item for item in tasks}
    for item in tasks:
        dependencies = item.depends_on.split(",") if item.depends_on else []
        if item.status == "blocked" and all(by_stage[dependency].status == "passed" for dependency in dependencies):
            item.status = "ready"
            session.add(Message(workflow_id=workflow_id, task_id=task.id, from_agent=task.agent_key, to_agent=item.agent_key, text=f"{task.stage_key} 已交付，{item.stage_key} 节点可以开始。"))
    if all(item.status == "passed" for item in tasks):
        session.get(Workflow, workflow_id).status = "completed"
    response = {"id": task.id, "status": task.status, "attempt_id": task.attempt_id}
    session.add(TaskAttempt(id="attempt_" + uuid4().hex, workflow_id=workflow_id, task_id=task.id,
                            attempt_number=task.attempt_id, lease_version=task.lease_version, status="passed",
                            worker_session_id=task.worker_session_id,
                            idempotency_key=idempotency_key or f"complete_{task.id}_{task.attempt_id}"))
    append_event(session, session.get(Workflow, workflow_id), "task.passed", task_id=task.id,
                 payload={"response": response, "attempt_id": task.attempt_id}, idempotency_key=idempotency_key)
    session.commit()
    return response
