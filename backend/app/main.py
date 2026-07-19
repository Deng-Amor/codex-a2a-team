import os
import json
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
    instructions: Mapped[str] = mapped_column(String, default="")
    plan: Mapped[str] = mapped_column(String, default="")
    artifacts: Mapped[str] = mapped_column(String, default="")
    execution_log: Mapped[str] = mapped_column(String, default="")
    handoff_summary: Mapped[str] = mapped_column(String, default="")
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


def decode(value: str):
    return json.loads(value) if value else []


def task_data(item: Task, details=False):
    data = {"id": item.id, "stage": item.stage_key, "agent": item.agent_key, "status": item.status,
            "depends_on": item.depends_on.split(",") if item.depends_on else [], "iterations": item.iterations,
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
        detail = task_detail(key, workflow, route)
        session.add(Task(id=f"{workflow_id}_{key}", workflow_id=workflow_id, stage_key=key, agent_key=agent,
                         status="ready" if not dependencies else "blocked", depends_on=dependencies,
                         instructions=detail["instructions"], plan=json.dumps(detail["plan"], ensure_ascii=False),
                         artifacts=json.dumps(detail["artifacts"], ensure_ascii=False), execution_log=json.dumps(detail["execution_log"], ensure_ascii=False),
                         handoff_summary=detail["handoff_summary"]))
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
    session.commit()
    return {"id": task.id, "status": task.status}
