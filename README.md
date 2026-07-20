# Codex A2A Team

一个可移植到其他 Windows 电脑的本地 A2A 控制平面：Codex 对话负责确认需求；FastAPI、PostgreSQL 和 LangGraph 记录工作流；Vue 3 + Vite Dashboard 展示节点、交接消息、缺陷与回归状态。

> 当前可用范围：工作流账本、Team Lead / Contract Audit 闸门、前后端并行状态、基于持久化审计输入的 Audit fan-in、缺陷回派、人工验收闸门与 Dashboard。阶段三的外部 Codex CLI Worker、Git Worktree、`interrupt()/Command(resume)`、自动 PR/部署尚未接入，不能把当前版本当作无人值守发布系统。

## 架构与端口

| 组件 | 技术 | 地址 | 数据 |
| --- | --- | --- | --- |
| Dashboard | Vue 3 + Vite | `http://127.0.0.1:20002` | 只读 FastAPI API |
| Control Plane | FastAPI + LangGraph | `http://127.0.0.1:8010` | PostgreSQL 业务账本、Checkpoint、Outbox |
| PostgreSQL | 已有 Docker PostgreSQL | 默认 `5432` | 数据库 `agent_to_agent` |

Dashboard 直接读取业务表；LangGraph Checkpoint 仅用于恢复与回放，不能直接作为展示数据。一次 A2A 流程为：Team Lead → Contract Audit → 前端/后端并行 → Audit → Test → Acceptance；DEFECT 会回到责任节点并阻断其下游回归节点。

## 办公电脑首次安装

### 1. 前置条件

- Windows 10/11、PowerShell 7 或 Windows PowerShell。
- Python 3.11+、Node.js 20+、Git。
- Docker Desktop 中已有 PostgreSQL 容器；不要再映射新的 `5433`，使用现有 `5432`。
- 已安装并登录 Codex；GitHub CLI 仅在阶段三接入 PR 时需要。

克隆仓库：

```powershell
git clone https://github.com/Deng-Amor/codex-a2a-team.git
Set-Location codex-a2a-team
```

### 2. 创建数据库与环境变量

先找到现有 PostgreSQL 容器：

```powershell
docker ps
docker exec -it <postgres-container> psql -U postgres -c "CREATE DATABASE agent_to_agent;"
```

若数据库已存在，第二条命令会报已存在，可忽略。不要把账号、密码或连接串提交到 Git。

```powershell
Copy-Item .env.example .env
notepad .env
```

把 `.env` 中的 `YOUR_PASSWORD` 换为本机 PostgreSQL 密码；保留数据库名 `agent_to_agent`。`.env` 已被 Git 忽略。

### 3. 安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r backend\requirements.txt

Set-Location frontend
npm ci
Set-Location ..
```

### 4. 启动服务

服务分别在两个 PowerShell 窗口启动，工作目录都必须是仓库根目录。

窗口 A：

```powershell
.\.venv\Scripts\Activate.ps1
python -m uvicorn app.main:app --app-dir backend --host 127.0.0.1 --port 8010
```

窗口 B：

```powershell
Set-Location frontend
npm run dev -- --host 127.0.0.1
```

打开 `http://127.0.0.1:20002`。首次启动会创建 SQLAlchemy 表、运行已提交的兼容迁移，并由 `PostgresSaver` 创建 LangGraph Checkpoint 表。不要用本地 JSON 作为账本。

### 5. 启动前检查

```powershell
Invoke-WebRequest http://127.0.0.1:8010/api/workflows | Select-Object StatusCode
Invoke-WebRequest http://127.0.0.1:20002 | Select-Object StatusCode
Set-Location frontend; npm run build; Set-Location ..
```

三项均成功后再开始工作流。端口被占用时，先确认占用程序，再停止旧进程；不要同时启动两套 8010 或 20002 服务。

## 从 Codex 发起一个 A2A 任务

1. 在 Codex 对话提出需求，先确认范围、排除项、验收标准和目标仓库。
2. 每次写操作都明确选择 `A2A` 或 `主控直改`。选择只对当前任务有效。
3. 选择 A2A 后，创建 Workflow 并打开 Dashboard；先给 Team Lead 写入输入，再启动 Team Lead 节点。
4. Contract Audit 通过前，前端和后端不得开始；两者通过 Contract 后可并行。
5. Audit/Test 产生 DEFECT 时，责任节点显示 `修复中` 和缺陷内容；修复后重新经过 Audit 与 Test。Audit 只在 `audit.depends_on` 指定的本轮交付均有当前 attempt 证据且无未解决缺陷时运行。
6. Test 通过后进入 `acceptance_pending_human`；只能由人工通过 `POST /api/workflows/{workflow_id}/acceptance/decision` 决定 `PASS` 或 `REJECT`，不得由产品 Agent 自动标记验收通过。
7. `passed` 必须有实际交付证据，不能用计划或模拟结果冒充。

可用脚本创建 Workflow 并打开 Dashboard（仅在 Codex 已确认执行后运行）：

```powershell
.\start-workflow.ps1 -Title 'eSIM 卡管理系统' -Request '实现 eSIM 卡 CRUD、状态筛选和 REST API' -Repository 'C:\Work\esim-platform'
```

脚本会输出 `workflow_id`。随后由 Codex 通过 API 写入 Team Lead 输入、推进节点；Dashboard 只用于观察，不能替代对话中的确认与人工决策。

## 可复制给 Codex 的 Skill

把仓库中的 `skills\a2a-execution` 复制到办公电脑当前用户的 Codex Skills 目录，重启/刷新 Codex 后使用。Skill 强制执行：模式选择 → 创建 Workflow → 打开 Dashboard → Team Lead 输入 → 节点执行。

```powershell
$target = Join-Path $env:USERPROFILE '.codex\skills\a2a-execution'
New-Item -ItemType Directory -Force -Path $target | Out-Null
Copy-Item -Recurse -Force .\skills\a2a-execution\* $target
```

## 数据库、迁移与备份

- 正式数据只在 PostgreSQL `agent_to_agent` 中保存。
- 模型/字段变更必须同时提交 SQLAlchemy 模型和 `backend/migrations/` 中的版本化 SQL；不要手工只改某台电脑的数据库。
- `.env`、备份文件、Token 和数据库密码不得提交。
- RPO 目标为 2 小时：使用 Windows 任务计划每 2 小时运行 `pg_dump -Fc` 到 `data/backups/`；保留最近 3 天小时备份和 30 天日归档。恢复演练至少每月一次。

备份命令示例（密码通过环境变量或 Docker 容器配置提供）：

```powershell
pg_dump -Fc -h 127.0.0.1 -p 5432 -U postgres -d agent_to_agent -f data\backups\agent_to_agent_hourly_YYYYMMDD_HHMMSS.dump
```

## 当前开发边界与后续路线

1. 阶段三：长任务 `interrupt()`、lease/fencing token、Worker 回调签名、Codex CLI + Git Worktree、PR 与部署。
2. Redis 仅在确实需要队列、分布式锁、限流或高并发时再接入。

历史 Node 4318 原型及 Vue 模板演示文件已移除，避免办公电脑误启动错误入口。当前唯一入口是本 README 的 FastAPI 8010 + Vite 20002 命令。

## 常见问题

| 现象 | 处理 |
| --- | --- |
| Dashboard 显示 502 | 确认 `http://127.0.0.1:8010/api/workflows` 返回 200，再刷新 20002。短暂重启错误应在下次成功轮询后清除。 |
| Dashboard 空白或 Failed to fetch | 检查 8010、Vite 代理以及 `.env` 的 `DATABASE_URL`。 |
| `password authentication failed` | 只修改本机 `.env` 的密码；不要修改代码或提交密码。 |
| 端口 8010/20002 被占用 | 查明旧进程后停止它，再启动一套服务。 |
| 节点显示已交付但没有证据 | 这是数据一致性缺陷；不要手工改状态，应创建修复任务并保留审计记录。 |

## 本次交付记录

`execution_mode=direct`：更新跨电脑部署与运行说明、修正 Skill 和启动脚本的当前入口。验证证据：`npm run build` 通过；本地 `8010/api/workflows` 与 `20002/api/workflows` 均返回 200。
