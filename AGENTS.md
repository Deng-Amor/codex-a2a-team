# A2A 项目强制执行规则

每个进入本仓库的 Codex Agent、Worker 在开始任何分析、修改、命令或外部调用前，必须先阅读完整规则：[docs/PROJECT_GUARDRAILS.md](docs/PROJECT_GUARDRAILS.md)。规则与临时任务冲突时，以该文件为准；例外必须获得用户明确确认。

执行红线：

1. Web 使用 Vue 3 + Vite；小程序、H5、App 使用 uni-app；后端使用 FastAPI + PostgreSQL；API 使用 RESTful。
2. 需要新增或调用 MCP 时，先说明用途、权限和影响，等待用户确认。
3. Team Lead 的 API Contract 必须先通过方案审计，才能释放开发任务；节点只能在有实际交付证据后标记 `passed`。
4. 所有变更使用分支和 PR。简单 Bug 修复在检查、审计、测试通过后可自动合并；其他合并、数据库迁移、部署和高风险操作必须人工确认。
5. 数据库结构变更必须同步 SQLAlchemy 模型和版本化迁移；禁止只手工改库。
6. 密钥不得进入 Git 或日志；Dashboard 只允许展示脱敏值。
7. 数据库按完整规则执行每 2 小时备份、3 天小时备份保留、30 天日归档保留及恢复演练。
8. 任何发布必须有代码、制品、数据库迁移和数据补偿四层回滚方案。
