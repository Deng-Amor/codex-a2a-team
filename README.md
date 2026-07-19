# Codex A2A Team

Local A2A broker and dashboard for independent Codex CLI agents. The broker persists workflows, direct messages, task status, and repair loops in `data/broker.json`. Agents use the same broker, not a parent-child subagent tree.

## Start

```powershell
.\run-team.ps1 -Repository 'C:\path\to\your\git-repo'
```

This starts one Codex worker per role from `agents.json` and the dashboard at `http://127.0.0.1:4318`. Stop the broker with `Ctrl+C`, then run `.\stop-team.ps1` to stop the workers.

Extract the package to a stable local path, set `A2A_TEAM_HOME` to that path, then install `skills/a2a-execution` into the target user's Codex skills directory. The user enters work in Codex, Codex confirms scope in chat, and an explicit `确认执行` runs `start-workflow.ps1`, starts the broker, and opens the dashboard. The dashboard is not a task-entry form. The API remains available for automation:

```powershell
Invoke-RestMethod http://127.0.0.1:4318/api/workflows -Method Post -ContentType application/json -Body '{"title":"生图模块","request":"实现天猫主图生成模块，输出 1440×1440 图片","repository":"C:\\path\\to\\your\\git-repo","autoMerge":false}'
```

Run `node test.mjs` before use. Codex CLI must be installed and authenticated. If its global npm path is nonstandard, set `CODEX_NODE_PATH` and `CODEX_CLI_PATH` to the Node executable and `@openai/codex/bin/codex.js`. Automatic PR creation needs authenticated GitHub CLI (`gh`); automatic merging remains disabled unless the workflow explicitly sets `autoMerge: true` and repository policy permits it. A missing `gh`, failed push, or blocked merge is returned as a visible `BLOCKED` deployment task for human review.
