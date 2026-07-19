---
name: a2a-execution
description: "Coordinate software work through the local Codex A2A Team broker. Use when the user asks to execute a confirmed development task with independent agents, a dashboard, Git branches, pull requests, audit, testing, and deployment. First summarize scope and acceptance criteria in the Codex chat and wait for the user to explicitly say confirm execution; only then start the local A2A workflow and dashboard."
---

# Codex A2A Execution

Keep the user interaction in this Codex task. Do not send the user to a dashboard to enter the task.

## Execution-mode gate

Before any non-read-only change, ask the user to select `A2A` or `主控直改`. A request described as small, urgent, or a bug fix still needs this selection. A prior selection applies only to its named workflow; do not reuse it for a later change. If A2A is selected, create the workflow and open the Dashboard before modifying code. If direct mode is selected, state `execution_mode=direct` with the final verification evidence.

1. Convert the request into a concise confirmation containing scope, exclusions, acceptance criteria, repository path, and whether low-risk auto-merge is allowed.
2. Wait for explicit confirmation such as `确认执行`. Do not start any A2A worker before it.
3. On confirmation, require `A2A_TEAM_HOME` to point to the installed A2A Team directory, then run `$env:A2A_TEAM_HOME\start-workflow.ps1`. Pass the confirmed scope as `Request`, the project root as `Repository`, and set `-AutoMerge` only if the user explicitly authorized low-risk auto-merge.
4. Tell the user the workflow has started and that the local dashboard has opened. Continue to use this chat for material decisions, blocked deployment/PR review, and final delivery.
5. Do not claim a stage passed unless the dashboard/result evidence reports it. If the deployment agent returns `BLOCKED`, give the user the PR URL and the reason.

The dashboard is observability and control for the confirmed run; the Codex chat remains the task-entry and approval surface.
