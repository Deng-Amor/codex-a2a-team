import { execFile } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { promisify } from "node:util";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const exec = promisify(execFile);
const root = resolve(fileURLToPath(new URL(".", import.meta.url)));
const agent = process.argv[2];
const brokerUrl = process.env.A2A_BROKER_URL || "http://127.0.0.1:4318";
const repository = process.env.A2A_REPOSITORY;
const pollMs = Number(process.env.A2A_POLL_MS || 2500);
const codexNode = process.env.CODEX_NODE_PATH || process.execPath;
const codexCli = process.env.CODEX_CLI_PATH || join(dirname(codexNode), "node_modules", "@openai", "codex", "bin", "codex.js");
const roles = JSON.parse(await readFile(join(root, "agents.json"), "utf8"));
if (!roles[agent]) throw new Error(`Unknown agent. Use one of: ${Object.keys(roles).join(", ")}`);
if (!repository) throw new Error("Set A2A_REPOSITORY to the target Git repository before starting a worker.");

const api = async (path, options = {}) => {
  const response = await fetch(`${brokerUrl}${path}`, { ...options, headers: { "content-type": "application/json", ...(options.headers || {}) } });
  const value = await response.json(); if (!response.ok) throw new Error(value.error || response.statusText); return value;
};
const git = (args, cwd = repository) => exec("git", args, { cwd, windowsHide: true });
const sleep = ms => new Promise(ok => setTimeout(ok, ms));

async function worktree(workflow, task) {
  if (!task.branch) return repository;
  const path = join(repository, ".a2a-worktrees", workflow.id, task.id);
  try { await git(["rev-parse", "--is-inside-work-tree"], path); return path; } catch {}
  await mkdir(resolve(path, ".."), { recursive: true });
  await git(["worktree", "add", "-b", task.branch, path, "HEAD"]);
  return path;
}
function prompt(workflow, task, directory) {
  return `You are ${agent}. ${roles[agent]}

This is an independent A2A task. Workflow ID: ${workflow.id}; task ID: ${task.id}; Codex session must stay scoped to this task.
Confirmed user request:\n${workflow.request}
Auto-merge policy: ${workflow.autoMerge ? "enabled only for low-risk changes after every required gate passes" : "disabled; create a PR and return it for human review"}
Repository: ${directory}
Your upstream task results:\n${JSON.stringify(Object.fromEntries(Object.entries(workflow.tasks).filter(([, t]) => t.status === "passed").map(([k, t]) => [k, t.result])), null, 2)}

Do only your assigned stage: ${task.stage}. Do not impersonate another agent. Do not modify files outside your owned task branch. Use existing project commands for validation.
Return only the JSON schema result. For defects, choose owner frontend-agent, backend-agent, or test-agent and include exact reproduction.`;
}
async function runCodex(workflow, task, directory) {
  const resultPath = join(root, "data", `${task.id}.result.json`); await mkdir(join(root, "data"), { recursive: true });
  const sandbox = task.branch || agent === "deployment-agent" ? "workspace-write" : "read-only";
  const { stdout } = await exec(codexNode, [codexCli, "exec", "-C", directory, "-s", sandbox, "--json", "--output-schema", join(root, "result.schema.json"), "--output-last-message", resultPath, prompt(workflow, task, directory)], { windowsHide: true, maxBuffer: 2_000_000 });
  let sessionId = "";
  for (const line of stdout.split(/\r?\n/)) try {
    const event = JSON.parse(line); const text = JSON.stringify(event);
    sessionId ||= text.match(/"(?:thread_id|session_id|threadId|sessionId)"\s*:\s*"([^"]+)"/)?.[1] || "";
  } catch {}
  return { ...JSON.parse(await readFile(resultPath, "utf8")), sessionId };
}
async function commitAndPush(task, directory) {
  if (!task.branch) return {};
  const { stdout } = await git(["status", "--porcelain"], directory); if (!stdout.trim()) return {};
  await git(["add", "-A"], directory); await git(["commit", "-m", `a2a: ${task.stage} for ${task.id}`], directory);
  try { await git(["remote", "get-url", "origin"], directory); } catch { return { pullRequest: "", notice: "Branch committed locally. Add an origin and install/authenticate GitHub CLI to push and create the PR automatically." }; }
  await git(["push", "-u", "origin", task.branch], directory);
  try {
    const { stdout: url } = await exec("gh", ["pr", "create", "--fill", "--head", task.branch], { cwd: directory, windowsHide: true });
    return { pullRequest: url.trim() };
  } catch { return { pullRequest: "", notice: "Branch committed locally. Add an origin and install/authenticate GitHub CLI to push and create the PR automatically." }; }
}
async function one() {
  await api(`/api/agents/${agent}/heartbeat`, { method: "POST", body: JSON.stringify({ pid: process.pid, role: roles[agent] }) });
  const assignment = await api(`/api/agents/${agent}/next`); if (!assignment) return false;
  const { workflow, task } = assignment; const directory = await worktree(workflow, task);
  let result;
  try { result = { ...(await runCodex(workflow, task, directory)), ...(await commitAndPush(task, directory)) }; }
  catch (error) { result = { status: "BLOCKED", summary: error.message, defects: [] }; }
  const route = task.stage === "repair" ? "repair-complete" : "complete";
  await api(`/api/workflows/${workflow.id}/tasks/${task.id}/${route}`, { method: "POST", body: JSON.stringify(result) });
  return true;
}
while (true) { try { if (!await one()) await sleep(pollMs); } catch (error) { console.error(`[${agent}]`, error.message); await sleep(pollMs); } }
