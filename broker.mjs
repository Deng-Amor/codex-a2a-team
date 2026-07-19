import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

const now = () => new Date().toISOString();
const stages = [
  ["decompose", "task-decomposer"], ["architecture", "architecture-agent"],
  ["product", "product-agent"], ["frontend", "frontend-agent"], ["backend", "backend-agent"],
  ["audit", "audit-agent"], ["test", "test-agent"], ["acceptance", "product-agent"], ["deploy", "deployment-agent"]
];
const depends = {
  decompose: [], architecture: ["decompose"], product: ["architecture"],
  frontend: ["product"], backend: ["product"], audit: ["frontend", "backend"],
  test: ["audit"], acceptance: ["test"], deploy: ["acceptance"]
};

export class Broker {
  // ponytail: JSON persistence is single-host/single-process; replace with Postgres before multi-user or HA use.
  constructor(file) { this.file = file; this.data = { workflows: {}, messages: [], agents: {} }; }
  async load() { try { this.data = JSON.parse(await readFile(this.file, "utf8")); } catch { await this.save(); } let migrated = false; for (const wf of Object.values(this.data.workflows)) for (const task of Object.values(wf.tasks)) if (task.startedAt && !task.executionId) { task.executionId = `legacy_${task.id}`; migrated = true; } if (migrated) await this.save(); return this; }
  async save() {
    await mkdir(dirname(this.file), { recursive: true });
    const temp = `${this.file}.tmp`;
    await writeFile(temp, JSON.stringify(this.data, null, 2));
    await rename(temp, this.file);
  }
  snapshot() { return this.data; }
  async createWorkflow({ title, request, repository = "", autoMerge = false }) {
    if (!title || !request) throw new Error("title and request are required");
    const id = `wf_${randomUUID().slice(0, 8)}`;
    const tasks = Object.fromEntries(stages.map(([stage, agent]) => [stage, {
      id: `${id}_${stage}`, stage, agent, status: "blocked", dependsOn: depends[stage], attempts: 0,
      branch: ["frontend", "backend", "test"].includes(stage) ? `a2a/${id}/${stage}` : "",
      messages: [], result: null
    }]));
    this.data.workflows[id] = { id, title, request, repository, autoMerge, status: "needs_confirmation", createdAt: now(), updatedAt: now(), tasks };
    await this.save(); return this.data.workflows[id];
  }
  async confirm(id) {
    const wf = this.mustWorkflow(id); if (wf.status !== "needs_confirmation") throw new Error("workflow is not ready for confirmation"); wf.status = "running"; this.advance(wf); await this.save(); return wf;
  }
  mustWorkflow(id) { const wf = this.data.workflows[id]; if (!wf) throw new Error("workflow not found"); return wf; }
  task(wf, taskId) { const task = Object.values(wf.tasks).find(t => t.id === taskId); if (!task) throw new Error("task not found"); return task; }
  advance(wf) {
    if (wf.status !== "running") return;
    for (const task of Object.values(wf.tasks)) {
      if (task.status === "blocked" && !task.result && task.dependsOn.every(stage => wf.tasks[stage].status === "passed")) task.status = "ready";
    }
    if (Object.values(wf.tasks).every(t => t.status === "passed")) wf.status = "completed";
    wf.updatedAt = now();
  }
  async next(agent) {
    for (const wf of Object.values(this.data.workflows)) {
      if (wf.status !== "running") continue;
      const task = Object.values(wf.tasks).find(t => t.agent === agent && t.status === "ready");
      if (task) { task.status = "running"; task.attempts++; task.executionId ||= `run_${randomUUID().slice(0, 8)}`; task.startedAt = now(); await this.save(); return { workflow: wf, task }; }
    }
    return null;
  }
  async complete(workflowId, taskId, result) {
    const wf = this.mustWorkflow(workflowId); const task = this.task(wf, taskId);
    if (task.status !== "running") throw new Error("only a running task can complete");
    task.result = result; task.finishedAt = now();
    if (result.status === "PASS") task.status = "passed";
    else if (result.status === "DEFECTS") task.status = "waiting_repair";
    else task.status = "blocked";
    if (result.defects?.length) for (const defect of result.defects) await this.send({ workflowId, from: task.agent, to: defect.owner, type: "DEFECT", taskId, payload: defect });
    this.advance(wf); await this.save(); return task;
  }
  async send({ workflowId, from, to, type, taskId = "", payload = {} }) {
    const wf = this.mustWorkflow(workflowId);
    const message = { id: `msg_${randomUUID().slice(0, 8)}`, workflowId, from, to, type, taskId, payload, createdAt: now() };
    this.data.messages.push(message);
    if (type === "DEFECT") {
      const source = this.task(wf, taskId); source.status = "waiting_repair";
      const id = `repair_${message.id}`;
      wf.tasks[id] = { id: `${wf.id}_${id}`, stage: "repair", agent: to, status: "ready", dependsOn: [], attempts: 0, branch: `a2a/${wf.id}/${id}`, messages: [message.id], result: null, repairs: source.id };
    }
    await this.save(); return message;
  }
  async repairComplete(workflowId, taskId, result) {
    const wf = this.mustWorkflow(workflowId); const repair = this.task(wf, taskId);
    repair.status = result.status === "PASS" ? "passed" : "blocked"; repair.result = result; repair.finishedAt = now();
    if (repair.status === "passed" && repair.repairs) { const source = this.task(wf, repair.repairs); source.status = "ready"; source.result = null; }
    this.advance(wf); await this.save(); return repair;
  }
  async heartbeat(agent, details = {}) { this.data.agents[agent] = { agent, ...details, seenAt: now() }; await this.save(); return this.data.agents[agent]; }
}
