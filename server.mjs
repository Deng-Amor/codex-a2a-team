import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Broker } from "./broker.mjs";

const root = dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PORT || 4318);
const broker = await new Broker(process.env.A2A_DATA_FILE || join(root, "data", "broker.json")).load();
const json = (res, code, value) => { res.writeHead(code, { "content-type": "application/json; charset=utf-8" }); res.end(JSON.stringify(value)); };
const body = async req => JSON.parse(await new Promise((ok, fail) => { let text = ""; req.on("data", chunk => text += chunk); req.on("end", () => ok(text || "{}")); req.on("error", fail); }));
const route = async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`); const parts = url.pathname.split("/").filter(Boolean);
  try {
    if (req.method === "GET" && url.pathname === "/api/state") return json(res, 200, broker.snapshot());
    if (req.method === "POST" && url.pathname === "/api/workflows") return json(res, 201, await broker.createWorkflow(await body(req)));
    if (req.method === "POST" && parts[0] === "api" && parts[1] === "workflows" && parts[3] === "confirm") return json(res, 200, await broker.confirm(parts[2]));
    if (req.method === "GET" && parts[0] === "api" && parts[1] === "agents" && parts[3] === "next") return json(res, 200, await broker.next(parts[2]));
    if (req.method === "POST" && parts[0] === "api" && parts[1] === "agents" && parts[3] === "heartbeat") return json(res, 200, await broker.heartbeat(parts[2], await body(req)));
    if (req.method === "POST" && url.pathname === "/api/messages") return json(res, 201, await broker.send(await body(req)));
    if (req.method === "POST" && parts[0] === "api" && parts[1] === "workflows" && parts[3] === "tasks" && parts[5] === "complete") {
      const payload = await body(req); const task = await broker.complete(parts[2], parts[4], payload); return json(res, 200, task);
    }
    if (req.method === "POST" && parts[0] === "api" && parts[1] === "workflows" && parts[3] === "tasks" && parts[5] === "repair-complete") {
      const payload = await body(req); const task = await broker.repairComplete(parts[2], parts[4], payload); return json(res, 200, task);
    }
    if (req.method === "GET" && (url.pathname === "/" || url.pathname.startsWith("/workflow/"))) { res.writeHead(200, { "content-type": "text/html; charset=utf-8" }); return res.end(await readFile(join(root, "dashboard.html"))); }
    json(res, 404, { error: "not found" });
  } catch (error) { json(res, 400, { error: error.message }); }
};
createServer(route).listen(port, "127.0.0.1", () => console.log(`A2A dashboard: http://127.0.0.1:${port}`));
