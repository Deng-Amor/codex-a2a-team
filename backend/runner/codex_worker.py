"""Local Codex CLI worker. It never accepts another adapter or a client-selected worktree."""
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


BASE_URL = os.environ.get("A2A_CONTROL_PLANE_URL", "http://127.0.0.1:8010")
TOKEN = os.environ["A2A_RUNNER_TOKEN"]
RUNTIME_ID = os.environ["A2A_RUNNER_RUNTIME_ID"]
AGENT_KEY = os.environ["A2A_RUNNER_AGENT_KEY"]


def call(path, payload, key):
    request = Request(BASE_URL + path, data=json.dumps(payload).encode(), method="POST", headers={
        "Authorization": "Bearer " + TOKEN, "Content-Type": "application/json", "Idempotency-Key": key,
    })
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read())
    except HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def evidence(command, result, started, finished):
    return {"command": " ".join(command), "exit_code": result.returncode,
            "stdout_summary": result.stdout[-16384:], "stderr_summary": result.stderr[-16384:],
            "started_at": started, "finished_at": finished, "artifacts": [],
            "tests": [{"name": "codex_cli", "status": "passed" if result.returncode == 0 else "failed",
                       "duration_ms": 0, "summary": "Codex CLI subprocess completed"}]}


def main():
    call("/api/v1/agent-runtimes/register", {"runtime_id": RUNTIME_ID, "agent_key": AGENT_KEY,
         "session_ref": os.environ.get("CODEX_SESSION_ID", "")}, "register-" + RUNTIME_ID)
    while True:
        call(f"/api/v1/agent-runtimes/{RUNTIME_ID}/heartbeat", {}, "runtime-" + str(time.time_ns()))
        claimed = call("/api/v1/worker/jobs/claim", {"runtime_id": RUNTIME_ID}, "claim-" + str(time.time_ns())).get("job")
        if not claimed:
            time.sleep(15)
            continue
        command = [os.environ.get("CODEX_CLI", "codex"), "exec", claimed["instructions"]]
        started = iso()
        # shell=False is deliberate: instructions never become shell syntax.
        process = subprocess.Popen(command, shell=False, cwd=claimed["binding"]["worktree_path"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.monotonic() + int(os.environ.get("CODEX_JOB_TIMEOUT_SECONDS", "3600"))
        while process.poll() is None:
            if time.monotonic() >= deadline:
                process.kill()
                break
            time.sleep(15)
            call(f"/api/v1/worker/jobs/{claimed['id']}/heartbeat", {"runtime_id": RUNTIME_ID,
                 "attempt_id": claimed["attempt_id"], "lease_version": claimed["lease_version"]}, "job-heartbeat-" + str(time.time_ns()))
        stdout, stderr = process.communicate()
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        finished = iso()
        body = {"runtime_id": RUNTIME_ID, "attempt_id": claimed["attempt_id"], "lease_version": claimed["lease_version"],
                "callback_id": "callback-" + hashlib.sha256((claimed["id"] + finished).encode()).hexdigest()[:32],
                "outcome": "succeeded" if result.returncode == 0 else "failed", "evidence": evidence(command, result, started, finished)}
        if result.returncode:
            body["error"] = {"code": "codex_cli_failed", "message": result.stderr[-4096:]}
        call(f"/api/v1/worker/jobs/{claimed['id']}/callback", body, "callback-" + body["callback_id"])


if __name__ == "__main__":
    main()
