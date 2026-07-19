"""Stage-one, deterministic LangGraph repair loop.

This graph deliberately has no database, LLM, CLI, or HTTP side effects.  The
FastAPI layer can later persist the returned events/tasks with an outbox and
use a Postgres checkpointer.  Keeping this module pure makes replay safe.
"""

from __future__ import annotations

from operator import add
from typing import Annotated, Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph


GRAPH_VERSION = "langgraph_v1"
STATE_SCHEMA_VERSION = 1
MAX_TASK_ITERATIONS = 3


class LoopState(TypedDict, total=False):
    # Frozen workflow identity; workflow_id is also the future LangGraph thread_id.
    workflow_id: str
    run_id: str
    engine: str
    graph_version: str
    state_schema_version: int
    request: str

    # External-worker correlation fields.  Stage one does not claim or execute jobs.
    job_id: str
    attempt_id: str
    lease_version: int
    callback_id: str
    idempotency_key: str
    worker_leases: dict[str, dict[str, Any]]

    contract: dict[str, Any]
    contract_revision: int
    contract_revision_requested: bool
    contract_audit_outcomes: list[str]
    contract_audit_cursor: int
    contract_audit_decision: str

    frontend_artifact: dict[str, Any]
    frontend_iterations: int
    audit_outcomes: list[str]
    audit_cursor: int
    audit_decision: str
    defects_for_frontend: list[dict[str, str]]
    max_task_iterations: int
    escalation_reason: str
    workflow_status: str

    workflow_event_sequence: int
    events: Annotated[list[dict[str, Any]], add]


def new_state(workflow_id: str, request: str, run_id: str = "run_1") -> LoopState:
    """Return the smallest valid input for a new stage-one run."""
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "engine": GRAPH_VERSION,
        "graph_version": GRAPH_VERSION,
        "state_schema_version": STATE_SCHEMA_VERSION,
        "request": request,
        "job_id": "",
        "attempt_id": "",
        "lease_version": 0,
        "callback_id": "",
        "idempotency_key": f"{workflow_id}:{run_id}:start",
        "worker_leases": {},
        "contract": {},
        "contract_revision": 0,
        "contract_revision_requested": False,
        "contract_audit_outcomes": [],
        "contract_audit_cursor": 0,
        "contract_audit_decision": "",
        "frontend_artifact": {},
        "frontend_iterations": 0,
        "audit_outcomes": [],
        "audit_cursor": 0,
        "audit_decision": "",
        "defects_for_frontend": [],
        "max_task_iterations": MAX_TASK_ITERATIONS,
        "escalation_reason": "",
        "workflow_status": "running",
        "workflow_event_sequence": 0,
        "events": [],
    }


def _event(state: LoopState, node: str, event: str, detail: str) -> dict[str, Any]:
    sequence = state.get("workflow_event_sequence", 0) + 1
    return {
        "sequence": sequence,
        "node": node,
        "event": event,
        "detail": detail,
        "idempotency_key": f"{state['workflow_id']}:{state['run_id']}:{node}:{sequence}",
    }


def _next_outcome(state: LoopState, key: str, cursor_key: str) -> tuple[str, int]:
    outcomes = state.get(key, [])
    cursor = state.get(cursor_key, 0)
    return (outcomes[cursor].upper() if cursor < len(outcomes) else "PASS", cursor + 1)


def team_lead(state: LoopState) -> dict[str, Any]:
    """Produce/revise a REST contract, or stop at the human escalation gate."""
    if state.get("escalation_reason"):
        event = _event(state, "team_lead", "escalated", state["escalation_reason"])
        return {
            "workflow_status": "waiting_human",
            "workflow_event_sequence": event["sequence"],
            "events": [event],
        }

    revision = state.get("contract_revision", 0) + 1
    event = _event(state, "team_lead", "contract_ready", f"REST API Contract revision {revision}")
    return {
        "contract_revision": revision,
        "contract_revision_requested": False,
        "contract": {
            "revision": revision,
            "style": "RESTful",
            "endpoints": [
                {"method": "GET", "path": "/api/v1/resources"},
                {"method": "POST", "path": "/api/v1/resources"},
            ],
        },
        "workflow_event_sequence": event["sequence"],
        "events": [event],
    }


def contract_audit(state: LoopState) -> dict[str, Any]:
    outcome, cursor = _next_outcome(state, "contract_audit_outcomes", "contract_audit_cursor")
    event = _event(state, "contract_audit", "passed" if outcome == "PASS" else "defects", outcome)
    return {
        "contract_audit_cursor": cursor,
        "contract_audit_decision": "pass" if outcome == "PASS" else "defects",
        "contract_revision_requested": outcome != "PASS",
        "workflow_event_sequence": event["sequence"],
        "events": [event],
    }


def frontend(state: LoopState) -> dict[str, Any]:
    iteration = state.get("frontend_iterations", 0) + 1
    repairing = bool(state.get("defects_for_frontend"))
    mode = "repair" if repairing else "implement"
    event = _event(state, "frontend", mode, f"frontend attempt {iteration}")
    return {
        "frontend_iterations": iteration,
        "frontend_artifact": {
            "kind": "frontend_change",
            "mode": mode,
            "contract_revision": state.get("contract_revision", 0),
            "defects_addressed": list(state.get("defects_for_frontend", [])),
        },
        "defects_for_frontend": [],
        "workflow_event_sequence": event["sequence"],
        "events": [event],
    }


def audit(state: LoopState) -> dict[str, Any]:
    outcome, cursor = _next_outcome(state, "audit_outcomes", "audit_cursor")
    max_iterations = state.get("max_task_iterations", MAX_TASK_ITERATIONS)
    if outcome == "PASS":
        decision: Literal["pass", "defects", "escalate"] = "pass"
        defects: list[dict[str, str]] = []
        detail = "audit passed"
    elif state.get("frontend_iterations", 0) >= max_iterations:
        decision = "escalate"
        defects = []
        detail = f"frontend exceeded {max_iterations} repair attempts"
    else:
        decision = "defects"
        defects = [{"defect_id": f"audit-{cursor}", "owner": "frontend", "detail": "deterministic audit defect"}]
        detail = "audit returned defects to frontend"
    event = _event(state, "audit", decision, detail)
    return {
        "audit_cursor": cursor,
        "audit_decision": decision,
        "defects_for_frontend": defects,
        "escalation_reason": detail if decision == "escalate" else "",
        "workflow_status": "completed" if decision == "pass" else "running",
        "workflow_event_sequence": event["sequence"],
        "events": [event],
    }


def _after_team_lead(state: LoopState) -> str:
    return END if state.get("workflow_status") == "waiting_human" else "contract_audit"


def _after_contract_audit(state: LoopState) -> str:
    return "team_lead" if state.get("contract_audit_decision") == "defects" else "frontend"


def _after_audit(state: LoopState) -> str:
    decision = state.get("audit_decision")
    return {"pass": END, "defects": "frontend", "escalate": "team_lead"}[decision]


def build_stage_one_graph(*, checkpointer: Any = None):
    graph = StateGraph(LoopState)
    graph.add_node("team_lead", team_lead)
    graph.add_node("contract_audit", contract_audit)
    graph.add_node("frontend", frontend)
    graph.add_node("audit", audit)
    graph.add_edge(START, "team_lead")
    graph.add_conditional_edges("team_lead", _after_team_lead)
    graph.add_conditional_edges("contract_audit", _after_contract_audit)
    graph.add_edge("frontend", "audit")
    graph.add_conditional_edges("audit", _after_audit)
    return graph.compile(checkpointer=checkpointer)


def demo() -> None:
    graph = build_stage_one_graph()
    repaired = graph.invoke({**new_state("wf_demo", "修改前端节点"), "audit_outcomes": ["DEFECTS", "PASS"]})
    assert repaired["workflow_status"] == "completed"
    assert repaired["frontend_iterations"] == 2
    assert {"defects", "repair", "passed"} <= {event["event"] for event in repaired["events"]}
    assert [event["sequence"] for event in repaired["events"]] == list(range(1, len(repaired["events"]) + 1))

    revised = graph.invoke({**new_state("wf_contract", "修改前端节点"), "contract_audit_outcomes": ["DEFECTS", "PASS"]})
    assert revised["contract_revision"] == 2
    assert revised["workflow_status"] == "completed"

    escalated = graph.invoke({**new_state("wf_escalate", "修改前端节点"), "audit_outcomes": ["DEFECTS"] * 3})
    assert escalated["workflow_status"] == "waiting_human"
    assert "exceeded" in escalated["escalation_reason"]


if __name__ == "__main__":
    demo()
    print("stage-one loop demo passed")
