"""Contract guards for the authenticated, versioned Codex CLI Runner API."""
import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "app" / "main.py"
WORKER = ROOT / "runner" / "codex_worker.py"


class CodexRunnerContractTests(unittest.TestCase):
    def test_exactly_six_versioned_runner_posts_and_no_legacy_posts(self):
        tree = ast.parse(MAIN.read_text(encoding="utf-8"))
        paths = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute) and decorator.func.attr == "post":
                        if decorator.args and isinstance(decorator.args[0], ast.Constant):
                            paths.append(decorator.args[0].value)
        runner_paths = [path for path in paths if "agent-runtimes" in path or "/worker/jobs" in path]
        self.assertEqual(set(path for path in runner_paths if path.startswith("/api/v1/")), {
            "/api/v1/agent-runtimes/register", "/api/v1/agent-runtimes/{runtime_id}/heartbeat",
            "/api/v1/worker/jobs/claim", "/api/v1/worker/jobs/{job_id}/heartbeat",
            "/api/v1/worker/jobs/{job_id}/callback", "/api/v1/worker/jobs/reap-expired",
        })
        self.assertNotIn("/api/agent-runtimes/register", paths)
        self.assertNotIn("/api/worker/jobs/claim", paths)

    def test_runner_is_codex_cli_only_and_never_uses_a_shell(self):
        source = WORKER.read_text(encoding="utf-8")
        self.assertIn('"codex"', source)
        self.assertIn("shell=False", source)
        self.assertIn("/jobs/{claimed['id']}/heartbeat", source)
        self.assertNotIn("workbuddy", source.lower())

    def test_migration_registry_and_immutable_receipts_are_present(self):
        source = MAIN.read_text(encoding="utf-8")
        self.assertIn('"20260721_codex_cli_runner"', source)
        self.assertIn("migration checksum mismatch", source)
        self.assertIn("uq_runner_receipt_actor_endpoint_key", source)
        self.assertIn("uq_runner_evidence_callback", source)
        self.assertIn("canonical_json", source)
        self.assertIn("callback_sha256", source)

    def test_structured_evidence_rejects_uncontracted_properties(self):
        source = MAIN.read_text(encoding="utf-8")
        self.assertIn("set(value) !=", source)
        self.assertIn("len(value[\"artifacts\"]) > 50", source)
        self.assertIn("len(value[\"tests\"]) > 100", source)


if __name__ == "__main__":
    unittest.main()
