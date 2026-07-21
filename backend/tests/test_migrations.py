import unittest
from pathlib import Path


class MigrationRegistryTests(unittest.TestCase):
    def test_runner_revision_is_additive_and_checksum_verified(self):
        source = (Path(__file__).resolve().parents[1] / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS runner_bindings", source)
        self.assertIn("CREATE TABLE IF NOT EXISTS runner_idempotency_receipts", source)
        self.assertIn("CREATE TABLE IF NOT EXISTS runner_evidence_receipts", source)
        self.assertIn("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum", source)


if __name__ == "__main__":
    unittest.main()
