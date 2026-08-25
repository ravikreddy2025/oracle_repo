"""Keep the shipped example working.

An example that has quietly rotted is worse than no example, and this one is
the first thing a new developer runs.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples" / "simulated_run.py"


@pytest.fixture(scope="module")
def example():
    spec = importlib.util.spec_from_file_location("simulated_run", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["simulated_run"] = module
    spec.loader.exec_module(module)
    return module


class TestSimulatedRun:
    def test_completes_successfully(self, example, capsys):
        assert example.main(["--env", "dev"]) == 0

    def test_every_table_succeeds(self, example, capsys):
        example.main(["--env", "dev"])
        out = capsys.readouterr().out
        assert "status   : SUCCEEDED" in out
        assert out.count("SUCCEEDED  rows=") == 3

    def test_shows_the_three_generated_statements(self, example, capsys):
        example.main(["--env", "dev", "--sql-only"])
        out = capsys.readouterr().out
        assert "FROM GLOWNER.GL_TRANSACTIONS" in out       # Oracle extract
        assert "ROW_NUMBER() OVER" in out                   # dedupe before merge
        assert "MERGE INTO dev_lakehouse.bronze" in out     # Delta load

    def test_watermark_advances_for_incremental_tables(self, example, capsys):
        example.main(["--env", "dev"])
        out = capsys.readouterr().out
        assert "finance.gl_transactions    SUCCEEDED  rows=1000 watermark=advanced" in out
        # A full-load table has no watermark to move.
        assert "finance.gl_accounts        SUCCEEDED  rows=1000 watermark=held" in out

    def test_reconciliation_balances_for_every_table(self, example, capsys):
        example.main(["--env", "dev"])
        out = capsys.readouterr().out
        reconciliation = out.split("Reconciliation")[-1]
        assert "FAILED" not in reconciliation

    def test_audit_trail_covers_the_lifecycle(self, example, capsys):
        example.main(["--env", "dev"])
        out = capsys.readouterr().out
        for event in ["RUN_STARTED", "TASK_STARTED", "EXTRACT_DONE", "LOAD_DONE",
                      "WATERMARK_ADVANCED", "TASK_SUCCEEDED", "RUN_FINISHED"]:
            assert event in out

    def test_scn_table_uses_a_numeric_bound(self, example, capsys):
        example.main(["--env", "dev", "--table", "sales.order_events", "--sql-only"])
        out = capsys.readouterr().out
        assert "ORA_ROWSCN" in out
        assert "TO_TIMESTAMP" not in out.split("1. Oracle source query")[1].split("---")[0]

    def test_runs_against_prod_config_too(self, example, capsys):
        assert example.main(["--env", "prod"]) == 0

    def test_states_what_was_stubbed(self, example, capsys):
        # The example must not read as proof that Delta was exercised.
        example.main(["--env", "dev"])
        assert "stubbed" in capsys.readouterr().out
