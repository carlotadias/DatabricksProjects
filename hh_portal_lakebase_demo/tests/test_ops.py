"""hhportal_data.ops: the daily job's sync and refresh steps with fakes (nothing is called)."""

from types import SimpleNamespace as NS

import pytest

from hhportal_data import gold as pg
from hhportal_data import ops


class FakePipelines:
    def __init__(self, states, conflict=False):
        self.states = list(states)
        self.conflict = conflict
        self.started = []

    def start_update(self, pipeline_id, full_refresh=False):
        if self.conflict:
            raise RuntimeError("ResourceConflict: An active update already exists for this pipeline")
        self.started.append((pipeline_id, full_refresh))
        return NS(update_id="u-1")

    def get(self, pipeline_id):
        return NS(latest_updates=[NS(update_id="u-running")])

    def get_update(self, pipeline_id, update_id):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return NS(update=NS(state=NS(value=state), update_id=update_id))


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def synced(state="SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"):
    return {"status": {"pipeline_id": "p-1", "detailed_state": state}}


def test_sync_starts_an_incremental_update_and_waits_for_completion():
    clock, calls = Clock(), []

    def api_get(path):
        calls.append(path)
        return synced()

    pipes = FakePipelines(["QUEUED", "RUNNING", "COMPLETED"])
    out = ops.sync_silver(api_get, pipes, "cat.portal.silver_hh_readings", sleep=clock.sleep, clock=clock, poll_s=30)
    assert calls[0] == "/api/2.0/postgres/synced_tables/cat.portal.silver_hh_readings"
    assert pipes.started == [("p-1", False)]  # never a full refresh
    assert out["state"] == "COMPLETED" and out["update_id"] == "u-1" and out["pipeline_id"] == "p-1"
    assert out["synced_state"].startswith("SYNCED_TABLE_ONLINE") and clock.t == 60


def test_sync_waits_for_an_update_that_is_already_running():
    pipes = FakePipelines(["RUNNING", "COMPLETED"], conflict=True)
    clock = Clock()
    out = ops.sync_silver(lambda p: synced(), pipes, "x.y.z", sleep=clock.sleep, clock=clock)
    assert out["update_id"] == "u-running" and out["state"] == "COMPLETED"


def test_sync_fails_on_a_failed_update_or_timeout():
    clock = Clock()
    with pytest.raises(RuntimeError, match="FAILED"):
        ops.sync_silver(lambda p: synced(), FakePipelines(["FAILED"]), "x.y.z", sleep=clock.sleep, clock=clock)
    clock = Clock()
    with pytest.raises(RuntimeError, match="not finished"):
        ops.sync_silver(lambda p: synced(), FakePipelines(["RUNNING"]), "x.y.z", sleep=clock.sleep, clock=clock,
                        timeout_s=120, poll_s=60)


class FakeConn:
    def __init__(self, fail_on=None):
        self.statements, self.fail_on = [], fail_on

    def execute(self, sql):
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("could not create unique index")

        class R:
            def fetchone(self):
                return (7,)
        return R()


def test_refresh_gold_refreshes_concurrently_in_order_counts_and_analyzes():
    stmts = pg.load()
    conn, clock = FakeConn(), Clock()
    rows = ops.refresh_gold(conn, stmts, clock=clock)
    assert conn.statements[0] == "SET statement_timeout = 0"
    refreshes = [s for s in conn.statements if s.startswith("REFRESH")]
    assert "REFRESH MATERIALIZED VIEW portal_work.hh_account_totals" in refreshes
    assert refreshes == [s for _, s in pg.refresh_statements(stmts)]
    assert conn.statements.index("REFRESH MATERIALIZED VIEW CONCURRENTLY portal_gold.directory") < \
        conn.statements.index("SELECT count(*) FROM portal_gold.directory")
    assert [r["step"] for r in rows] == [f"refresh_gold:{mv}" for mv, _ in pg.refresh_statements(stmts)] + ["analyze"]
    assert "refresh_gold:portal_work.hh_account_totals" in [r["step"] for r in rows]
    assert all(r["status"] == "ok" for r in rows) and rows[0]["rows"] == 7
    assert any(s.startswith("ANALYZE portal_gold.") for s in conn.statements)


def test_refresh_gold_stops_at_the_first_failure_and_reports_it():
    conn = FakeConn(fail_on="portal_gold.daily_account")
    with pytest.raises(ops.StepFailed) as e:
        ops.refresh_gold(conn, pg.load(), clock=Clock())
    rows = e.value.rows
    assert rows[-1]["step"] == "refresh_gold:daily_account" and rows[-1]["status"] == "failed"
    assert "unique index" in rows[-1]["detail"]
    assert not any("monthly_account" in s for s in conn.statements)
