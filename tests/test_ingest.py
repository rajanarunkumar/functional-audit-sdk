"""Ingest statement shape against a recording session (system tables exist only on Databricks)."""
from datetime import datetime, timezone

import pytest

from functional_audit.config import Settings
from functional_audit.runtime import ingest as ingestmod


class _Result:
    def __init__(self, value=0):
        self._value = value

    def collect(self):
        return [[self._value]]


class RecordingSpark:
    def __init__(self):
        self.statements: list[str] = []

    def sql(self, q, **_):
        self.statements.append(q)
        return _Result(0)


def test_run_id_must_be_a_uuid():
    with pytest.raises(ValueError):
        ingestmod.ingest(RecordingSpark(), Settings(), datetime.now(timezone.utc), run_id="r' OR 1=1 --")


def test_statements_are_bounded_ranked_and_reap_abandoned_runs(monkeypatch):
    spark = RecordingSpark()
    calls = []
    monkeypatch.setattr(ingestmod.Store, "mark_abandoned", lambda self, hours: calls.append(hours))
    res = ingestmod.ingest(spark, Settings(abandon_after_hours=6), datetime(2026, 9, 1, tzinfo=timezone.utc),
                           run_id="01890000-0000-7000-8000-000000000000")
    text = "\n".join(spark.statements)
    since = "TIMESTAMP '2026-09-01 00:00:00'"
    assert text.count(f"x.created >= {since}") == 3                      # every NOT EXISTS is bounded
    assert "min(CASE match_method WHEN 'ENTITY' THEN 1 WHEN 'TAG' THEN 2 ELSE 3 END)" in text
    assert "m.best_rank <= 2" in text and "max(match_method)" not in text
    assert "SET match_method = 'TAG'" in text
    assert "r.run_id = '01890000-0000-7000-8000-000000000000'" in text
    assert calls == [6] and res["stale_provisional"] == 0 and res["since"] == "2026-09-01 00:00:00"
