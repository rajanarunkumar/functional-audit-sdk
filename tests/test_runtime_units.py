"""Runtime units that do not need a SparkSession."""
import time

from functional_audit.ids import uuid7, content_hash
from functional_audit.runtime.explain import view_name, view_sql
from functional_audit.init.migrate import load_migrations, render, split_statements, checksum
from functional_audit.config import Settings, check_fqn, check_identifier
import pytest


def test_uuid7_is_time_ordered():
    a = uuid7()
    time.sleep(0.002)
    b = uuid7()
    assert a < b


def test_content_hash_is_order_independent():
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})


def test_view_naming_is_domain_agnostic():
    s = Settings()
    assert view_name(s, "cap_gold.retail.rwa") == "platform_gov.functional_audit_views.cap_gold__retail__rwa"
    assert "t.__run_id" in view_sql(s, "cap_gold.retail.rwa") and "t.*" in view_sql(s, "cap_gold.retail.rwa")
    assert "e.reporting_period AS fa_reporting_period" in view_sql(s, "cap_gold.retail.rwa")
    with pytest.raises(ValueError):
        view_sql(s, "cap_gold.retail.rwa; DROP TABLE x")


def test_migrations_render_split_and_checksum():
    ms = load_migrations()
    assert [m.version for m in ms] == ["001", "002"]
    stmts = split_statements(render(ms[0].sql, Settings()))
    assert any("CREATE TABLE IF NOT EXISTS platform_gov.functional_audit.runs" in x for x in stmts)
    assert not any("${" in x for x in stmts)
    assert any("CREATE OR REPLACE FUNCTION platform_gov.functional_audit.explain" in x
               for x in split_statements(render(ms[1].sql, Settings())))
    assert checksum(ms[0].sql) != checksum(ms[1].sql) and len(checksum(ms[0].sql)) == 64


def test_split_statements_ignores_semicolons_inside_literals_and_comments():
    sql = "CREATE TABLE t (c STRING COMMENT 'a; b'); -- trailing; comment\nINSERT INTO t VALUES ('x;y');"
    stmts = split_statements(sql)
    assert stmts == ["CREATE TABLE t (c STRING COMMENT 'a; b')", "INSERT INTO t VALUES ('x;y')"]


def test_settings_validation_rejects_unsafe_identifiers(monkeypatch):
    monkeypatch.setenv("FA_CATALOG", "plat form")
    with pytest.raises(ValueError):
        Settings.from_env()
    monkeypatch.setenv("FA_CATALOG", "platform_gov")
    monkeypatch.setenv("FA_RUN_ID_COLUMN", "run_id")
    with pytest.raises(ValueError):
        Settings.from_env()
    assert check_identifier("ok_1") == "ok_1" and check_fqn("a.b.c") == "a.b.c"
    with pytest.raises(ValueError):
        check_fqn("a.b.c.d.e")
