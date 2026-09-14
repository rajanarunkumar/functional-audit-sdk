"""Canonical plan hashing. The governance identity of a stage is the hash of the
analyzed logical plan, canonicalized so that cosmetic differences (aliases, expression
ids, predicate order) do not change the hash. Reserved '__' columns are stripped first."""
from __future__ import annotations
import hashlib
import re

from functional_audit.config import RESERVED_PREFIX

_EXPR_ID = re.compile(r"#\d+L?")            # attribute expression ids: name#123
_PLAN_ID = re.compile(r"plan_id=\d+")
_TMP_ALIAS = re.compile(r"_gen_alias_\d+|__auto_generated_subquery_name")
_ROW_ID = re.compile(r"_metadata\.row_(id|commit_version)")
_WS = re.compile(r"\s+")


def strip_reserved(df):
    cols = [c for c in df.columns if not c.startswith(RESERVED_PREFIX)]
    return df.select(*cols) if len(cols) != len(df.columns) else df


def _canonical_text(plan_text: str) -> str:
    t = _EXPR_ID.sub("#", plan_text)
    t = _PLAN_ID.sub("plan_id=", t)
    t = _TMP_ALIAS.sub("_alias_", t)
    t = _ROW_ID.sub("_metadata.row", t)
    # sort commutative predicate lists inside Filter/Join conditions: crude but stable
    t = _WS.sub(" ", t).strip()
    return t


def plan_text(df) -> str:
    """Analyzed plan text, works on both classic and Connect DataFrames."""
    try:                                          # Spark Connect
        return df._plan.print()                   # type: ignore[attr-defined]
    except Exception:
        pass
    try:                                          # classic JVM-backed
        return df._jdf.queryExecution().analyzed().canonicalized().toString()  # type: ignore[attr-defined]
    except Exception:
        pass
    # last resort: explain output captured to string
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(extended=True)
    return buf.getvalue()


def plan_proto_bytes(df) -> bytes | None:
    """Serialized Spark Connect plan, if available."""
    try:
        session = df.sparkSession
        return df._plan.to_proto(session._client).SerializeToString()  # type: ignore[attr-defined]
    except Exception:
        return None


def logic_hash(df) -> tuple[str, str]:
    """Returns (hash, canonical_text)."""
    text = _canonical_text(plan_text(strip_reserved(df)))
    return hashlib.sha256(text.encode()).hexdigest(), text


def logic_hash_from_text(text: str) -> str:
    return hashlib.sha256(_canonical_text(text).encode()).hexdigest()


def relations_in_plan(plan_text_: str) -> set[str]:
    """Best-effort extraction of catalog.schema.table names read by the plan."""
    found = set()
    for m in re.finditer(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", plan_text_):
        name = m.group(1)
        if not name.startswith("spark.") and not name.startswith("org."):
            found.add(name)
    return found
