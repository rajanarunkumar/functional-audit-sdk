"""Canonical plan identity.

The governance identity of a stage is the server-side *optimized* logical plan — the same
string on classic and Spark Connect sessions — normalized so that regeneration differences
do not change it, with literal values lifted out. A run therefore carries two hashes:

  structure_hash   what the plan does: relations, joins, projections, expressions
  binding          the numeric/date literals (in plan order) the structure was bound to;
                   the stage combines them with pinned input versions into binding_hash

Normalization (in order): protect type parameters, strip expression/plan ids and
generated aliases, drop time-travel options, lift literals, sort commutative operands
(AND, OR, =, +, *), collapse whitespace.
"""
from __future__ import annotations
import contextlib
import hashlib
import io
import json
import re
from dataclasses import dataclass, field

from functional_audit.config import RESERVED_PREFIX

_SECTION = re.compile(r"^== (Parsed|Analyzed|Optimized) Logical Plan ==$|^== Physical Plan ==$", re.M)
_EXPR_ID = re.compile(r"#\d+L?")
_PLAN_ID = re.compile(r"plan_id=\d+")
_GEN_ALIAS = re.compile(r"_gen_alias_\d+|__auto_generated_subquery_name\w*|_col\d+")
_TIME_TRAVEL = re.compile(r"(?:,\s*)?(?:versionAsOf|timestampAsOf)=[^,\]\s]+|@v\d+\b")
_ROW_META = re.compile(r"_metadata\.row_(id|commit_version)")
_TYPE_PARAM = re.compile(r"\b(decimal|varchar|char)\((\d+)(?:,\s*(\d+))?\)", re.I)
_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b")
_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_NUMBER = re.compile(r"(?<![\w#.\-])-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?(?![\w.])")
_WS = re.compile(r"\s+")
_CATALOG_PREFIX = re.compile(r"\bspark_catalog\.")
_RELATION_V1 = re.compile(r"\bRelation\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,3})\[")
_RELATION_V2 = re.compile(r"\bRelationV2\[[^\]]*\]\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,3})\b")
_SUBQUERY_ALIAS = re.compile(r"\bSubqueryAlias\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,3})\b")


@dataclass
class PlanIdentity:
    structure_hash: str
    canonical_text: str
    literals: list[str] = field(default_factory=list)
    relations: set[str] = field(default_factory=set)
    analyzed_text: str = ""
    optimized_text: str = ""

    @property
    def binding(self) -> list[str]:
        return list(self.literals)


class PlanUnavailable(RuntimeError):
    """The session could not produce a server-side plan string."""


# --- plan text ---------------------------------------------------------------

def strip_reserved(df):
    cols = [c for c in df.columns if not c.startswith(RESERVED_PREFIX)]
    return df.select(*cols) if len(cols) != len(df.columns) else df


def explain_sections(df) -> dict[str, str]:
    """{'parsed','analyzed','optimized','physical'} from df.explain(mode='extended').

    Works identically on classic and Spark Connect DataFrames: both print the server's
    explain string, so the hash does not depend on which client produced the plan."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="extended")
    text = buf.getvalue()
    parts = _SECTION.split(text)
    # re.split with a group yields: [pre, name1, body1, name2, body2, ...]; the physical
    # header has no group so its name is None.
    out: dict[str, str] = {}
    for i in range(1, len(parts), 2):
        name = parts[i]
        body = parts[i + 1] if i + 1 < len(parts) else ""
        key = name.lower() if name else "physical"
        out.setdefault(key, body.strip())
    if "optimized" not in out or "analyzed" not in out:
        raise PlanUnavailable("explain(mode='extended') did not return analyzed and optimized plans")
    return out


# --- normalization ------------------------------------------------------------

def _protect_types(text: str) -> str:
    # decimal(10,2) -> decimal<p10s2>: the digits are glued to letters so the literal lifter skips them
    return _TYPE_PARAM.sub(lambda m: f"{m.group(1)}<p{m.group(2)}{'s' + m.group(3) if m.group(3) else ''}>", text)


def _lift_literals(text: str) -> tuple[str, list[str]]:
    lits: list[str] = []

    def take(m):
        lits.append(m.group(0))
        return "?"

    text = _TIMESTAMP.sub(take, text)
    text = _DATE.sub(take, text)
    text = _NUMBER.sub(take, text)
    return text, lits


def _split_top_level(s: str, sep: str) -> list[str]:
    parts, depth, start = [], 0, 0
    i = 0
    while i < len(s):
        c = s[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and s.startswith(sep, i):
            parts.append(s[start:i])
            i += len(sep)
            start = i
            continue
        i += 1
    parts.append(s[start:])
    return parts


def _sort_commutative(s: str) -> str:
    """Recursively sort the operands of top-level AND / OR / = inside every parenthesized group."""
    out, i, n = [], 0, len(s)
    while i < n:
        if s[i] == "(":
            depth, j = 1, i + 1
            while j < n and depth:
                depth += s[j] == "("
                depth -= s[j] == ")"
                j += 1
            inner = _sort_commutative(s[i + 1:j - 1])
            # Spark prints binary operators pairwise-parenthesized, so sorting each group's
            # top-level operands canonicalizes a AND b, a OR b, a = b, a + b and a * b
            for sep in (" AND ", " OR ", " = ", " + ", " * "):
                parts = _split_top_level(inner, sep)
                if len(parts) > 1:
                    inner = sep.join(sorted(p.strip() for p in parts))
                    break
            out.append("(" + inner + ")")
            i = j
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def normalize(plan_text_: str) -> tuple[str, list[str]]:
    """Canonical text and the lifted literal vector for one plan section."""
    t = _protect_types(plan_text_)
    t = _EXPR_ID.sub("#", t)
    t = _PLAN_ID.sub("", t)
    t = _GEN_ALIAS.sub("_alias_", t)
    t = _TIME_TRAVEL.sub("", t)
    t = _ROW_META.sub("_metadata.row", t)
    t = _CATALOG_PREFIX.sub("", t)
    t, lits = _lift_literals(t)
    t = _sort_commutative(t)
    t = _WS.sub(" ", t).strip()
    return t, lits


def hash_text(canonical_text: str) -> str:
    return hashlib.sha256(canonical_text.encode()).hexdigest()


def canonical_name(name: str) -> str:
    """Table names compare case-insensitively and without the implicit spark_catalog prefix."""
    return _CATALOG_PREFIX.sub("", name).lower()


def relations_in_plan(analyzed_text: str) -> set[str]:
    """Catalog tables read by the plan, from the analyzed plan's relation nodes."""
    found: set[str] = set()
    for rx in (_RELATION_V1, _RELATION_V2, _SUBQUERY_ALIAS):
        for m in rx.finditer(analyzed_text):
            name = canonical_name(m.group(1))
            if "." in name:
                found.add(name)
    return found


def identify(df) -> PlanIdentity:
    sections = explain_sections(strip_reserved(df))
    canonical, lits = normalize(sections["optimized"])
    return PlanIdentity(structure_hash=hash_text(canonical), canonical_text=canonical, literals=lits,
                        relations=relations_in_plan(sections["analyzed"]),
                        analyzed_text=sections["analyzed"], optimized_text=sections["optimized"])


def binding_hash(literals: list[str], input_versions: dict[str, int | None],
                 reporting_period: str | None, scenario_params: dict | None) -> str:
    payload = {"literals": literals, "inputs": sorted((k, v) for k, v in input_versions.items()),
               "reporting_period": reporting_period, "scenario_params": scenario_params or {}}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def plan_proto_bytes(df) -> bytes | None:
    """Serialized Spark Connect plan when the session is a Connect session; None otherwise."""
    try:
        return df._plan.to_proto(df.sparkSession._client).SerializeToString()  # type: ignore[attr-defined]
    except Exception:
        return None
