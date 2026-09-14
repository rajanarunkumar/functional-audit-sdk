"""functional_audit — transformation contract and evidence framework.

Public surface:
    fa.stage(contract_id)       decorator for governed stages
    fa.sql(sql, contract_id)    governed SQL stage
    fa.explain_table(name)      business table joined to audit context
    fa.business_columns(df)     strip reserved system columns
    fa.diff_runs(a, b)          decompose the difference between two runs
"""
from functional_audit.config import Settings
from functional_audit.runtime.stage import stage, sql, RunOptions, StageError
from functional_audit.runtime.explain import explain_table, business_columns, diff_runs

__version__ = "0.2.0"
__all__ = ["stage", "sql", "RunOptions", "StageError", "explain_table", "business_columns", "diff_runs", "Settings"]
