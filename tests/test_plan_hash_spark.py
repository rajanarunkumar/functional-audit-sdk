"""The property the design depends on: regenerations of the same logic share a structure hash,
real logic changes do not, and parameters move to the binding vector. Run on real plans."""
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from functional_audit.runtime import plan_hash  # noqa: E402


def ead(spark, t, period="2027-03-31", ccf=None):
    exp = spark.table(t["exp"])
    ref = spark.table(ccf or t["ccf"])
    return (exp.join(ref, "facility_type", "left")
               .withColumn("ead", F.col("drawn") + F.coalesce(F.col("ccf"), F.lit(0)) * F.col("undrawn"))
               .withColumn("reporting_period", F.lit(period))
               .select("exposure_id", "reporting_period", "ead"))


def H(df):
    return plan_hash.identify(df).structure_hash


def test_same_code_is_deterministic(spark, tables):
    assert H(ead(spark, tables)) == H(ead(spark, tables))


def test_alias_names_do_not_change_hash(spark, tables):
    aliased = (spark.table(tables["exp"]).alias("e").join(spark.table(tables["ccf"]).alias("c"), "facility_type", "left")
               .withColumn("ead", F.col("drawn") + F.coalesce(F.col("ccf"), F.lit(0)) * F.col("undrawn"))
               .withColumn("reporting_period", F.lit("2027-03-31"))
               .select("exposure_id", "reporting_period", "ead"))
    assert H(aliased) == H(ead(spark, tables))


def test_withcolumn_chain_and_single_select_share_hash(spark, tables):
    one = (spark.table(tables["exp"]).join(spark.table(tables["ccf"]), "facility_type", "left")
           .select("exposure_id", F.lit("2027-03-31").alias("reporting_period"),
                   (F.col("drawn") + F.coalesce(F.col("ccf"), F.lit(0)) * F.col("undrawn")).alias("ead")))
    assert H(one) == H(ead(spark, tables))


def test_predicate_and_join_key_order_do_not_change_hash(spark, tables):
    exp = spark.table(tables["exp"])
    f1 = exp.filter((F.col("drawn") > 100) & (F.col("undrawn") > 0)).select("exposure_id")
    f2 = exp.filter((F.col("undrawn") > 0) & (F.col("drawn") > 100)).select("exposure_id")
    assert H(f1) == H(f2)
    ref = spark.table(tables["ccf"])
    j1 = exp.join(ref, exp.facility_type == ref.facility_type).select("exposure_id", "ccf")
    j2 = exp.join(ref, ref.facility_type == exp.facility_type).select("exposure_id", "ccf")
    assert H(j1) == H(j2)


def test_commutative_arithmetic_operand_order_does_not_change_hash(spark, tables):
    exp, ref = spark.table(tables["exp"]), spark.table(tables["ccf"])
    a = exp.join(ref, "facility_type").select((F.col("drawn") + F.col("ccf") * F.col("undrawn")).alias("x"))
    b = exp.join(ref, "facility_type").select((F.col("undrawn") * F.col("ccf") + F.col("drawn")).alias("x"))
    assert H(a) == H(b)
    assert H(exp.select((F.col("drawn") - F.col("undrawn")).alias("x"))) != H(exp.select((F.col("undrawn") - F.col("drawn")).alias("x")))


def test_sql_formatting_and_aliases_do_not_change_hash(spark, tables):
    a = spark.sql(f"SELECT e.exposure_id, e.drawn + coalesce(c.ccf, 0) * e.undrawn AS ead "
                  f"FROM {tables['exp']} e LEFT JOIN {tables['ccf']} c ON e.facility_type = c.facility_type")
    b = spark.sql(f"select x.exposure_id, x.drawn+coalesce(r.ccf,0)*x.undrawn as ead\n"
                  f"from {tables['exp']} x left join {tables['ccf']} r on r.facility_type=x.facility_type")
    assert H(a) == H(b)
    assert H(spark.sql(f"SELECT exposure_id, drawn * 2 AS ead FROM {tables['exp']}")) != H(a)


def test_reporting_period_moves_to_binding_not_structure(spark, tables):
    a, b = plan_hash.identify(ead(spark, tables, "2027-03-31")), plan_hash.identify(ead(spark, tables, "2027-06-30"))
    assert a.structure_hash == b.structure_hash
    assert "2027-03-31" in a.literals and "2027-06-30" in b.literals
    assert plan_hash.binding_hash(a.literals, {}, None, None) != plan_hash.binding_hash(b.literals, {}, None, None)


def test_threshold_change_is_a_binding_change(spark, tables):
    exp = spark.table(tables["exp"])
    a = plan_hash.identify(exp.filter(F.col("drawn") > 100).select("exposure_id"))
    b = plan_hash.identify(exp.filter(F.col("drawn") > 200).select("exposure_id"))
    assert a.structure_hash == b.structure_hash and a.literals != b.literals


def test_hardcoded_constant_replacing_join_changes_hash(spark, tables):
    hard = (spark.table(tables["exp"]).withColumn("ead", F.col("drawn") + F.lit(0.5) * F.col("undrawn"))
            .withColumn("reporting_period", F.lit("2027-03-31")).select("exposure_id", "reporting_period", "ead"))
    assert H(hard) != H(ead(spark, tables))


def test_same_schema_table_swap_changes_hash_and_is_visible(spark, tables):
    right, wrong = plan_hash.identify(ead(spark, tables)), plan_hash.identify(ead(spark, tables, ccf=tables["ccf_draft"]))
    assert right.structure_hash != wrong.structure_hash
    # OSS tables resolve under spark_catalog, which is stripped; on UC these are three-part names
    assert right.relations == {"cap_silver.exposures", "cap_ref.ccf_offbalance"}
    assert wrong.relations == {"cap_silver.exposures", "cap_ref.ccf_draft"}


def test_relations_parsed_from_v1_v2_and_alias_nodes():
    txt = ("Relation cap.s.a[x#1] parquet\n RelationV2[y#2] cap.s.b\n SubqueryAlias cap.s.c\n"
           " SubqueryAlias tmp\n Relation spark_catalog.d.e[z#3] parquet")
    assert plan_hash.relations_in_plan(txt) == {"cap.s.a", "cap.s.b", "cap.s.c", "d.e"}


def test_reserved_columns_are_stripped_before_hashing(spark, tables):
    df = ead(spark, tables)
    assert H(df.withColumn("__run_id", F.lit("x"))) == H(df)


def test_type_parameters_are_not_lifted_as_literals():
    text, lits = plan_hash.normalize("Project [cast(x#1 as decimal(10,2)) AS y#2, 3 AS z#3]")
    assert "decimal<p10s2>" in text and lits == ["3"]


def test_commutative_sort_is_recursive():
    a, _ = plan_hash.normalize("Filter ((a# > 1) AND ((b# = c#) OR (d# < 2)))")
    b, _ = plan_hash.normalize("Filter (((d# < 2) OR (c# = b#)) AND (a# > 1))")
    assert a == b
