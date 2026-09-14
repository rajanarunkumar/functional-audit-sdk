import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CONNECT = bool(os.getenv("FA_TEST_CONNECT"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark 4 session with Delta enabled (skips when pyspark is missing).

    Set FA_TEST_CONNECT=1 to run the plan-identity tests through an embedded Spark Connect
    server instead; Delta-backed tests skip in that mode."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    wh = tmp_path_factory.mktemp("warehouse")
    b = SparkSession.builder.appName("functional_audit-tests")
    if CONNECT:
        b = b.remote("local[2]")
    else:
        b = (b.master("local[2]")
              .config("spark.sql.warehouse.dir", str(wh))
              .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={wh}/derby")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        from delta import configure_spark_with_delta_pip
        b = configure_spark_with_delta_pip(b)
    s = (b.config("spark.sql.ansi.enabled", "true").config("spark.sql.session.timeZone", "UTC")
          .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    if not CONNECT:
        s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def delta(spark):
    if CONNECT:
        pytest.skip("Delta-backed tests run on the classic local session")
    return spark


@pytest.fixture(scope="session")
def tables(spark):
    """Three-part catalog tables shaped like the rwa_retail example."""
    fmt = "parquet" if CONNECT else "delta"
    spark.sql("CREATE DATABASE IF NOT EXISTS cap_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS cap_ref")
    spark.sql("CREATE DATABASE IF NOT EXISTS cap_gold")
    spark.createDataFrame([(i, "A" if i % 2 else "B", 100.0 + i, 10.0) for i in range(60)],
                          "exposure_id INT, facility_type STRING, drawn DOUBLE, undrawn DOUBLE"
                          ).write.format(fmt).mode("overwrite").saveAsTable("cap_silver.exposures")
    spark.createDataFrame([("A", 0.5), ("B", 0.2)], "facility_type STRING, ccf DOUBLE"
                          ).write.format(fmt).mode("overwrite").saveAsTable("cap_ref.ccf_offbalance")
    spark.createDataFrame([("A", 0.9), ("B", 0.9)], "facility_type STRING, ccf DOUBLE"
                          ).write.format(fmt).mode("overwrite").saveAsTable("cap_ref.ccf_draft")
    return {"exp": "spark_catalog.cap_silver.exposures", "ccf": "spark_catalog.cap_ref.ccf_offbalance",
            "ccf_draft": "spark_catalog.cap_ref.ccf_draft"}


@pytest.fixture(scope="session")
def settings(delta, tmp_path_factory):
    """Audit schema settings for the local catalog; the stage reads them from the environment."""
    from functional_audit.config import Settings
    os.environ.update({"FA_CATALOG": "spark_catalog", "FA_SCHEMA": "functional_audit", "FA_ENFORCE": "warn",
                       "FA_TELEMETRY_TIER": "2", "FA_PLANS_PATH": str(tmp_path_factory.mktemp("plans"))})
    return Settings.from_env()


@pytest.fixture(scope="session")
def audit_schema(delta, settings):
    """The system schema created by the real migrations."""
    from functional_audit import __version__
    from functional_audit.init import migrate
    applied = migrate.apply(delta, settings, __version__)
    assert applied == ["001", "002"]
    assert migrate.apply(delta, settings, __version__) == []      # idempotent
    return settings
