"""Example governed stages for the rwa_retail calculation."""
from pyspark.sql import functions as F
import functional_audit as fa


@fa.stage("rwa_retail.ead")
def build_ead(inputs, reporting_period: str = "2027-03-31"):
    exp = inputs["cap_silver.retail.exposures"]
    ccf = inputs["cap_ref.capital.ccf_offbalance"]
    return (exp.join(ccf, "facility_type", "left")
               .withColumn("ead", F.col("drawn") + F.coalesce(F.col("ccf"), F.lit(0)) * F.col("undrawn"))
               .withColumn("reporting_period", F.lit(reporting_period))
               .select("exposure_id", "reporting_period", "product", "lien_position", "first_lien_owned",
                       "drawn", "undrawn", "ead"))


@fa.stage("rwa_retail.risk_weight")
def build_rwa(inputs, reporting_period: str = "2027-03-31"):
    ead = inputs["cap_gold.retail.ead"]
    bands = inputs["cap_ref.capital.rw_resi_ltv"]
    vals = inputs["cap_silver.retail.property_valuations"]
    with_ltv = (ead.join(vals.select("exposure_id", "property_value"), "exposure_id", "left")
                   .withColumn("ltv", F.col("ead") / F.col("property_value")))
    joined = with_ltv.join(bands, (F.col("ltv") >= F.col("ltv_min")) & (F.col("ltv") < F.col("ltv_max")), "left")
    return (joined.withColumn("rw", F.coalesce(F.col("rw_band"), F.lit(1.0)))
                  .withColumn("rwa", F.col("ead") * F.col("rw"))
                  .select("exposure_id", "reporting_period", "product", "lien_position", "first_lien_owned",
                          "ead", "ltv", "rw", "rwa"))
