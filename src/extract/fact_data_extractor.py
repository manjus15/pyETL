from typing import Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class FactDataExtractor:
    """Extracts and processes fact data tables."""

    def __init__(self, spark: SparkSession, jdbc_url: str, properties: Dict[str, str]):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.properties = properties

    def extract_claims(self) -> DataFrame:
        """Extract claims data with transformations."""
        claims = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.f_claim_submission",
                properties=self.properties
            )
            .withColumn(
                "encounterstart_date",
                F.to_date(F.col("encounterstart"), "dd/MM/yyyy")
            )
            .withColumn(
                "cleaned_activityid",
                F.when(
                    F.col("activityid").contains("-I"),
                    F.expr("substring(activityid, 1, locate('-I', activityid) - 1)")
                ).when(
                    F.col("activityid").contains("-R"),
                    F.expr("substring(activityid, 1, locate('-R', activityid) - 1)")
                ).otherwise(F.col("activityid"))
            )
            .withColumn(
                "EM",
                F.when(
                    F.trim(F.col("activitycode")).isin(
                        "99201", "99202", "99203", "99204", "99205",
                        "99211", "99212", "99213", "99214", "99215",
                        "99281", "99282", "99283", "99284", "99285"
                    ),
                    F.trim(F.col("activitycode"))
                ).otherwise("NA")
            )
            .withColumn(
                "DXBConsult",
                F.when(
                    F.trim(F.col("activitycode")).isin(
                        "9", "9.01", "9.02", "10", "10.01", "10.02",
                        "11", "11.01", "11.02", "61.01", "61.02",
                        "61.03", "61.04", "61.08"
                    ),
                    F.trim(F.col("activitycode"))
                ).otherwise("NA")
            )
            .withColumn(
                "Consult",
                F.when(
                    (F.trim(F.col("activitycode")) >= "99201") &
                    (F.trim(F.col("activitycode")) <= "99499"),
                    F.trim(F.col("activitycode"))
                ).otherwise("NA")
            )
            .withColumn(
                "doc_id",
                F.when(F.col("clinician").isNull(), F.col("orderingclinician"))
                .otherwise(F.col("clinician"))
            )
            .withColumn(
                "ordering_doc_id",
                F.when(
                    (F.col("orderingclinician").isNull()) | (F.col("orderingclinician") == ""),
                    F.when(
                        F.col("charge_head").isin("OPDOC", "ROPDOC"),
                        F.col("payee_doctor_id")
                    ).otherwise(
                        F.when(F.col("prescribing_dr_id").isNull(),
                            F.when(F.col("payee_doctor_id").isNull(),
                                F.col("admitting_dr_id")
                            ).otherwise(F.col("payee_doctor_id"))
                        ).otherwise(F.col("prescribing_dr_id"))
                    )
                ).otherwise(F.col("orderingclinician"))
            )
        )

        return claims

    def extract_remittance(self) -> DataFrame:
        """Extract remittance data with transformations."""
        window_spec = Window.partitionBy("claim_id", "activity_id")
        
        remittance = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.f_remittance_advice",
                properties=self.properties
            )
            .withColumn(
                "ra_date",
                F.floor(F.col("transaction_date"))
            )
            .withColumn(
                "ra_year",
                F.year(F.col("transaction_date"))
            )
            .withColumn(
                "ra_month",
                F.month(F.col("transaction_date"))
            )
            .withColumn(
                "first_received",
                F.when(F.col("ra_seq") == 2, F.col("transaction_date"))
            )
            .withColumn(
                "second_received",
                F.when(F.col("ra_seq") == 3, F.col("transaction_date"))
            )
            .withColumn(
                "third_received",
                F.when(F.col("ra_seq") == 4, F.col("transaction_date"))
            )
            .withColumn(
                "_join_key",
                F.expr("monotonically_increasing_id()")
            )
            .withColumn(
                "ra_join_key",
                F.expr("monotonically_increasing_id()")
            )
        )

        # Get latest denial codes
        latest_denials = (
            remittance
            .select("claim_id", "activity_id", "denial_code", "ra_seq")
            .where(F.col("denial_code").isNotNull())
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("claim_id", "activity_id")
                    .orderBy(F.col("ra_seq").desc())
                )
            )
            .where(F.col("rn") == 1)
            .select(
                F.col("claim_id").alias("denial_claim_id"),
                F.col("activity_id").alias("denial_activity_id"),
                F.col("denial_code").alias("latest_denial_code")
            )
        )

        return remittance, latest_denials

    def extract_open_claims(self) -> DataFrame:
        """Extract open claims data with transformations."""
        return (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.f_open_claims",
                properties=self.properties
            )
            .withColumn(
                "encounter_start_date",
                F.to_date(F.substring(F.col("start_date"), 1, 10), "dd/MM/yyyy")
            )
            .withColumn(
                "enc_date",
                F.floor(F.to_date(F.substring(F.col("start_date"), 1, 10)))
            )
            .withColumn(
                "enc_year",
                F.year(F.to_date(F.substring(F.col("start_date"), 1, 10)))
            )
            .withColumn(
                "enc_month",
                F.month(F.to_date(F.substring(F.col("start_date"), 1, 10)))
            )
            .withColumn(
                "ordering_doc_id",
                F.when(F.col("prescribing_dr_id").isNull(),
                    F.when(F.col("payee_doctor_id").isNull(),
                        F.col("admitting_dr_id")
                    ).otherwise(F.col("payee_doctor_id"))
                ).otherwise(F.col("prescribing_dr_id"))
            )
            .withColumn(
                "doctor_id",
                F.when(F.col("payee_doctor_id").isNull(),
                    F.when(F.col("prescribing_dr_id").isNull(),
                        F.col("admitting_dr_id")
                    ).otherwise(F.col("prescribing_dr_id"))
                ).otherwise(F.col("payee_doctor_id"))
            )
            .withColumn("claim_status", F.lit("U"))
            .withColumn("claimstatus", F.lit("O"))
            .withColumn("new_claim_status", F.lit("Open"))
            .withColumn("org_sub_seq", F.lit(1))
            .withColumn("cor_sub_seq", F.lit(1))
            .withColumn("patientshare", F.lit(0))
        )

    def extract_all_fact_data(self, output_path: str) -> None:
        """Extract all fact data and save to parquet files."""
        # Extract and save claims
        claims_df = self.extract_claims()
        claims_df.write.mode("overwrite").parquet(f"{output_path}/claims.parquet")

        # Extract and save remittance
        remittance_df, latest_denials_df = self.extract_remittance()
        remittance_df.write.mode("overwrite").parquet(f"{output_path}/remittance.parquet")
        latest_denials_df.write.mode("overwrite").parquet(f"{output_path}/latest_denials.parquet")

        # Extract and save open claims
        open_claims_df = self.extract_open_claims()
        open_claims_df.write.mode("overwrite").parquet(f"{output_path}/open_claims.parquet") 