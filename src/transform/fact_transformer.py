from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class FactTransformer:
    """Transforms and joins fact data with master data."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def join_master_data(
        self,
        claims_df: DataFrame,
        remittance_df: DataFrame,
        open_claims_df: DataFrame,
        master_data: Dict[str, DataFrame]
    ) -> DataFrame:
        """Join fact data with master data tables."""
        
        # Join claims with master data
        enriched_claims = (
            claims_df
            .join(
                master_data["hospitals"],
                claims_df.hospital_id == master_data["hospitals"].hospital_id,
                "left"
            )
            .join(
                master_data["insurance_companies"],
                claims_df.insurance_id == master_data["insurance_companies"].insurance_id,
                "left"
            )
            .join(
                master_data["plans"],
                claims_df.plan_id == master_data["plans"].plan_id,
                "left"
            )
            .join(
                master_data["doctors"].alias("doc"),
                claims_df.doc_id == F.col("doc.doctor_id"),
                "left"
            )
            .join(
                master_data["doctors"].alias("ordering_doc"),
                claims_df.ordering_doc_id == F.col("ordering_doc.doctor_id"),
                "left"
            )
            .join(
                master_data["encounter_types"],
                claims_df.encounter_type == master_data["encounter_types"].encounter_type_id,
                "left"
            )
        )

        # Join remittance with master data
        enriched_remittance = (
            remittance_df
            .join(
                master_data["denial_codes"],
                remittance_df.denial_code == master_data["denial_codes"].denial_code,
                "left"
            )
        )

        # Join open claims with master data
        enriched_open_claims = (
            open_claims_df
            .join(
                master_data["hospitals"],
                open_claims_df.hospital_id == master_data["hospitals"].hospital_id,
                "left"
            )
            .join(
                master_data["insurance_companies"],
                open_claims_df.insurance_id == master_data["insurance_companies"].insurance_id,
                "left"
            )
            .join(
                master_data["plans"],
                open_claims_df.plan_id == master_data["plans"].plan_id,
                "left"
            )
            .join(
                master_data["doctors"].alias("doc"),
                open_claims_df.doctor_id == F.col("doc.doctor_id"),
                "left"
            )
            .join(
                master_data["doctors"].alias("ordering_doc"),
                open_claims_df.ordering_doc_id == F.col("ordering_doc.doctor_id"),
                "left"
            )
        )

        return enriched_claims, enriched_remittance, enriched_open_claims

    def calculate_metrics(
        self,
        claims_df: DataFrame,
        remittance_df: DataFrame,
        open_claims_df: DataFrame
    ) -> DataFrame:
        """Calculate business metrics and KPIs."""
        
        # Calculate claims metrics
        claims_metrics = (
            claims_df
            .withColumn(
                "total_claim_amount",
                F.sum("claim_amount").over(Window.partitionBy("claim_id"))
            )
            .withColumn(
                "total_approved_amount",
                F.sum("approved_amount").over(Window.partitionBy("claim_id"))
            )
            .withColumn(
                "rejection_rate",
                F.when(
                    F.col("total_claim_amount") > 0,
                    (F.col("total_claim_amount") - F.col("total_approved_amount")) / F.col("total_claim_amount")
                ).otherwise(0)
            )
            .withColumn(
                "processing_days",
                F.datediff(F.col("submission_date"), F.col("encounterstart_date"))
            )
        )

        # Calculate remittance metrics
        remittance_metrics = (
            remittance_df
            .withColumn(
                "total_paid_amount",
                F.sum("paid_amount").over(Window.partitionBy("claim_id"))
            )
            .withColumn(
                "payment_ratio",
                F.when(
                    F.col("claim_amount") > 0,
                    F.col("total_paid_amount") / F.col("claim_amount")
                ).otherwise(0)
            )
            .withColumn(
                "denial_count",
                F.count("denial_code").over(Window.partitionBy("claim_id"))
            )
        )

        return claims_metrics, remittance_metrics

    def create_final_dataset(
        self,
        claims_metrics: DataFrame,
        remittance_metrics: DataFrame,
        open_claims_df: DataFrame
    ) -> DataFrame:
        """Create final dataset for reporting."""
        
        # Join claims and remittance metrics
        final_claims = (
            claims_metrics
            .join(
                remittance_metrics.select(
                    "claim_id",
                    "total_paid_amount",
                    "payment_ratio",
                    "denial_count"
                ),
                "claim_id",
                "left"
            )
            .withColumn(
                "claim_status",
                F.when(F.col("total_paid_amount").isNull(), "Pending")
                .when(F.col("total_paid_amount") == 0, "Denied")
                .when(F.col("total_paid_amount") < F.col("total_claim_amount"), "Partially Paid")
                .otherwise("Fully Paid")
            )
        )

        # Union with open claims
        final_dataset = (
            final_claims
            .unionByName(
                open_claims_df.select(final_claims.columns),
                allowMissingColumns=True
            )
        )

        return final_dataset

    def transform_all_data(
        self,
        claims_df: DataFrame,
        remittance_df: DataFrame,
        open_claims_df: DataFrame,
        master_data: Dict[str, DataFrame],
        output_path: str
    ) -> None:
        """Transform all data and save final dataset."""
        
        # Join with master data
        enriched_claims, enriched_remittance, enriched_open_claims = self.join_master_data(
            claims_df, remittance_df, open_claims_df, master_data
        )

        # Calculate metrics
        claims_metrics, remittance_metrics = self.calculate_metrics(
            enriched_claims, enriched_remittance, enriched_open_claims
        )

        # Create final dataset
        final_dataset = self.create_final_dataset(
            claims_metrics, remittance_metrics, enriched_open_claims
        )

        # Save final dataset
        (
            final_dataset
            .write
            .partitionBy("enc_year", "enc_month")
            .mode("overwrite")
            .parquet(f"{output_path}/final_dataset.parquet")
        ) 