from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType
)
from pyspark.sql.window import Window
from config.spark_config import SPARK_CONFIGS, get_jdbc_properties


class SparkExtractor:
    """Spark-based extractor for efficient processing of large-scale claims data."""

    def __init__(self, db_credentials: Dict[str, str], host: str = "localhost", port: int = 5432, database: str = "nmc"):
        """Initialize Spark session with optimized configuration.
        
        Args:
            db_credentials: Dictionary containing 'user' and 'password'
            host: Database host
            port: Database port
            database: Database name
        """
        self.spark = (
            SparkSession.builder
            .appName("Claims ETL")
            # Disable event logging for local mode
            .config("spark.eventLog.enabled", "false")
            # Performance configurations
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "10g")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            # Local mode specific settings
            .config("spark.driver.host", "localhost")
            .master("local[*]")
            .getOrCreate()
        )
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.properties = get_jdbc_properties(db_credentials)
        
        # Register schemas
        self._register_schemas()
        
        # Cache master data
        self._cache_master_data()

    def _register_schemas(self):
        """Register schemas for all tables to ensure proper data types."""
        self.claims_schema = StructType([
            StructField("receiverid", StringType(), True),
            StructField("transactiondate", StringType(), True),
            StructField("recordcount", StringType(), True),
            StructField("dispositionflag", StringType(), True),
            StructField("claimid", StringType(), False),
            StructField("idpayer", StringType(), True),
            StructField("memberid", StringType(), True),
            StructField("payerid", StringType(), True),
            StructField("providerid", StringType(), True),
            StructField("emiratesidnumber", StringType(), True),
            StructField("gross", DoubleType(), True),
            StructField("patientshare", DoubleType(), True),
            StructField("netclaim", DoubleType(), True),
            StructField("encountertype", StringType(), True),
            StructField("patientid", StringType(), True),
            StructField("encounterstart", StringType(), True),
            StructField("encounterend", StringType(), True),
            StructField("encounterstarttype", StringType(), True),
            StructField("encounterendtype", StringType(), True),
            StructField("activityid", StringType(), True),
            StructField("activitystart", StringType(), True),
            StructField("activitytype", StringType(), True),
            StructField("activitycode", StringType(), True),
            StructField("activityquantity", DoubleType(), True),
            StructField("activitynet", DoubleType(), True),
            StructField("orderingclinician", StringType(), True),
            StructField("clinician", StringType(), True),
            StructField("visit_id", StringType(), True),
            StructField("patient_type", StringType(), True),
            StructField("org_id", StringType(), True),
            StructField("plan_id", StringType(), True),
            StructField("center_id", StringType(), True),
            StructField("charge_id", StringType(), True),
            StructField("is_primary", StringType(), True),
            StructField("submission_batch_id", StringType(), True),
            StructField("submission_date", TimestampType(), True),
            StructField("created_date", TimestampType(), True),
            StructField("is_resubmission", StringType(), True),
            StructField("insurance_category_id", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("discharge_date", TimestampType(), True),
            StructField("insurance_co_id", StringType(), True),
            StructField("tpa_id", StringType(), True),
            StructField("bill_open_date", TimestampType(), True),
            StructField("bill_finalized_date", TimestampType(), True),
            StructField("is_correction", StringType(), True),
            StructField("primary_icd", StringType(), True),
            StructField("bill_no", StringType(), True),
            StructField("charge_group", StringType(), True),
            StructField("charge_head", StringType(), True),
            StructField("act_description_id", StringType(), True),
            StructField("act_quantity", StringType(), True),
            StructField("service_sub_group_id", StringType(), True),
            StructField("act_description", StringType(), True),
            StructField("op_type", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("closure_type", StringType(), True),
            StructField("chargenet", StringType(), True),
            StructField("chargerecd", StringType(), True),
            StructField("charge_claim_status", StringType(), True),
            StructField("charge_closure_type", StringType(), True),
            StructField("payee_doctor_id", StringType(), True),
            StructField("prescribing_dr_id", StringType(), True),
            StructField("admitting_dr_id", StringType(), True),
            StructField("org_sub_seq", IntegerType(), True),
            StructField("cor_sub_seq", IntegerType(), True),
            StructField("is_claim_corrected", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_id", IntegerType(), True),
            StructField("modified_date", TimestampType(), True),
            StructField("claimstatus", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("audit_control_number", StringType(), True),
            StructField("denial_acceptance_remarks", StringType(), True),
            StructField("rejection_reason_category_id", IntegerType(), True),
            StructField("denial_acceptance_date", TimestampType(), True),
            StructField("denial_code", StringType(), True),
            StructField("denial_acceptance_amount", DoubleType(), True),
            StructField("prior_auth_id", StringType(), True),
            StructField("is_reconciliation", StringType(), True),
            StructField("encounter_start_date", TimestampType(), True),
            StructField("encounter_end_date", TimestampType(), True),
            StructField("update_flag", StringType(), True),
            StructField("denial_accepted_user", StringType(), True),
            StructField("senderid", StringType(), True),
            StructField("cleaned_activityid", StringType(), True)
        ])

        self.remittance_schema = StructType([
            StructField("remittance_id", IntegerType(), True),
            StructField("received_date", TimestampType(), True),
            StructField("claim_id", StringType(), False),
            StructField("denial_code", StringType(), True),
            StructField("xml_payment_reference", StringType(), True),
            StructField("received_amount", DoubleType(), True),
            StructField("clinician_id", StringType(), True),
            StructField("act_description", StringType(), True),
            StructField("act_quantity", DoubleType(), True),
            StructField("activity_id", StringType(), True),
            StructField("activitystart", TimestampType(), True),
            StructField("act_rate_plan_item_code", StringType(), True),
            StructField("code_type", StringType(), True),
            StructField("charge_id", StringType(), True),
            StructField("insurance_claim_amt", DoubleType(), True),
            StructField("rej_accepted_amt", DoubleType(), True),
            StructField("file_name", StringType(), True),
            StructField("reference_no", StringType(), True),
            StructField("org_id", IntegerType(), True),
            StructField("customer_number", StringType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("mod_time", TimestampType(), True),
            StructField("ra_seq", IntegerType(), True),
            StructField("ra_sub_seq", IntegerType(), True),
            StructField("bill_no", StringType(), True),
            StructField("ra_id", IntegerType(), True),
            StructField("modified_date", TimestampType(), True),
            StructField("health_authority", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("first_received", TimestampType(), True),
            StructField("second_received", TimestampType(), True),
            StructField("third_received", TimestampType(), True)
        ])

    def _cache_master_data(self):
        """Cache master data tables in memory for faster joins."""
        # Load and cache master data
        self.hospitals_df = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_hospital_center_master",
                properties=self.properties
            )
            .join(
                self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="nmc.m_cluster_master",
                    properties=self.properties
                ),
                "center_id",
                "left"
            )
            .cache()
        )

        self.doctors_df = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_doctors",
                properties=self.properties
            )
            .join(
                self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="nmc.m_department",
                    properties=self.properties
                ),
                "dept_id",
                "left"
            )
            .cache()
        )

        # Cache other master data similarly...

    def extract_claims(
        self,
        start_date: datetime,
        end_date: datetime,
        center_ids: Optional[List[str]] = None,
        claim_status: Optional[List[str]] = None,
    ) -> DataFrame:
        """Extract claims data using Spark."""
        # Read claims data in partitions
        claims_df = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.f_claim_submission",
                properties=self.properties,
                predicates=[
                    f"bill_finalized_date >= '{start_date}' AND bill_finalized_date < '{end_date}'",
                ],
                numPartitions=50  # Adjust based on data size
            )
        )

        if center_ids:
            claims_df = claims_df.filter(F.col("center_id").isin(center_ids))
        if claim_status:
            claims_df = claims_df.filter(F.col("claim_status").isin(claim_status))

        # Clean activity IDs
        claims_df = claims_df.withColumn(
            "cleaned_activityid",
            F.when(
                F.col("activityid").contains("-I"),
                F.expr("substring(activityid, 1, locate('-I', activityid) - 1)")
            ).when(
                F.col("activityid").contains("-R"),
                F.expr("substring(activityid, 1, locate('-R', activityid) - 1)")
            ).otherwise(F.col("activityid"))
        )

        return claims_df

    def extract_remittance(
        self,
        start_date: datetime,
        end_date: datetime,
        org_ids: Optional[List[int]] = None,
    ) -> DataFrame:
        """Extract remittance data using Spark."""
        # Read remittance data in partitions
        remittance_df = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.f_remittance_advice",
                properties=self.properties,
                predicates=[
                    f"transaction_date >= '{start_date}' AND transaction_date < '{end_date}'",
                ],
                numPartitions=50  # Adjust based on data size
            )
        )

        if org_ids:
            remittance_df = remittance_df.filter(F.col("org_id").isin(org_ids))

        # Calculate sequence-based dates
        window_spec = Window.partitionBy("claim_id", "activity_id")
        
        remittance_df = remittance_df.withColumn(
            "first_received",
            F.when(F.col("ra_seq") == 2, F.col("transaction_date")).otherwise(None)
        ).withColumn(
            "second_received",
            F.when(F.col("ra_seq") == 3, F.col("transaction_date")).otherwise(None)
        ).withColumn(
            "third_received",
            F.when(F.col("ra_seq") == 4, F.col("transaction_date")).otherwise(None)
        )

        return remittance_df

    def process_matched_data(
        self,
        claims_df: DataFrame,
        remittance_df: DataFrame,
    ) -> DataFrame:
        """Process matched claims and remittance data efficiently."""
        # Join claims and remittance
        matched_df = (
            claims_df
            .join(
                remittance_df,
                (claims_df.claimid == remittance_df.claim_id) &
                (claims_df.cleaned_activityid == remittance_df.activity_id),
                "left"
            )
            # Join with cached master data
            .join(self.hospitals_df, "center_id", "left")
            .join(
                self.doctors_df.alias("doc"),
                claims_df.clinician == F.col("doc.doctor_id"),
                "left"
            )
            .join(
                self.doctors_df.alias("ord_doc"),
                claims_df.orderingclinician == F.col("ord_doc.doctor_id"),
                "left"
            )
        )

        # Calculate claim status
        matched_df = matched_df.withColumn(
            "new_claim_status",
            F.when(F.col("claim_status") == "N", "Pending Reconciliation")
            .when(
                (F.col("claim_status") == "S") &
                (F.col("org_sub_seq") == 1) &
                (F.coalesce(F.col("ra_seq"), F.lit(0)) == 0),
                "Submitted"
            )
            .when(
                (F.col("claim_status") == "S") &
                (F.col("org_sub_seq") == 1) &
                (F.coalesce(F.col("ra_seq"), F.lit(0)) >= 1),
                "Denied"
            )
            .when(
                (F.col("claim_status") == "S") &
                (F.col("org_sub_seq") > 1),
                "Resubmitted"
            )
            .when(
                F.col("claim_status") == "C",
                "Closed"
            )
            .otherwise("Unknown")
        )

        return matched_df

    def calculate_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate metrics using Spark SQL functions."""
        return (
            df
            .withColumn(
                "reimbursement_rate",
                F.round(F.col("netclaim") / F.col("gross"), 4)
            )
            .withColumn(
                "patient_share_pct",
                F.round(F.col("patientshare") / F.col("gross"), 4)
            )
            .withColumn(
                "days_to_payment",
                F.round(
                    F.datediff(
                        F.col("transaction_date"),
                        F.col("bill_finalized_date")
                    )
                )
            )
            .withColumn(
                "is_delayed_payment",
                F.col("days_to_payment") > 30
            )
            .withColumn(
                "write_off_amount",
                F.round(F.col("netclaim") - F.col("received_amount"), 2)
            )
        )

    def aggregate_by_center(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by center using Spark SQL functions."""
        return (
            df.groupBy("center_id", "center_name")
            .agg(
                F.count("claimid").alias("total_claims"),
                F.sum("gross").alias("total_gross"),
                F.sum("netclaim").alias("total_net"),
                F.sum("received_amount").alias("total_received"),
                F.sum("write_off_amount").alias("total_write_off"),
                F.avg("days_to_payment").alias("avg_days_to_payment"),
                F.avg(F.when(F.col("is_delayed_payment"), 1).otherwise(0))
                .alias("delayed_payment_rate"),
                F.avg("reimbursement_rate").alias("avg_reimbursement_rate")
            )
        )

    def close(self):
        """Close Spark session."""
        if self.spark:
            self.spark.stop() 