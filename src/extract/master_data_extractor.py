from typing import Dict, Optional
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

class MasterDataExtractor:
    """Extracts and processes master data tables."""

    def __init__(self, spark: SparkSession, jdbc_url: str, properties: Dict[str, str]):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.properties = properties

    def extract_hospitals(self) -> DataFrame:
        """Extract hospital and cluster data."""
        hospitals = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_hospital_center_master",
                properties=self.properties
            )
            .select(
                F.trim(F.col("center_id")).alias("center_id"),
                "center_name",
                "hospital_center_service_reg_no",
                "health_authority",
                F.upper(F.col("center_name")).alias("CENTER_NAME_KEY")
            )
            .filter(F.col("center_id") != "0")
        )

        clusters = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_cluster_master",
                properties=self.properties
            )
            .select(
                "cluster_id",
                "cluster_name",
                F.trim(F.col("center_id")).alias("center_id")
            )
        )

        return hospitals.join(clusters, "center_id", "left")

    def extract_insurance(self) -> DataFrame:
        """Extract insurance company data."""
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table="nmc.m_insurance_company_master",
            properties=self.properties
        )

    def extract_plans(self) -> DataFrame:
        """Extract insurance plans and network types."""
        plans = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_insurance_plan_main",
                properties=self.properties
            )
            .select(
                "plan_id",
                "category_id",
                "plan_name",
                "rateplan_rule_id"
            )
        )

        network_types = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_insurance_category_master",
                properties=self.properties
            )
            .select("category_id", "category_name")
        )

        return plans.join(network_types, "category_id", "left")

    def extract_organizations(self) -> DataFrame:
        """Extract organization details."""
        return (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_organization_details",
                properties=self.properties
            )
            .select("org_id", "org_name")
        )

    def extract_encounter_types(self) -> DataFrame:
        """Extract encounter type codes."""
        return (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_encounter_type_codes",
                properties=self.properties
            )
            .select(
                F.col("id").alias("encountertype"),
                "encounter_type_desc"
            )
        )

    def extract_tpa(self) -> DataFrame:
        """Extract TPA master data."""
        return (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_tpa_master",
                properties=self.properties
            )
            .withColumn(
                "claim_format_desc",
                F.when(F.col("claim_format") == "XML", "TPA")
                .otherwise("CORPORATE")
            )
        )

    def extract_denial_codes(self) -> DataFrame:
        """Extract denial codes."""
        return (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_insurance_denial_codes",
                properties=self.properties
            )
            .select(
                F.col("denial_code").alias("latest_denial_code"),
                F.col("code_description").alias("denial_description"),
                F.col("responsibility").alias("denial_responsibility")
            )
        )

    def extract_doctors(self) -> DataFrame:
        """Extract doctor information."""
        doctors = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_doctors",
                properties=self.properties
            )
            .join(
                self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="nmc.m_ha_doc_license_details",
                    properties=self.properties
                ),
                "doctor_id",
                "inner"
            )
            .select(
                "doctor_id",
                "doctor_name",
                "specialization",
                "dept_id",
                "doctor_license_number"
            )
        )

        departments = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="nmc.m_department",
                properties=self.properties
            )
            .select("dept_id", "dept_name")
        )

        return doctors.join(departments, "dept_id", "left")

    def extract_ordering_doctors(self) -> DataFrame:
        """Extract ordering doctor information with complex joins."""
        return self.spark.sql("""
            SELECT 
                doctor_license_number,
                doctor_name,
                max(dept_name) as dept_name
            FROM (
                SELECT 
                    hdoc.doctor_license_number,
                    max(doc.doctor_name) as doctor_name,
                    dpt.dept_name
                FROM nmc.m_doctors doc
                INNER JOIN nmc.m_ha_doc_license_details hdoc 
                    ON doc.doctor_id = hdoc.doctor_id
                INNER JOIN nmc.m_department dpt 
                    ON doc.dept_id = dpt.dept_id
                GROUP BY hdoc.doctor_license_number, dpt.dept_name
                
                UNION
                
                SELECT 
                    clinician_id as doctor_license_number,
                    max(name) as doctor_name,
                    '' as dept_name
                FROM nmc.m_referral
                WHERE clinician_id NOT IN (
                    SELECT doctor_license_number 
                    FROM nmc.m_ha_doc_license_details 
                    WHERE doctor_license_number is not null
                )
                GROUP BY clinician_id
                
                UNION
                
                SELECT 
                    doc.doctor_id,
                    doc.doctor_name,
                    dpt.dept_name
                FROM nmc.m_doctors doc
                INNER JOIN nmc.m_ha_doc_license_details hdoc 
                    ON doc.doctor_id = hdoc.doctor_id
                INNER JOIN nmc.m_department dpt 
                    ON doc.dept_id = dpt.dept_id
            ) doc
            GROUP BY doctor_license_number, doctor_name
        """)

    def extract_all_master_data(self, output_path: str) -> None:
        """Extract all master data and save to parquet files."""
        # Extract and save each master dataset
        self.extract_hospitals().write.mode("overwrite").parquet(f"{output_path}/hospitals.parquet")
        self.extract_insurance().write.mode("overwrite").parquet(f"{output_path}/insurance.parquet")
        self.extract_plans().write.mode("overwrite").parquet(f"{output_path}/plans.parquet")
        self.extract_organizations().write.mode("overwrite").parquet(f"{output_path}/organizations.parquet")
        self.extract_encounter_types().write.mode("overwrite").parquet(f"{output_path}/encounter_types.parquet")
        self.extract_tpa().write.mode("overwrite").parquet(f"{output_path}/tpa.parquet")
        self.extract_denial_codes().write.mode("overwrite").parquet(f"{output_path}/denial_codes.parquet")
        self.extract_doctors().write.mode("overwrite").parquet(f"{output_path}/doctors.parquet")
        self.extract_ordering_doctors().write.mode("overwrite").parquet(f"{output_path}/ordering_doctors.parquet") 