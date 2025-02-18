# config/config.py
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict

from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str


class Config:
    # Environment configuration
    ENV = os.getenv("ENV", "dev")  # 'dev', 'test', or 'prod'

    DB_CONFIG = DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", ""),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        schema=os.getenv("DB_SCHEMA", "public"),
    )

    # Test data filters
    TEST_FILTERS = {
        "f_claim_submission": {
            "date_range": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
            "limit": 1000,
            # "center_ids": [1, 2, 3],  # Specific centers for testing
            # "claim_status": ["Approved", "Pending"],  # Specific statuses
            "sample_claims": ["C1323000246176"],  # Specific claim IDs
        },
        "f_remittance_advice": {
            "date_range": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
            "limit": 1000,
            # "org_ids": [1, 2, 3]  # Specific organizations
            "sample_claims": ["C1323000246176"],  # Specific claim IDs
        },
    }

    FACT_TABLES = {
        "f_claim_submission": {
            "date_column": "bill_finalized_date",
            "numeric_columns": ["gross", "patientshare", "netclaim", "activitynet"],
            "columns_to_keep": [
                "claimid",
                "memberid",
                "gross",
                "patientshare",
                "netclaim",
                "encountertype",
                "activitynet",
                "bill_finalized_date",
                "claim_status",
                "center_id",
            ],
            "filters": {
                "dev": {
                    "where_clause": """
                        bill_finalized_date >= %(start_date)s 
                        AND bill_finalized_date <= %(end_date)s
                        AND center_id = ANY(%(center_ids)s)
                        AND claim_status = ANY(%(claim_status)s)
                        LIMIT %(limit)s
                    """,
                    "params": {
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "center_ids": [1, 2, 3],
                        "claim_status": ["Approved", "Pending"],
                        "limit": 1000,
                    },
                },
                "test": {
                    "where_clause": "claimid = ANY(%(claim_ids)s)",
                    "params": {"claim_ids": ["C1323000246176"]},
                },
                "prod": {"where_clause": "TRUE"},
            },
        },
        "f_remittance_advice": {
            "date_column": "transaction_date",
            "numeric_columns": ["received_amount", "insurance_claim_amt"],
            "columns_to_keep": [
                "claim_id",
                "denial_code",
                "received_amount",
                "activity_id",
                "transaction_date",
                "insurance_claim_amt",
                "org_id",
            ],
            "filters": {
                "dev": {
                    "where_clause": """
                        transaction_date >= %(start_date)s 
                        AND transaction_date <= %(end_date)s
                        AND org_id = ANY(%(org_ids)s)
                        LIMIT %(limit)s
                    """,
                    "params": {
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "org_ids": [1, 2, 3],
                        "limit": 1000,
                    },
                },
                "test": {
                    "where_clause": "org_id = ANY(%(org_ids)s) LIMIT 100",
                    "params": {"org_ids": [1]},
                },
                "prod": {"where_clause": "TRUE"},
            },
        },
    }

    MASTER_TABLES = {
        "m_hospital_center_master": {
            "key_column": "center_id",
            "columns_to_keep": [
                "center_id",
                "center_name",
                "center_code",
                "city_id",
                "state_id",
                "country_id",
            ],
        },
        "m_cluster_master": {
            "key_column": "cluster_id",
            "columns_to_keep": ["cluster_id", "cluster_name", "center_id"],
        },
    }

    BATCH_SIZE = 50000
    MAX_WORKERS = 4
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    OUTPUT_PATH = "output"
    TEMP_PATH = "temp"
