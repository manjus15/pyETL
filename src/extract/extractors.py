# src/extract/extractors.py
import logging
from typing import Dict

import pandas as pd
from config.config import Config
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DataExtractor:
    def __init__(self):
        self.db = DatabaseConnection()
        self.schema = Config.DB_CONFIG.schema

    def extract_fact_table(self, table_name: str) -> pd.DataFrame:
        try:
            table_config = Config.FACT_TABLES.get(table_name)
            if not table_config:
                raise ValueError(f"Configuration not found for table: {table_name}")

            columns = ", ".join(table_config["columns_to_keep"])
            query = f"""
            SELECT {columns}
            FROM {self.schema}.{table_name}
            """

            data = self.db.execute_query(query)
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error extracting fact table {table_name}: {str(e)}")
            raise

    def extract_master_table(self, table_name: str) -> pd.DataFrame:
        try:
            table_config = Config.MASTER_TABLES.get(table_name)
            if not table_config:
                raise ValueError(f"Configuration not found for table: {table_name}")

            columns = ", ".join(table_config["columns_to_keep"])
            query = f"""
            SELECT {columns}
            FROM {self.schema}.{table_name}
            """

            data = self.db.execute_query(query)
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error extracting master table {table_name}: {str(e)}")
            raise
