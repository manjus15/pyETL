# src/transform/transformers.py
import logging
from typing import Dict

import numpy as np
import pandas as pd
from config.config import Config

logger = logging.getLogger(__name__)


class DataTransformer:
    def transform_fact_tables(
        self, fact_dfs: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        transformed_dfs = {}

        for table_name, df in fact_dfs.items():
            logger.info(f"Transforming fact table: {table_name}")

            # Convert date columns
            date_col = Config.FACT_TABLES[table_name]["date_column"]
            df[date_col] = pd.to_datetime(df[date_col])

            # Handle numeric columns
            numeric_cols = Config.FACT_TABLES[table_name]["numeric_columns"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            transformed_dfs[table_name] = df

        return transformed_dfs

    def transform_master_tables(
        self, master_dfs: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        transformed_dfs = {}

        for table_name, df in master_dfs.items():
            logger.info(f"Transforming master table: {table_name}")

            # Handle missing values
            df = df.fillna(
                {
                    "center_name": "Unknown",
                    "center_code": "Unknown",
                    "cluster_name": "Unknown",
                }
            )

            transformed_dfs[table_name] = df

        return transformed_dfs
