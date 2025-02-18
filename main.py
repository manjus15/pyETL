import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from config.config import Config
from src.extract.extractors import DataExtractor
from src.load.loaders import DataLoader
from src.transform.transformers import DataTransformer
from utils.helpers import (DataFrameOptimizer, check_system_resources,
                           generate_data_profile, setup_logging,
                           timer_decorator)

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.optimizer = DataFrameOptimizer()

        # Create output directory
        Path(Config.OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    @timer_decorator
    def extract_data(self) -> Dict[str, pd.DataFrame]:
        """
        Extract data from all configured tables
        """
        logger.info("Starting data extraction...")

        # Extract fact tables
        fact_dfs = {}
        for table_name in Config.FACT_TABLES:
            logger.info(f"Extracting fact table: {table_name}")
            fact_dfs[table_name] = self.extractor.extract_fact_table(table_name)

        # Extract master tables
        master_dfs = {}
        for table_name in Config.MASTER_TABLES:
            logger.info(f"Extracting master table: {table_name}")
            master_dfs[table_name] = self.extractor.extract_master_table(table_name)

        return {"fact_tables": fact_dfs, "master_tables": master_dfs}

    @timer_decorator
    def transform_data(
        self, raw_data: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Dict[str, pd.DataFrame]:
        """
        Transform extracted data
        """
        logger.info("Starting data transformation...")

        # Transform fact tables
        transformed_facts = self.transformer.transform_fact_tables(
            raw_data["fact_tables"]
        )

        # Transform master tables
        transformed_masters = self.transformer.transform_master_tables(
            raw_data["master_tables"]
        )

        # Join fact and master tables
        joined_data = self._join_tables(transformed_facts, transformed_masters)

        # Optimize memory usage
        optimized_data = {}
        for table_name, df in joined_data.items():
            optimized_data[table_name] = self.optimizer.optimize_dataframe(df)

        return optimized_data

    def _join_tables(
        self,
        fact_tables: Dict[str, pd.DataFrame],
        master_tables: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        """
        Join fact and master tables based on configuration
        """
        logger.info("Joining fact and master tables...")
        joined_data = {}

        for fact_name, fact_df in fact_tables.items():
            # Get join configurations for this fact table
            join_config = Config.JOIN_MAPPINGS.get(fact_name, {})

            if not join_config:
                joined_data[fact_name] = fact_df
                continue

            result_df = fact_df.copy()

            # Perform joins with master tables
            for master_table, join_keys in join_config.items():
                if master_table in master_tables:
                    result_df = pd.merge(
                        result_df,
                        master_tables[master_table],
                        how="left",
                        on=join_keys,
                        suffixes=("", f"_{master_table}"),
                    )

            joined_data[fact_name] = result_df

        return joined_data

    @timer_decorator
    def load_data(self, transformed_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load transformed data to target destination
        """
        logger.info("Starting data load...")

        for table_name, df in transformed_data.items():
            target_path = (
                Path(Config.OUTPUT_PATH)
                / f"{table_name}_{datetime.now().strftime('%Y%m%d')}.parquet"
            )

            # Generate data profile before loading
            if Config.GENERATE_PROFILES:
                profile_path = (
                    Path(Config.OUTPUT_PATH)
                    / f"{table_name}_profile_{datetime.now().strftime('%Y%m%d')}.html"
                )
                generate_data_profile(df, profile_path)

            # Load data
            logger.info(f"Loading table {table_name} to {target_path}")
            self.loader.load_table(df, target_path)

    def validate_data(
        self, transformed_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, Dict[str, bool]]:
        """
        Perform data quality checks
        """
        logger.info("Performing data validation...")
        validation_results = {}

        for table_name, df in transformed_data.items():
            table_results = {
                "has_data": len(df) > 0,
                "no_nulls": not df.isnull().any().any(),
                "expected_columns": all(
                    col in df.columns
                    for col in Config.REQUIRED_COLUMNS.get(table_name, [])
                ),
                "unique_keys": all(
                    df.groupby(Config.PRIMARY_KEYS.get(table_name, [])).size().max()
                    == 1
                ),
            }
            validation_results[table_name] = table_results

            # Log validation failures
            failed_checks = {k: v for k, v in table_results.items() if not v}
            if failed_checks:
                logger.warning(
                    f"Validation failed for table {table_name}: {failed_checks}"
                )

        return validation_results

    def run_pipeline(self) -> None:
        """
        Execute the complete ETL pipeline
        """
        try:
            # Check system resources
            check_system_resources()

            # Extract
            raw_data = self.extract_data()

            # Transform
            transformed_data = self.transform_data(raw_data)

            # Validate
            validation_results = self.validate_data(transformed_data)

            # Only proceed with load if validation passes
            if all(all(checks.values()) for checks in validation_results.values()):
                # Load
                self.load_data(transformed_data)
                logger.info("ETL pipeline completed successfully")
            else:
                logger.error("ETL pipeline failed validation checks")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    # Setup logging
    setup_logging()

    # Run pipeline
    pipeline = ETLPipeline()
    pipeline.run_pipeline()
