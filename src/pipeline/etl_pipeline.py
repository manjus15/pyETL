from typing import Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession

from src.extract.master_data_extractor import MasterDataExtractor
from src.extract.fact_data_extractor import FactDataExtractor
from src.transform.fact_transformer import FactTransformer
from src.config.spark_config import SPARK_CONFIGS

class ETLPipeline:
    """Main ETL pipeline for processing claims data."""

    def __init__(
        self,
        host: str,
        port: str,
        database: str,
        db_credentials: Dict[str, str],
        output_path: str
    ):
        self.host = host
        self.port = port
        self.database = database
        self.db_credentials = db_credentials
        self.output_path = output_path
        self.spark = self._create_spark_session()
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        return (
            SparkSession.builder
            .appName("Claims ETL Pipeline")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate()
        )

    def run_pipeline(self) -> None:
        """Execute the complete ETL pipeline."""
        try:
            # Extract master data
            master_extractor = MasterDataExtractor(
                self.spark,
                self.jdbc_url,
                self.db_credentials
            )
            master_data = master_extractor.extract_all_master_data(
                f"{self.output_path}/master_data"
            )

            # Extract fact data
            fact_extractor = FactDataExtractor(
                self.spark,
                self.jdbc_url,
                self.db_credentials
            )
            fact_extractor.extract_all_fact_data(
                f"{self.output_path}/fact_data"
            )

            # Load extracted data
            claims_df = self.spark.read.parquet(f"{self.output_path}/fact_data/claims.parquet")
            remittance_df = self.spark.read.parquet(f"{self.output_path}/fact_data/remittance.parquet")
            open_claims_df = self.spark.read.parquet(f"{self.output_path}/fact_data/open_claims.parquet")

            # Transform data
            transformer = FactTransformer(self.spark)
            transformer.transform_all_data(
                claims_df,
                remittance_df,
                open_claims_df,
                master_data,
                f"{self.output_path}/transformed_data"
            )

            print("ETL pipeline completed successfully!")

        except Exception as e:
            print(f"Error in ETL pipeline: {str(e)}")
            raise

        finally:
            self.spark.stop()

def main():
    """Main entry point for the ETL pipeline."""
    # Configuration
    config = {
        "host": "localhost",
        "port": "5432",
        "database": "claims_db",
        "db_credentials": {
            "user": "your_username",
            "password": "your_password"
        },
        "output_path": "/path/to/output"
    }

    # Run pipeline
    pipeline = ETLPipeline(**config)
    pipeline.run_pipeline()

if __name__ == "__main__":
    main() 