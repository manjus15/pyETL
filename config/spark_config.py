"""Spark configuration settings."""

from typing import Dict

# Spark session configurations
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "10g",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "50000",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
    "spark.default.parallelism": "100",
    "spark.sql.broadcastTimeout": "600",
}

# JDBC connection properties
JDBC_PROPS = {
    "driver": "org.postgresql.Driver",
    "fetchsize": "100000",
    "batchsize": "50000",
}

# Partition configurations
PARTITION_CONFIG = {
    "claims": {
        "num_partitions": 50,
        "partition_column": "bill_finalized_date",
    },
    "remittance": {
        "num_partitions": 50,
        "partition_column": "transaction_date",
    },
}

# Cache configurations
CACHE_CONFIG = {
    "master_tables": [
        "hospitals",
        "doctors",
        "plans",
        "encounter_types",
        "denial_codes",
    ],
    "storage_level": "MEMORY_AND_DISK",
}

def get_jdbc_properties(credentials: Dict[str, str]) -> Dict[str, str]:
    """Get JDBC properties with credentials.
    
    Args:
        credentials: Database credentials
        
    Returns:
        Dict containing all JDBC properties
    """
    return {
        **JDBC_PROPS,
        "user": credentials["user"],
        "password": credentials["password"],
    }

def get_partition_predicates(
    table: str,
    start_date: str,
    end_date: str,
    num_partitions: int = None
) -> list:
    """Generate partition predicates for parallel reading.
    
    Args:
        table: Table name
        start_date: Start date
        end_date: End date
        num_partitions: Number of partitions (optional)
        
    Returns:
        List of predicates for parallel reading
    """
    if num_partitions is None:
        num_partitions = PARTITION_CONFIG[table]["num_partitions"]
        
    partition_column = PARTITION_CONFIG[table]["partition_column"]
    
    return [
        f"{partition_column} >= '{start_date}' AND {partition_column} < '{end_date}'"
        for start_date, end_date in _generate_date_ranges(start_date, end_date, num_partitions)
    ]

def _generate_date_ranges(start_date: str, end_date: str, num_partitions: int) -> list:
    """Generate date ranges for partitioning.
    
    Args:
        start_date: Start date
        end_date: End date
        num_partitions: Number of partitions
        
    Returns:
        List of (start_date, end_date) tuples
    """
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = (end - start) / num_partitions
    
    ranges = []
    for i in range(num_partitions):
        partition_start = start + delta * i
        partition_end = start + delta * (i + 1)
        ranges.append((
            partition_start.strftime("%Y-%m-%d"),
            partition_end.strftime("%Y-%m-%d")
        ))
    
    return ranges 