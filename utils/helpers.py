# utils/helpers.py
import json
import logging
import os
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import psutil
from config.config import Config


def setup_logging(log_file: str = None):
    """
    Set up logging configuration
    """
    logging_config = {
        "level": getattr(logging, Config.LOG_LEVEL),
        "format": Config.LOG_FORMAT,
        "handlers": [logging.StreamHandler()],
    }

    if log_file:
        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_path / log_file)
        file_handler.setFormatter(logging.Formatter(Config.LOG_FORMAT))
        logging_config["handlers"].append(file_handler)

    logging.basicConfig(**logging_config)


def timer_decorator(func):
    """
    Decorator to measure function execution time
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        logger = logging.getLogger(__name__)
        logger.info(
            f"Function {func.__name__} took {end_time - start_time:.2f} seconds to execute"
        )

        return result

    return wrapper


class DataFrameValidator:
    """
    Utility class for DataFrame validation
    """

    @staticmethod
    def validate_schema(
        df: pd.DataFrame,
        expected_columns: List[str],
        nullable_columns: List[str] = None,
    ) -> bool:
        """
        Validate DataFrame schema
        """
        nullable_columns = nullable_columns or []

        # Check for required columns
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            return False

        # Check for null values in non-nullable columns
        non_nullable = set(expected_columns) - set(nullable_columns)
        for col in non_nullable:
            if df[col].isnull().any():
                logging.error(f"Null values found in non-nullable column: {col}")
                return False

        return True

    @staticmethod
    def validate_data_types(df: pd.DataFrame, type_mapping: Dict[str, str]) -> bool:
        """
        Validate DataFrame column data types
        """
        for column, expected_type in type_mapping.items():
            if column not in df.columns:
                logging.error(f"Column {column} not found in DataFrame")
                return False

            actual_type = df[column].dtype
            if not pd.api.types.is_dtype_equal(actual_type, expected_type):
                logging.error(
                    f"Column {column} has incorrect type. Expected {expected_type}, got {actual_type}"
                )
                return False

        return True


class DataFrameTransformations:
    """
    Utility class for common DataFrame transformations
    """

    @staticmethod
    def convert_dates(
        df: pd.DataFrame, date_columns: List[str], date_format: str = None
    ) -> pd.DataFrame:
        """
        Convert date columns to datetime
        """
        df_copy = df.copy()
        for col in date_columns:
            try:
                if date_format:
                    df_copy[col] = pd.to_datetime(df_copy[col], format=date_format)
                else:
                    df_copy[col] = pd.to_datetime(df_copy[col])
            except Exception as e:
                logging.error(f"Error converting column {col} to datetime: {str(e)}")
                raise

        return df_copy

    @staticmethod
    def handle_outliers(
        df: pd.DataFrame,
        numeric_columns: List[str],
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> pd.DataFrame:
        """
        Handle outliers in numeric columns
        """
        df_copy = df.copy()

        for col in numeric_columns:
            if method == "iqr":
                Q1 = df_copy[col].quantile(0.25)
                Q3 = df_copy[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR

                df_copy[col] = df_copy[col].clip(lower_bound, upper_bound)
            elif method == "zscore":
                z_scores = np.abs(
                    (df_copy[col] - df_copy[col].mean()) / df_copy[col].std()
                )
                df_copy[col] = df_copy[col].mask(
                    z_scores > threshold, df_copy[col].mean()
                )

        return df_copy

    @staticmethod
    def aggregate_data(
        df: pd.DataFrame,
        group_by_columns: List[str],
        agg_functions: Dict[str, Union[str, List[str]]],
    ) -> pd.DataFrame:
        """
        Aggregate data with specified functions
        """
        try:
            return df.groupby(group_by_columns).agg(agg_functions).reset_index()
        except Exception as e:
            logging.error(f"Error aggregating data: {str(e)}")
            raise


class DataFrameOptimizer:
    """
    Utility class for optimizing DataFrame memory usage
    """

    @staticmethod
    def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage by choosing appropriate data types
        """
        df_optimized = df.copy()

        # Optimize integer columns
        int_columns = df_optimized.select_dtypes(include=["int64"]).columns
        for col in int_columns:
            col_min = df_optimized[col].min()
            col_max = df_optimized[col].max()

            if col_min >= 0:
                if col_max < 255:
                    df_optimized[col] = df_optimized[col].astype(np.uint8)
                elif col_max < 65535:
                    df_optimized[col] = df_optimized[col].astype(np.uint16)
                elif col_max < 4294967295:
                    df_optimized[col] = df_optimized[col].astype(np.uint32)
            else:
                if col_min > -128 and col_max < 127:
                    df_optimized[col] = df_optimized[col].astype(np.int8)
                elif col_min > -32768 and col_max < 32767:
                    df_optimized[col] = df_optimized[col].astype(np.int16)
                elif col_min > -2147483648 and col_max < 2147483647:
                    df_optimized[col] = df_optimized[col].astype(np.int32)

        # Optimize float columns
        float_columns = df_optimized.select_dtypes(include=["float64"]).columns
        for col in float_columns:
            df_optimized[col] = df_optimized[col].astype(np.float32)

        # Optimize object (string) columns
        object_columns = df_optimized.select_dtypes(include=["object"]).columns
        for col in object_columns:
            num_unique = df_optimized[col].nunique()
            num_total = len(df_optimized)

            if num_unique / num_total < 0.5:  # If less than 50% unique values
                df_optimized[col] = df_optimized[col].astype("category")

        return df_optimized

    @staticmethod
    def monitor_memory_usage():
        """
        Monitor current memory usage
        """
        process = psutil.Process(os.getpid())
        memory_info = {
            "memory_usage_mb": process.memory_info().rss / 1024 / 1024,
            "memory_percent": process.memory_percent(),
            "cpu_percent": process.cpu_percent(),
        }
        return memory_info


def generate_data_profile(
    df: pd.DataFrame, output_path: Union[str, Path] = None
) -> Dict[str, Any]:
    """
    Generate a comprehensive data profile report
    """
    profile = {
        "basic_info": {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2,
            "duplicates": df.duplicated().sum(),
            "timestamp": datetime.now().isoformat(),
        },
        "column_info": {},
        "correlations": None,
        "memory_optimization_potential": {},
    }

    # Column-level analysis
    for col in df.columns:
        col_info = {
            "dtype": str(df[col].dtype),
            "null_count": df[col].isnull().sum(),
            "null_percentage": (df[col].isnull().sum() / len(df)) * 100,
            "unique_values": df[col].nunique(),
            "unique_percentage": (df[col].nunique() / len(df)) * 100,
        }

        if pd.api.types.is_numeric_dtype(df[col]):
            col_info.update(
                {
                    "mean": df[col].mean(),
                    "std": df[col].std(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "median": df[col].median(),
                    "skewness": df[col].skew(),
                    "kurtosis": df[col].kurtosis(),
                }
            )
        elif pd.api.types.is_string_dtype(df[col]):
            col_info.update(
                {
                    "most_common": df[col].value_counts().head(5).to_dict(),
                    "avg_length": (
                        df[col].str.len().mean() if df[col].dtype == object else None
                    ),
                }
            )

        profile["column_info"][col] = col_info

    # Calculate correlations for numeric columns
    numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
    if len(numeric_columns) > 1:
        profile["correlations"] = df[numeric_columns].corr().to_dict()

    # Memory optimization suggestions
    for col in df.columns:
        current_size = df[col].memory_usage(deep=True) / 1024**2
        optimized_size = None
        suggestion = None

        if pd.api.types.is_integer_dtype(df[col]):
            col_min = df[col].min()
            col_max = df[col].max()
            suggestion = f"Consider using smaller integer type. Current range: {col_min} to {col_max}"
            # Calculate optimized size based on value range
            optimized_size = (
                len(df)
                * (1 if col_max < 255 else 2 if col_max < 65535 else 4)
                / 1024**2
            )
        elif pd.api.types.is_float_dtype(df[col]):
            suggestion = "Consider using float32 instead of float64"
            optimized_size = current_size / 2
        elif (
            pd.api.types.is_string_dtype(df[col]) and df[col].nunique() / len(df) < 0.5
        ):
            suggestion = "Consider using category type for low-cardinality strings"
            optimized_size = (
                df[col].astype("category").memory_usage(deep=True) / 1024**2
            )

        if optimized_size is not None:
            profile["memory_optimization_potential"][col] = {
                "current_size_mb": current_size,
                "potential_size_mb": optimized_size,
                "savings_percentage": ((current_size - optimized_size) / current_size)
                * 100,
                "suggestion": suggestion,
            }

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(profile, f, indent=2)

    return profile


def check_system_resources():
    """
    Check system resources availability
    """
    return {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "available_memory_gb": psutil.virtual_memory().available / 1024**3,
    }


# Error handling decorator
def handle_errors(func):
    """
    Decorator for consistent error handling
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise

    return wrapper
