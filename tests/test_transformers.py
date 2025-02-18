# test_transformers.py
import pandas as pd
import pytest
from src.transform.transformers import DataTransformer


def test_transform_fact_tables(sample_fact_data):
    """Test fact table transformations"""
    transformer = DataTransformer()
    fact_dfs = {"f_claim_submission": sample_fact_data}
    transformed = transformer.transform_fact_tables(fact_dfs)

    df = transformed["f_claim_submission"]
    assert pd.api.types.is_datetime64_dtype(df["bill_finalized_date"])
    assert pd.api.types.is_numeric_dtype(df["gross"])
    assert pd.api.types.is_numeric_dtype(df["netclaim"])


def test_transform_master_tables(sample_master_data):
    """Test master table transformations"""
    transformer = DataTransformer()
    master_dfs = {"m_hospital_center_master": sample_master_data}
    transformed = transformer.transform_master_tables(master_dfs)

    df = transformed["m_hospital_center_master"]
    assert not df["center_name"].isnull().any()
    assert not df["center_code"].isnull().any()
