# test_extractors.py
import pytest
from src.extract.extractors import DataExtractor

from pyETL.config.config import Config


def test_extract_fact_table():
    """Test fact table extraction"""
    extractor = DataExtractor()
    df = extractor.extract_fact_table("f_claim_submission")

    assert not df.empty
    for col in Config.FACT_TABLES["f_claim_submission"]["columns_to_keep"]:
        assert col in df.columns


def test_extract_master_table():
    """Test master table extraction"""
    extractor = DataExtractor()
    df = extractor.extract_master_table("m_hospital_center_master")

    assert not df.empty
    for col in Config.MASTER_TABLES["m_hospital_center_master"]["columns_to_keep"]:
        assert col in df.columns
