# test_integration.py
import os

import pytest
from main import main

from pyETL.config.config import Config


def test_end_to_end():
    """Test complete ETL pipeline"""
    try:
        main()
        # Check if output files exist
        output_path = Config.OUTPUT_PATH
        assert os.path.exists(output_path)
        for table in Config.FACT_TABLES:
            assert os.path.exists(os.path.join(output_path, f"transformed_{table}.csv"))
        for table in Config.MASTER_TABLES:
            assert os.path.exists(os.path.join(output_path, f"transformed_{table}.csv"))
    except Exception as e:
        pytest.fail(f"End-to-end test failed: {str(e)}")
