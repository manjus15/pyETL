# test_loaders.py
import os

import pandas as pd
import pytest
from src.load.loaders import DataLoader


def test_save_to_csv(sample_fact_data):
    """Test CSV file saving"""
    loader = DataLoader()
    file_path = loader.save_to_csv(sample_fact_data, "test_output")

    assert os.path.exists(file_path)
    loaded_df = pd.read_csv(file_path)
    assert not loaded_df.empty
    os.remove(file_path)
