# conftest.py
import pandas as pd
import pytest
from config.config import Config
from src.database.connection import DatabaseConnection


@pytest.fixture
def sample_fact_data():
    return pd.DataFrame(
        {
            "claimid": ["C1", "C2", "C3"],
            "memberid": ["M1", "M2", "M3"],
            "gross": [1000.0, 2000.0, 3000.0],
            "patientshare": [100.0, 200.0, 300.0],
            "netclaim": [900.0, 1800.0, 2700.0],
            "encountertype": ["IP", "OP", "IP"],
            "bill_finalized_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "claim_status": ["Approved", "Pending", "Rejected"],
            "center_id": [1, 2, 3],
        }
    )


@pytest.fixture
def sample_master_data():
    return pd.DataFrame(
        {
            "center_id": [1, 2, 3],
            "center_name": ["Center A", "Center B", "Center C"],
            "center_code": ["CA", "CB", "CC"],
            "city_id": ["CTY1", "CTY2", "CTY3"],
            "state_id": ["ST1", "ST2", "ST3"],
            "country_id": ["CN1", "CN2", "CN3"],
        }
    )
