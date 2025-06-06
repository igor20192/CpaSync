import json
import pytest
import pandas as pd
from src.cpa_calculator import calculate_cpa, CpaCalculator
from src.data_reader import JsonDataReader


def test_calculate_cpa():
    assert calculate_cpa(10.0, 2) == 5.0
    assert calculate_cpa(10.0, 0) is None


def test_merge_data():
    reader = JsonDataReader()
    spend_data = [{"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.5}]
    conv_data = [{"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": 14}]
    with open("test_spend.json", "w") as f:
        json.dump(spend_data, f)
    with open("test_conv.json", "w") as f:
        json.dump(conv_data, f)
    df = reader.read("test_spend.json", "test_conv.json")
    assert len(df) == 1
    assert df.iloc[0]["spend"] == 37.5
    assert df.iloc[0]["conversions"] == 14


if __name__ == "__main__":
    pytest.main()
