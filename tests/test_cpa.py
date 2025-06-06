# tests/test_cpa.py
import pytest
import pandas as pd
from src.cpa_calculator import calculate_cpa, CpaCalculator
from src.data_reader import JsonDataReader
from src.db_repository import PostgresRepository, DailyStats
import json
import sqlalchemy as sa
from unittest.mock import Mock


@pytest.fixture
def sample_data(tmp_path):
    spend_data = [
        {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.5},
        {"date": "2025-06-04", "campaign_id": "CAMP-456", "spend": 0.0},
    ]
    conv_data = [
        {"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": 14},
        {"date": "2025-06-04", "campaign_id": "CAMP-456", "conversions": 3},
    ]
    spend_path = tmp_path / "spend.json"
    conv_path = tmp_path / "conv.json"
    spend_path.write_text(json.dumps(spend_data))
    conv_path.write_text(json.dumps(conv_data))
    return str(spend_path), str(conv_path)


def test_calculate_cpa():
    assert calculate_cpa(10.0, 2) == 5.0
    assert calculate_cpa(10.0, 0) is None
    assert calculate_cpa(0.0, 5) is None


def test_cpa_calculator():
    calculator = CpaCalculator()
    df = pd.DataFrame(
        [
            {
                "date": "2025-06-04",
                "campaign_id": "CAMP-123",
                "spend": 37.5,
                "conversions": 14,
            },
            {
                "date": "2025-06-04",
                "campaign_id": "CAMP-456",
                "spend": 0.0,
                "conversions": 3,
            },
            {
                "date": "2025-06-04",
                "campaign_id": "CAMP-789",
                "spend": 11.0,
                "conversions": 0,
            },
        ]
    )
    result = calculator.process(df)
    assert result.iloc[0]["cpa"] == pytest.approx(37.5 / 14)
    assert pd.isna(result.iloc[1]["cpa"])
    assert pd.isna(result.iloc[2]["cpa"])


def test_merge_data(sample_data):
    reader = JsonDataReader()
    spend_path, conv_path = sample_data
    df = reader.read(spend_path, conv_path)
    assert len(df) == 2
    assert df.iloc[0]["spend"] == 37.5
    assert df.iloc[0]["conversions"] == 14
    assert df.iloc[1]["spend"] == 0.0
    assert df.iloc[1]["conversions"] == 3


def test_merge_data_invalid_json(tmp_path):
    reader = JsonDataReader()
    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text('{"date": "2025-06-04"')  # Некорректный JSON
    valid_json = tmp_path / "valid.json"
    valid_json.write_text(
        '[{"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.5}]'
    )
    df = reader.read(str(valid_json), str(invalid_json))
    assert df.empty


def test_postgres_repository_upsert(mocker):
    engine = sa.create_engine("sqlite:///:memory:")
    repo = PostgresRepository(engine)
    df = pd.DataFrame(
        [
            {
                "date": "2025-06-04",
                "campaign_id": "CAMP-123",
                "spend": 37.5,
                "conversions": 14,
                "cpa": 2.6785714285714284,
            }
        ]
    )
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)

    mock_transaction = Mock()
    mock_transaction.__enter__ = Mock(return_value=mock_transaction)
    mock_transaction.__exit__ = Mock(return_value=None)
    mock_conn.begin = Mock(return_value=mock_transaction)
    mocker.patch.object(engine, "connect", return_value=mock_conn)
    repo.upsert(df)
    assert mock_conn.execute.called


def test_postgres_repository_check_date(mocker):
    engine = sa.create_engine("sqlite:///:memory:")
    repo = PostgresRepository(engine)
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.execute.return_value.fetchone.return_value = None
    mocker.patch.object(engine, "connect", return_value=mock_conn)
    assert repo.check_date_exists("2025-06-04") is False
