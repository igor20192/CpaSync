import unittest
import pandas as pd
from src.cpa_calculator import CpaCalculator
from src.data_reader import JsonDataReader
import json
import os
import tempfile


class TestCpaSync(unittest.TestCase):
    def setUp(self):
        # Create temporary JSON files
        self.temp_dir = tempfile.mkdtemp()
        self.spend_path = os.path.join(self.temp_dir, "spend.json")
        self.conv_path = os.path.join(self.temp_dir, "conv.json")

        spend_data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.5},
            {"date": "2025-06-04", "campaign_id": "CAMP-456", "spend": 19.9},
        ]
        conv_data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": 14},
            {"date": "2025-06-04", "campaign_id": "CAMP-789", "conversions": 3},
        ]

        with open(self.spend_path, "w") as f:
            json.dump(spend_data, f)
        with open(self.conv_path, "w") as f:
            json.dump(conv_data, f)

    def tearDown(self):
        # Delete temporary files
        os.remove(self.spend_path)
        os.remove(self.conv_path)
        os.rmdir(self.temp_dir)

    def test_cpa_calculation_and_merge(self):
        # Initialization
        reader = JsonDataReader()
        calculator = CpaCalculator()

        # Reading and merging data
        df = reader.read(self.spend_path, self.conv_path)

        # Checking the merge (outer join)
        self.assertEqual(len(df), 3, "Expected 3 rows after outer join")
        camp_123 = df[df["campaign_id"] == "CAMP-123"].iloc[0]
        camp_456 = df[df["campaign_id"] == "CAMP-456"].iloc[0]
        camp_789 = df[df["campaign_id"] == "CAMP-789"].iloc[0]
        self.assertEqual(camp_123["spend"], 37.5, "CAMP-123 spend incorrect")
        self.assertEqual(camp_123["conversions"], 14, "CAMP-123 conversions incorrect")
        self.assertEqual(camp_456["spend"], 19.9, "CAMP-456 spend incorrect")
        self.assertEqual(camp_456["conversions"], 0, "CAMP-456 conversions incorrect")
        self.assertEqual(camp_789["spend"], 0, "CAMP-789 spend incorrect")
        self.assertEqual(camp_789["conversions"], 3, "CAMP-789 conversions incorrect")

        # CPA Calculation
        df = calculator.process(df)

        # CPA Check
        self.assertAlmostEqual(
            df[df["campaign_id"] == "CAMP-123"].iloc[0]["cpa"],
            37.5 / 14,
            places=5,
            msg="CAMP-123 CPA incorrect",
        )
        self.assertTrue(
            pd.isna(df[df["campaign_id"] == "CAMP-456"].iloc[0]["cpa"]),
            "CAMP-456 CPA should be None (conversions=0)",
        )
        self.assertTrue(
            pd.isna(df[df["campaign_id"] == "CAMP-789"].iloc[0]["cpa"]),
            "CAMP-789 CPA should be None (spend=0)",
        )


if __name__ == "__main__":
    unittest.main()
