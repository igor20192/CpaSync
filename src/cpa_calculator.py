import pandas as pd
import logging


def calculate_cpa(spend: float, conversions: float) -> float:
    return spend / conversions if conversions != 0 else None


class CpaCalculator:
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df["cpa"] = df.apply(
            lambda row: calculate_cpa(row["spend"], row.get("conversions", 0)), axis=1
        )
        logging.info(f"Calculated CPA for {len(df)} records")
        return df
