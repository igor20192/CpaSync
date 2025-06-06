import pandas as pd
import logging


def calculate_cpa(spend: float, conversions: float) -> float | None:
    """
    Calculates Cost Per Acquisition (CPA).

    Args:
        spend: The total amount spent.
        conversions: The total number of conversions.

    Returns:
        The calculated CPA, or None if conversions are zero.
    """
    if conversions == 0 or spend == 0:
        return None  # CPA is undefined if there are no conversions
    # If spend is 0 and conversions > 0, CPA is 0.0
    return spend / conversions


class CpaCalculator:
    """Calculates CPA for a given DataFrame."""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a 'cpa' column to the DataFrame by calculating CPA for each row.

        Args:
            df: DataFrame with 'spend' and 'conversions' columns.

        Returns:
            DataFrame with an added 'cpa' column.
        """
        df["cpa"] = df.apply(
            # JsonDataReader ensures "spend" and "conversions" (filled with 0 if missing) exist
            lambda row: calculate_cpa(row["spend"], row["conversions"]),
            axis=1,
        )
        logging.info(f"Calculated CPA for {len(df)} records")
        return df
