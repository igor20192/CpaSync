import pandas as pd
import logging
from functools import wraps


def handle_exceptions(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {f.__name__}: {str(e)}")
            print(f"Error: {str(e)}")
            return pd.DataFrame()  # Return an empty DataFrame to continue

    return wrapper


class JsonDataReader:
    @handle_exceptions
    def read(self, spend_path: str, conv_path: str) -> pd.DataFrame:
        spend_df = pd.read_json(spend_path)
        conv_df = pd.read_json(conv_path)
        merged_df = spend_df.merge(
            conv_df, on=["date", "campaign_id"], how="left"
        ).fillna({"conversions": 0})
        logging.info(f"Read and merged {len(merged_df)} records")
        return merged_df
