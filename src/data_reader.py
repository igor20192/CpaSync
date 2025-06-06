import pandas as pd
import logging
from functools import wraps
from typing import Callable


def handle_exceptions(func: Callable) -> Callable:
    """
    Decorator for handling exceptions in functions or methods.

    If an exception is raised during execution, it logs the error and
    returns an empty DataFrame instead of propagating the exception.

    Args:
        func (Callable): The target function to wrap.

    Returns:
        Callable: The wrapped function with exception handling.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> pd.DataFrame:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            print(f"Error: {str(e)}")
            return pd.DataFrame()

    return wrapper


class JsonDataReader:
    """
    A class responsible for reading and merging JSON data files
    related to campaign spending and conversions.
    """

    @handle_exceptions
    def read(self, spend_path: str, conv_path: str) -> pd.DataFrame:
        """
        Reads two JSON files and merges them on 'date' and 'campaign_id'.

        Missing values for 'spend' and 'conversions' are filled with zeroes.

        Args:
            spend_path (str): Path to the JSON file containing spending data.
            conv_path (str): Path to the JSON file containing conversions data.

        Returns:
            pd.DataFrame: A merged DataFrame containing both spend and
                          conversion data, with missing values handled.
        """
        spend_df = pd.read_json(spend_path)
        conv_df = pd.read_json(conv_path)

        merged_df = spend_df.merge(
            conv_df, on=["date", "campaign_id"], how="outer"
        ).fillna({"spend": 0, "conversions": 0})

        logging.info(f"Read and merged {len(merged_df)} records")
        return merged_df
