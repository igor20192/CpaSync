import argparse
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine

from src.data_reader import JsonDataReader
from src.cpa_calculator import CpaCalculator
from src.db_repository import PostgresRepository
from src.scheduler import Scheduler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Logging format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Log file
file_handler = logging.FileHandler("app.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def validate_date(date_str: str) -> str:
    """Ensure the date string is in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD."
        )


def parse_arguments():
    """Parse and return validated command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process campaign data and calculate CPA."
    )
    parser.add_argument(
        "--start-date",
        type=validate_date,
        help="Start date (YYYY-MM-DD)",
        required=True,
    )
    parser.add_argument(
        "--end-date", type=validate_date, help="End date (YYYY-MM-DD)", required=True
    )

    args = parser.parse_args()

    # Optional: Ensure start_date <= end_date
    if args.start_date > args.end_date:
        parser.error("Start date must be earlier than or equal to end date.")

    return args


def main():
    """
    Main workflow:
    - Parse command-line arguments
    - Load data, process CPA, save results if not already in DB
    """
    args = parse_arguments()

    load_dotenv()

    data_reader = JsonDataReader()
    cpa_calculator = CpaCalculator()
    db_engine = create_engine(os.getenv("DB_URL"))
    repository = PostgresRepository(db_engine)

    logging.info(f"Started processing from {args.start_date} to {args.end_date}")

    try:
        for date in pd.date_range(args.start_date, args.end_date):
            date_str = date.strftime("%Y-%m-%d")

            if repository.check_date_exists(date_str):
                logging.info(f"Skipping {date_str}: already processed.")
                continue

            df = data_reader.read(os.getenv("SPEND_PATH"), os.getenv("CONV_PATH"))
            df = df[df["date"] == date_str]

            if df.empty:
                logging.info(f"No data found for {date_str}.")
                continue

            df = cpa_calculator.process(df)
            repository.upsert(df)

            logging.info(f"Processed and stored data for {date_str}.")
            print(f"Processed {date_str}: {len(df)} records")

    except Exception as e:
        logging.exception("An error occurred during processing.")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    scheduler = Scheduler(main)
    scheduler.start()
