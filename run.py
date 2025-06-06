import argparse
import logging
from datetime import datetime

import pandas as pd
from src.data_reader import JsonDataReader
from src.cpa_calculator import CpaCalculator
from src.db_repository import PostgresRepository
from src.scheduler import Scheduler
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    parser = argparse.ArgumentParser(
        description="Process campaign data and calculate CPA."
    )
    parser.add_argument(
        "--start-date", type=str, help="Start date (ISO format)", required=True
    )
    parser.add_argument(
        "--end-date", type=str, help="End date (ISO format)", required=True
    )
    args = parser.parse_args()

    load_dotenv()
    reader = JsonDataReader()
    calculator = CpaCalculator()
    engine = create_engine(os.getenv("DB_URL"))
    repo = PostgresRepository(engine)

    logging.info(f"Starting processing for {args.start_date} to {args.end_date}")
    try:
        for date in pd.date_range(args.start_date, args.end_date):
            date_str = date.strftime("%Y-%m-%d")
            if not repo.check_date_exists(date_str):
                df = reader.read(os.getenv("SPEND_PATH"), os.getenv("CONV_PATH"))
                df = df[df["date"] == date_str]
                if not df.empty:
                    df = calculator.process(df)
                    repo.upsert(df)
                    logging.info(f"Processed and saved data for {date_str}")
                    print(f"Processed {date_str}: {len(df)} records")
                else:
                    logging.info(f"No data for {date_str}")
            else:
                logging.info(f"Skipping {date_str}: already processed")
    except Exception as e:
        logging.error(f"Failed processing: {str(e)}")
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    scheduler = Scheduler(main)
    scheduler.start()
