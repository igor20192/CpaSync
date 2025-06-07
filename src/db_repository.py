from abc import ABC, abstractmethod
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
import logging

Base = declarative_base()


class DailyStats(Base):
    """
    SQLAlchemy ORM model representing daily campaign statistics.

    Attributes:
        date (str): The date of the record (ISO format).
        campaign_id (str): Unique identifier of the campaign.
        spend (float): Amount spent on the campaign.
        conversions (int): Number of conversions for the campaign.
        cpa (float, optional): Cost per acquisition (calculated).
    """

    __tablename__ = "daily_stats"

    date = Column(String, primary_key=True)
    campaign_id = Column(String, primary_key=True)
    spend = Column(Float)
    conversions = Column(Integer)
    cpa = Column(Float, nullable=True)


class DatabaseRepository(ABC):
    """
    Abstract base class for data persistence layer.
    Defines the interface for any database repository implementation.
    """

    @abstractmethod
    def upsert(self, data: pd.DataFrame):
        """
        Insert or update records in the database based on primary key constraints.

        Args:
            data (pd.DataFrame): Data to be persisted.
        """
        pass

    @abstractmethod
    def check_date_exists(self, date: str) -> bool:
        """
        Check if records already exist for a specific date.

        Args:
            date (str): The date to check (ISO format).

        Returns:
            bool: True if records exist, False otherwise.
        """
        pass


class PostgresRepository(DatabaseRepository):
    """
    PostgreSQL implementation of the DatabaseRepository.
    Handles reading and writing of daily campaign statistics.
    """

    def __init__(self, engine: sa.Engine):
        """
        Initialize the repository and create necessary tables if not present.

        Args:
            engine (sa.Engine): SQLAlchemy database engine.
        """
        self.engine = engine
        Base.metadata.create_all(engine)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def upsert(self, data: pd.DataFrame):
        """
        Insert or update campaign data in the PostgreSQL database.

        Utilizes PostgreSQL-specific `ON CONFLICT` clause to ensure upsert behavior.
        Retries the operation up to 3 times with 2-second intervals in case of failure.

        Args:
            data (pd.DataFrame): DataFrame containing campaign statistics.
        """
        records = data.to_dict("records")
        with self.engine.begin() as conn:
            stmt = insert(DailyStats).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "campaign_id"],
                set_={
                    "spend": stmt.excluded.spend,
                    "conversions": stmt.excluded.conversions,
                    "cpa": stmt.excluded.cpa,
                },
            )
            conn.execute(stmt)
        logging.info(f"Upserted {len(data)} records.")

    def check_date_exists(self, date: str) -> bool:
        """
        Check if data for the given date already exists in the database.

        Args:
            date (str): Date string in ISO format.

        Returns:
            bool: True if at least one record for the date exists, else False.
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.select(DailyStats).where(DailyStats.date == date)
            ).fetchone()
            return result is not None
