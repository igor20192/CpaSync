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
    __tablename__ = "daily_stats"
    date = Column(String, primary_key=True)
    campaign_id = Column(String, primary_key=True)
    spend = Column(Float)
    conversions = Column(Integer)
    cpa = Column(Float, nullable=True)


class DatabaseRepository(ABC):
    @abstractmethod
    def upsert(self, data: pd.DataFrame):
        pass

    @abstractmethod
    def check_date_exists(self, date: str) -> bool:
        pass


class PostgresRepository(DatabaseRepository):
    def __init__(self, engine: sa.Engine):
        self.engine = engine
        Base.metadata.create_all(engine)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def upsert(self, data: pd.DataFrame):
        with self.engine.connect() as conn:
            for _, row in data.iterrows():
                stmt = (
                    insert(DailyStats)
                    .values(
                        date=row["date"],
                        campaign_id=row["campaign_id"],
                        spend=row.get("spend", 0),
                        conversions=row.get("conversions", 0),
                        cpa=row["cpa"],
                    )
                    .on_conflict_do_update(
                        index_elements=["date", "campaign_id"],
                        set_=dict(
                            spend=row["spend"],
                            conversions=row["conversions"],
                            cpa=row["cpa"],
                        ),
                    )
                )
                conn.execute(stmt)
            conn.commit()
        logging.info(f"Upserted {len(data)} records")

    def check_date_exists(self, date: str) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.select(DailyStats).where(DailyStats.date == date)
            ).fetchone()
            return result is not None
