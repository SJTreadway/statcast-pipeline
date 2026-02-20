"""
statcast_backfill.py
====================
One-off DAG for loading historical Statcast data in weekly chunks.

Why chunk by week?
  Baseball Savant rate-limits large requests. Pulling an entire season 
  at once often results in timeouts or partial data. Weekly chunks are
  small enough to be reliable but large enough to finish in reasonable time.

Usage:
  1. Trigger manually from the Airflow UI
  2. Pass config: { "season": 2023 }
     Or a custom range: { "start_date": "2023-04-01", "end_date": "2023-09-30" }
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date

import pandas as pd
import pybaseball

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from utils.snowflake_utils import load_dataframe
from utils.transform_utils import clean_statcast, validate_dataframe

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "BASEBALL"
SNOWFLAKE_SCHEMA = "STATCAST"
TARGET_TABLE = "RAW_PITCHES"

# MLB regular season approximate boundaries (used when season= is passed)
SEASON_DATES = {
    2021: ("2021-04-01", "2021-10-03"),
    2022: ("2022-04-07", "2022-10-05"),
    2023: ("2023-03-30", "2023-10-01"),
    2024: ("2024-03-20", "2024-09-29"),
}

pybaseball.cache.enable()


@dag(
    dag_id="statcast_backfill",
    description="Backfill historical Statcast data by season or date range",
    schedule_interval=None,     # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    default_args={"owner": "steven.treadway", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["baseball", "statcast", "backfill"],
)
def statcast_backfill():

    @task
    def get_weekly_chunks(**context) -> list[dict]:
        """
        Break the requested date range into week-sized chunks.
        Returns a list of {start_date, end_date} dicts.
        """
        conf = context["dag_run"].conf or {}

        if "season" in conf:
            season = int(conf["season"])
            if season not in SEASON_DATES:
                raise ValueError(f"Season {season} not in known seasons: {list(SEASON_DATES.keys())}")
            start_str, end_str = SEASON_DATES[season]
        elif "start_date" in conf and "end_date" in conf:
            start_str = conf["start_date"]
            end_str = conf["end_date"]
        else:
            raise ValueError("Provide either 'season' or 'start_date'+'end_date' in DAG config.")

        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()

        chunks = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=6), end)
            chunks.append({
                "start_date": cursor.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
            })
            cursor = chunk_end + timedelta(days=1)

        logger.info(f"Split {start_str}→{end_str} into {len(chunks)} weekly chunks.")
        return chunks

    @task
    def extract_chunk(chunk: dict) -> dict:
        """Extract one week of Statcast data."""
        start = chunk["start_date"]
        end = chunk["end_date"]
        logger.info(f"Extracting chunk: {start} → {end}")

        df = pybaseball.statcast(start_dt=start, end_dt=end, verbose=False)

        if df is None or df.empty:
            logger.warning(f"No data for {start} → {end} (likely off-days).")
            return {**chunk, "data": None, "row_count": 0}

        return {
            **chunk,
            "data": df.to_json(orient="split", date_format="iso"),
            "row_count": len(df),
        }

    @task
    def transform_and_load_chunk(extract_result: dict) -> int:
        """Transform and immediately load one chunk. Returns rows loaded."""
        if not extract_result.get("data"):
            return 0

        df = pd.read_json(extract_result["data"], orient="split")
        df = clean_statcast(df)
        validate_dataframe(df)

        rows = load_dataframe(
            df=df,
            table=TARGET_TABLE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
            conn_id=SNOWFLAKE_CONN_ID,
        )
        return rows

    @task
    def summarize(row_counts: list[int]) -> None:
        total = sum(row_counts)
        logger.info(f"Backfill complete. Total rows loaded: {total:,}")

    # Wire it up
    # dynamic task mapping: one extract+load task spawned per weekly chunk
    chunks = get_weekly_chunks()
    extracted = extract_chunk.expand(chunk=chunks)
    row_counts = transform_and_load_chunk.expand(extract_result=extracted)
    summarize(row_counts)


statcast_backfill()
