"""
statcast_backfill.py
====================
One-off DAG for loading historical Statcast data in weekly chunks.
Trigger from the Airflow UI - a form will appear with season/date fields.

Note: Statcast pitch-tracking data is only available from 2015 onward.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
import pybaseball

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from utils.snowflake_utils import load_dataframe
from utils.transform_utils import clean_statcast, validate_dataframe

logger = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = "BASEBALL"
SNOWFLAKE_SCHEMA = "STATCAST"
TARGET_TABLE = "RAW_PITCHES"

# Statcast is available from 2015 onward (when MLB introduced the system).
# 2020 was shortened due to COVID - July 23 to Sept 27.
SEASON_DATES = {
    2015: ("2015-04-05", "2015-10-04"),
    2016: ("2016-04-03", "2016-10-02"),
    2017: ("2017-04-02", "2017-10-01"),
    2018: ("2018-03-29", "2018-10-01"),
    2019: ("2019-03-28", "2019-09-29"),
    2020: ("2020-07-23", "2020-09-27"),
    2021: ("2021-04-01", "2021-10-03"),
    2022: ("2022-04-07", "2022-10-05"),
    2023: ("2023-03-30", "2023-10-01"),
    2024: ("2024-03-20", "2024-09-29"),
}

pybaseball.cache.enable()


@dag(
    dag_id="statcast_backfill",
    description="Backfill historical Statcast data by season or date range",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={"owner": "steven.treadway", "retries": 2, "retry_delay": timedelta(minutes=10)},
    max_active_tasks=2,
    tags=["baseball", "statcast", "backfill"],
    params={
        "season": Param(2023, type="integer", description="Season year (2015-2024). Ignored if start_date/end_date are set."),
        "start_date": Param(None, type=["null", "string"], description="Optional custom start date YYYY-MM-DD"),
        "end_date": Param(None, type=["null", "string"], description="Optional custom end date YYYY-MM-DD"),
    },
)
def statcast_backfill():

    @task
    def get_weekly_chunks(**context) -> list[dict]:
        """
        Reads params from the UI trigger form and splits the date range
        into weekly chunks for parallel processing.
        """
        params = context.get("params", {})

        start_date = params.get("start_date")
        end_date = params.get("end_date")
        season = params.get("season", 2023)

        if start_date and end_date:
            start_str = start_date
            end_str = end_date
            logger.info(f"Custom range mode: {start_str} → {end_str}")
        else:
            season = int(season)
            if season not in SEASON_DATES:
                raise ValueError(f"Season {season} not supported. Statcast available 2015-2024.")
            start_str, end_str = SEASON_DATES[season]
            logger.info(f"Season mode: {season} ({start_str} → {end_str})")

        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()

        chunks = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=2), end)
            chunks.append({
                "start_date": cursor.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
            })
            cursor = chunk_end + timedelta(days=1)

        logger.info(f"Created {len(chunks)} weekly chunks.")
        return chunks

    @task
    def extract_chunk(chunk: dict) -> dict:
        """Extract one week of Statcast data."""
        start = chunk["start_date"]
        end = chunk["end_date"]
        logger.info(f"Extracting: {start} → {end}")

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
        """Transform and load one chunk. Returns rows loaded."""
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
        )
        return rows

    @task
    def summarize(row_counts: list[int]) -> None:
        total = sum(row_counts)
        logger.info(f"Backfill complete. Total rows loaded: {total:,}")

    chunks = get_weekly_chunks()
    extracted = extract_chunk.expand(chunk=chunks)
    row_counts = transform_and_load_chunk.expand(extract_result=extracted)
    summarize(row_counts)


statcast_backfill()
