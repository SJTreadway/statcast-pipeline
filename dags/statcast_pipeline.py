"""
statcast_pipeline.py
====================
Daily Airflow DAG that ingests MLB Statcast pitch-level data into Snowflake.

Schedule: Runs daily at 8 AM ET. Pulls the previous day's data so that
          completed games are always fully available from MLB's API.

Architecture:
    extract → validate → transform → load → run_dbt_models (optional)

Each task is a plain Python function decorated with @task (TaskFlow API).
TaskFlow is the modern Airflow style - it removes the boilerplate of
XCom.push/pull and makes data flow between tasks explicit and readable.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
import pybaseball

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from utils.snowflake_utils import get_max_loaded_date, load_dataframe, run_sql
from utils.transform_utils import clean_statcast, validate_dataframe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level configuration
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "BASEBALL"
SNOWFLAKE_SCHEMA = "STATCAST"
TARGET_TABLE = "RAW_PITCHES"

# pybaseball makes Statcast calls to baseballsavant.mlb.com.
# Caching avoids re-downloading the same data on retries.
pybaseball.cache.enable()

DEFAULT_ARGS = {
    "owner": "steven.treadway",
    "depends_on_past": False,
    # Retry once after 5 minutes - handles transient Baseball Savant API blips
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # swap for True + your email in production
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
@dag(
    dag_id="statcast_daily_ingest",
    description="Pull MLB Statcast pitch data from Baseball Savant and load to Snowflake",
    schedule_interval="0 8 * * *",  # 8 AM UTC daily
    start_date=days_ago(1),
    catchup=False,                  # Don't backfill missed runs automatically
    default_args=DEFAULT_ARGS,
    tags=["baseball", "statcast", "snowflake"],
    doc_md="""
    ## Statcast Daily Ingest

    Pulls pitch-level Statcast data for the **previous calendar day** and loads it
    into `BASEBALL.STATCAST.RAW_PITCHES` in Snowflake.

    ### Incremental logic
    The pipeline checks the max `game_date` already in Snowflake. If yesterday's
    data is already present it skips the load step gracefully (idempotent).

    ### Backfilling
    To backfill a specific date range, trigger the DAG manually with the config:
    ```json
    { "start_date": "2024-04-01", "end_date": "2024-04-07" }
    ```
    """,
)
def statcast_daily_ingest():

    # ------------------------------------------------------------------
    # TASK 1: Determine what date range to pull
    # ------------------------------------------------------------------
    @task
    def get_date_range(**context) -> dict:
        """
        Decide which dates to pull. Supports two modes:
        1. Manual trigger with config: use provided start/end dates
        2. Scheduled run: pull yesterday's games

        Returns a dict with 'start_date' and 'end_date' strings (YYYY-MM-DD).
        """
        dag_run_conf = context.get("dag_run").conf or {}

        if dag_run_conf.get("start_date") and dag_run_conf.get("end_date"):
            start = dag_run_conf["start_date"]
            end = dag_run_conf["end_date"]
            logger.info(f"Manual backfill mode: {start} → {end}")
        else:
            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
            start = end = yesterday
            logger.info(f"Scheduled mode: pulling {yesterday}")

        return {"start_date": start, "end_date": end}

    # ------------------------------------------------------------------
    # TASK 2: Check if data already exists (idempotency guard)
    # ------------------------------------------------------------------
    @task
    def check_already_loaded(date_range: dict) -> dict:
        """
        Check whether this date range is already in Snowflake.
        If it is, we flag it so the downstream load step can skip gracefully.
        This makes the pipeline idempotent - safe to re-run without duplicates.
        """
        max_date = get_max_loaded_date(
            table=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TARGET_TABLE}",
            conn_id=SNOWFLAKE_CONN_ID,
        )

        target_end = date_range["end_date"]
        already_loaded = max_date is not None and max_date >= target_end

        if already_loaded:
            logger.info(
                f"Data through {max_date} already in Snowflake. "
                f"Target end date {target_end} is covered - will skip load."
            )
        else:
            logger.info(
                f"Max loaded date: {max_date}. Will pull through {target_end}."
            )

        return {**date_range, "skip_load": already_loaded}

    # ------------------------------------------------------------------
    # TASK 3: Extract from Baseball Savant via pybaseball
    # ------------------------------------------------------------------
    @task
    def extract(date_range: dict) -> dict:
        """
        Pull Statcast pitch data using pybaseball.
        Returns the DataFrame serialized to JSON for XCom transfer.

        Note: pybaseball.statcast() hits Baseball Savant's API.
        Large date ranges (>2 weeks) can be slow - the daily schedule
        intentionally keeps each run small.
        """
        if date_range.get("skip_load"):
            logger.info("Skip flag set - bypassing extract.")
            return {"data": None, "row_count": 0, "skip_load": True}

        start = date_range["start_date"]
        end = date_range["end_date"]

        logger.info(f"Extracting Statcast data: {start} → {end}")
        df = pybaseball.statcast(start_dt=start, end_dt=end, verbose=True)

        if df is None or df.empty:
            logger.warning(f"No data returned for {start} → {end}. Possibly an off-day.")
            return {"data": None, "row_count": 0, "skip_load": True}

        logger.info(f"Extracted {len(df):,} rows, {len(df.columns)} columns.")

        # Serialize to JSON for XCom. orient="split" is efficient and 
        # preserves column types better than the default orient.
        return {
            "data": df.to_json(orient="split", date_format="iso"),
            "row_count": len(df),
            "skip_load": False,
            "start_date": start,
            "end_date": end,
        }

    # ------------------------------------------------------------------
    # TASK 4: Transform and validate
    # ------------------------------------------------------------------
    @task
    def transform_and_validate(extract_result: dict) -> dict:
        """
        Apply cleaning logic and run data quality checks.
        If validation fails, this task raises an exception, failing the DAG
        run and (in production) triggering an alert - catching bad data
        before it reaches Snowflake.
        """
        if extract_result.get("skip_load"):
            return extract_result

        df = pd.read_json(extract_result["data"], orient="split")

        # Apply all cleaning transforms (see transform_utils.py)
        df = clean_statcast(df)

        # Run data quality checks - raises ValueError on failure
        validate_dataframe(df)

        logger.info(f"Transform complete. {len(df):,} clean rows ready to load.")

        return {
            **extract_result,
            "data": df.to_json(orient="split", date_format="iso"),
            "row_count": len(df),
        }

    # ------------------------------------------------------------------
    # TASK 5: Load to Snowflake
    # ------------------------------------------------------------------
    @task
    def load_to_snowflake(transform_result: dict) -> dict:
        """
        Write the cleaned DataFrame to Snowflake RAW_PITCHES table.
        Uses write_pandas (PUT/COPY) for fast bulk loading.
        """
        if transform_result.get("skip_load"):
            logger.info("Skipping load - no new data or already loaded.")
            return {"rows_loaded": 0, "status": "skipped"}

        df = pd.read_json(transform_result["data"], orient="split")

        rows_loaded = load_dataframe(
            df=df,
            table=TARGET_TABLE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
            conn_id=SNOWFLAKE_CONN_ID,
        )

        return {
            "rows_loaded": rows_loaded,
            "start_date": transform_result["start_date"],
            "end_date": transform_result["end_date"],
            "status": "success",
        }

    # ------------------------------------------------------------------
    # TASK 6: Refresh downstream views
    # ------------------------------------------------------------------
    @task
    def refresh_views(load_result: dict) -> None:
        """
        After loading, re-run the aggregation views so analysts always
        see fresh data without needing to schedule separate refresh jobs.
        In production this is where you'd trigger a dbt run instead.
        """
        if load_result.get("status") == "skipped":
            logger.info("No new data loaded - skipping view refresh.")
            return

        # Recreating the view refreshes it since it's built on top of RAW_PITCHES
        run_sql(
            f"ALTER VIEW {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PITCHER_ARSENAL COMPILE",
            conn_id=SNOWFLAKE_CONN_ID,
        )
        logger.info("Views refreshed.")

    # ------------------------------------------------------------------
    # Wire tasks together (TaskFlow handles dependencies automatically)
    # ------------------------------------------------------------------
    date_range = get_date_range()
    checked = check_already_loaded(date_range)
    extracted = extract(checked)
    transformed = transform_and_validate(extracted)
    loaded = load_to_snowflake(transformed)
    refresh_views(loaded)


# Instantiate the DAG
statcast_daily_ingest()
