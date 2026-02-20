"""
utils/snowflake_utils.py
Connects to Snowflake directly via environment variables.
"""

import logging
import os
from typing import Optional
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)


def get_conn():
    """
    Create a Snowflake connection from environment variables.
    These are set in your .env file and passed into the container via docker-compose.
    """
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def get_max_loaded_date(
    table: str,
    date_col: str = "game_date",
    conn_id: str = "snowflake_default",  # kept for signature compatibility
) -> Optional[str]:
    """Return the most recent game_date already in the table, or None if empty."""
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX({date_col}) FROM {table}")
        result = cursor.fetchone()
        if result and result[0]:
            return str(result[0])
        return None
    finally:
        conn.close()


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    schema: str,
    database: str,
    conn_id: str = "snowflake_default",  # kept for signature compatibility
    chunk_size: int = 10_000,
) -> int:
    """Write a DataFrame to Snowflake using write_pandas (bulk PUT/COPY)."""
    if df.empty:
        logger.warning("DataFrame is empty - nothing to load.")
        return 0

    conn = get_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table.upper(),
            schema=schema.upper(),
            database=database.upper(),
            chunk_size=chunk_size,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if not success:
            raise RuntimeError(f"write_pandas failed after {nchunks} chunks")

        logger.info(f"Loaded {nrows:,} rows into {database}.{schema}.{table}")
        return nrows
    finally:
        conn.close()


def run_sql(sql: str, conn_id: str = "snowflake_default") -> None:
    """Execute a SQL statement."""
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        logger.info("SQL executed successfully.")
    finally:
        conn.close()
