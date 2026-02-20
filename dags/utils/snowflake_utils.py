"""
utils/snowflake_utils.py
Reusable helpers for interacting with Snowflake via the Airflow connection.
The SnowflakeHook handles auth entirely from the Airflow connection - no
credentials ever appear in application code.
"""

import logging
from typing import Optional
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)


def get_hook(conn_id: str = "snowflake_default") -> SnowflakeHook:
    """Return a SnowflakeHook using the Airflow connection."""
    return SnowflakeHook(snowflake_conn_id=conn_id)


def table_exists(table: str, schema: str, database: str, conn_id: str = "snowflake_default") -> bool:
    """Check whether a table exists in Snowflake."""
    hook = get_hook(conn_id)
    sql = f"""
        SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema.upper()}'
          AND TABLE_NAME   = '{table.upper()}'
    """
    result = hook.get_first(sql)
    return result[0] > 0


def get_max_loaded_date(
    table: str,
    date_col: str = "game_date",
    conn_id: str = "snowflake_default",
) -> Optional[str]:
    """
    Return the most recent game_date already in the table,
    or None if the table is empty. Used for incremental loading.
    """
    hook = get_hook(conn_id)
    sql = f"SELECT MAX({date_col}) FROM {table}"
    result = hook.get_first(sql)
    if result and result[0]:
        return str(result[0])
    return None


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    schema: str,
    database: str,
    conn_id: str = "snowflake_default",
    chunk_size: int = 10_000,
) -> int:
    """
    Write a DataFrame to Snowflake using write_pandas (fastest method - 
    uses Snowflake's PUT/COPY under the hood rather than row-by-row inserts).

    Returns the number of rows loaded.
    """
    if df.empty:
        logger.warning("DataFrame is empty - nothing to load.")
        return 0

    hook = get_hook(conn_id)

    # write_pandas requires an active snowflake-connector-python connection
    conn = hook.get_conn()

    from snowflake.connector.pandas_tools import write_pandas

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table.upper(),
        schema=schema.upper(),
        database=database.upper(),
        chunk_size=chunk_size,
        auto_create_table=False,  # We always create tables explicitly in SQL
        overwrite=False,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(f"write_pandas failed after {nchunks} chunks")

    logger.info(f"Loaded {nrows:,} rows into {database}.{schema}.{table}")
    return nrows


def run_sql(sql: str, conn_id: str = "snowflake_default") -> None:
    """Execute a SQL statement (DDL, DML, etc.)."""
    hook = get_hook(conn_id)
    hook.run(sql)
    logger.info("SQL executed successfully.")
