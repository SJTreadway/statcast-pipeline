"""
utils/transform_utils.py
Cleaning and transformation logic for raw Statcast data.
Keeping transforms separate from the DAG keeps the DAG readable
and makes this logic independently testable.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Columns we actually want to keep from pybaseball's output.
# pybaseball returns ~90 columns; we select the meaningful subset
# that maps to our Snowflake schema.
COLUMNS_TO_KEEP = [
    "game_pk", "game_date", "game_year", "game_type",
    "pitch_number", "inning", "inning_topbot", "at_bat_number",
    "pitch_name", "pitch_type",
    "pitcher", "batter", "player_name", "stand", "p_throws",
    "home_team", "away_team",
    "release_speed", "release_spin_rate", "release_extension",
    "release_pos_x", "release_pos_y", "release_pos_z",
    "pfx_x", "pfx_z", "plate_x", "plate_z",
    "vx0", "vy0", "vz0", "ax", "ay", "az",
    "effective_speed", "spin_axis",
    "description", "events", "type", "zone",
    "hit_location", "bb_type",
    "hc_x", "hc_y", "hit_distance_sc",
    "launch_speed", "launch_angle", "launch_speed_angle",
    "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
    "woba_value", "woba_denom", "babip_value", "iso_value",
    "delta_home_win_exp", "delta_run_exp",
    "balls", "strikes", "outs_when_up",
    "on_1b", "on_2b", "on_3b",
    "post_home_score", "post_away_score",
]


def clean_statcast(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning steps to a raw pybaseball DataFrame.
    Returns a cleaned DataFrame ready for Snowflake ingestion.
    """
    logger.info(f"Starting transform on {len(df):,} rows.")

    df = _select_columns(df)
    df = _parse_dates(df)
    df = _coerce_numerics(df)
    df = _drop_pitchless_rows(df)
    df = _standardize_column_names(df)

    logger.info(f"Transform complete. {len(df):,} rows remaining.")
    return df


def _select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the columns we care about, silently ignoring missing ones."""
    available = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    missing = set(COLUMNS_TO_KEEP) - set(available)
    if missing:
        logger.warning(f"Columns not found in source data (will be NULL): {missing}")
    return df[available].copy()


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure game_date is a proper date and game_year is derived from it."""
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    if "game_year" not in df.columns and "game_date" in df.columns:
        df["game_year"] = pd.to_datetime(df["game_date"]).dt.year
    return df


def _coerce_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    pybaseball occasionally returns numeric columns as object dtype.
    Force them to float/int, turning unparseable values into NaN.
    """
    float_cols = [
        "release_speed", "release_spin_rate", "release_extension",
        "release_pos_x", "release_pos_y", "release_pos_z",
        "pfx_x", "pfx_z", "plate_x", "plate_z",
        "vx0", "vy0", "vz0", "ax", "ay", "az",
        "effective_speed", "spin_axis",
        "hc_x", "hc_y", "hit_distance_sc",
        "launch_speed", "launch_angle",
        "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
        "woba_value", "babip_value", "iso_value",
        "delta_home_win_exp", "delta_run_exp",
    ]
    int_cols = [
        "game_pk", "game_year", "pitch_number", "inning",
        "at_bat_number", "zone", "hit_location", "launch_speed_angle",
        "woba_denom", "balls", "strikes", "outs_when_up",
        "on_1b", "on_2b", "on_3b", "post_home_score", "post_away_score",
        "pitcher", "batter",
    ]

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    for col in int_cols:
        if col in df.columns:
            # Use Int64 (nullable integer) so NaN doesn't break int conversion
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def _drop_pitchless_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rows with no pitch_type are non-pitch events (pickoffs, etc.).
    Dropping them keeps the table focused on actual pitch data.
    """
    before = len(df)
    df = df[df["pitch_type"].notna() & (df["pitch_type"] != "")]
    dropped = before - len(df)
    if dropped:
        logger.info(f"Dropped {dropped:,} non-pitch rows.")
    return df


def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase all column names to match Snowflake convention."""
    df.columns = [c.upper() for c in df.columns]
    return df


def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Lightweight data quality checks before loading.
    Raises ValueError if any check fails, which will fail the Airflow task
    and trigger alerting rather than silently loading bad data.
    """
    checks = {
        "No rows in DataFrame": len(df) == 0,
        "Missing GAME_PK column": "GAME_PK" not in df.columns,
        "All RELEASE_SPEED values are null": (
            "RELEASE_SPEED" in df.columns and df["RELEASE_SPEED"].isna().all()
        ),
        "Impossible launch angle (>90 or <-90)": (
            "LAUNCH_ANGLE" in df.columns
            and df["LAUNCH_ANGLE"].dropna().abs().gt(90).any()
        ),
        "Pitch speed below 40 mph detected": (
            "RELEASE_SPEED" in df.columns
            and df["RELEASE_SPEED"].dropna().lt(40).any()
        ),
    }

    failures = [msg for msg, failed in checks.items() if failed]
    if failures:
        raise ValueError(f"Data quality checks failed:\n" + "\n".join(f"  - {f}" for f in failures))

    logger.info(f"All data quality checks passed. ({len(df):,} rows)")
