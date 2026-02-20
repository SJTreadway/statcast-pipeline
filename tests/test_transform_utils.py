"""
tests/test_transform_utils.py
Unit tests for the transform layer. These run without any Airflow or 
Snowflake connection, so they're fast and can run in CI.

Run with: pytest tests/
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../dags"))
from utils.transform_utils import clean_statcast, validate_dataframe, _drop_pitchless_rows


def make_sample_df(**overrides) -> pd.DataFrame:
    """Create a minimal valid Statcast-like DataFrame for testing."""
    base = {
        "game_pk": [717465, 717465, 717466],
        "game_date": ["2023-07-15", "2023-07-15", "2023-07-16"],
        "game_year": [2023, 2023, 2023],
        "game_type": ["R", "R", "R"],
        "pitch_number": [1, 2, 1],
        "inning": [1, 1, 3],
        "inning_topbot": ["Top", "Top", "Bot"],
        "at_bat_number": [1, 1, 12],
        "pitch_name": ["4-Seam Fastball", "Slider", "Curveball"],
        "pitch_type": ["FF", "SL", "CU"],
        "pitcher": [543037, 543037, 669203],
        "batter": [660271, 660271, 592450],
        "player_name": ["Gerrit Cole", "Gerrit Cole", "Dylan Cease"],
        "stand": ["R", "R", "L"],
        "p_throws": ["R", "R", "R"],
        "home_team": ["NYY", "NYY", "CHW"],
        "away_team": ["BOS", "BOS", "HOU"],
        "release_speed": [97.2, 88.5, 82.1],
        "release_spin_rate": [2400, 2650, 2900],
        "pfx_x": [0.8, -0.5, 0.1],
        "pfx_z": [1.2, 0.3, -0.8],
        "plate_x": [-0.2, 0.7, 0.0],
        "plate_z": [2.8, 1.9, 2.1],
        "release_extension": [6.7, 6.5, 5.9],
        "launch_speed": [None, None, 95.4],
        "launch_angle": [None, None, 22.0],
        "type": ["S", "B", "X"],
        "description": ["swinging_strike", "ball", "hit_into_play"],
        "events": [None, None, "single"],
        "balls": [0, 1, 2],
        "strikes": [0, 1, 0],
        "outs_when_up": [0, 0, 1],
    }
    base.update(overrides)
    return pd.DataFrame(base)


class TestDropPitchlessRows:
    def test_drops_null_pitch_type(self):
        df = make_sample_df()
        df.loc[0, "pitch_type"] = None
        result = _drop_pitchless_rows(df)
        assert len(result) == 2

    def test_drops_empty_string_pitch_type(self):
        df = make_sample_df()
        df.loc[1, "pitch_type"] = ""
        result = _drop_pitchless_rows(df)
        assert len(result) == 2

    def test_keeps_all_valid_rows(self):
        df = make_sample_df()
        result = _drop_pitchless_rows(df)
        assert len(result) == 3


class TestCleanStatcast:
    def test_output_columns_are_uppercase(self):
        df = make_sample_df()
        result = clean_statcast(df)
        assert all(c == c.upper() for c in result.columns)

    def test_game_date_is_parsed(self):
        df = make_sample_df()
        result = clean_statcast(df)
        # After cleaning, GAME_DATE should be python date objects
        assert str(result["GAME_DATE"].dtype) == "object"  # stored as date objects

    def test_release_speed_is_float(self):
        df = make_sample_df()
        df["release_speed"] = df["release_speed"].astype(str)  # simulate object dtype
        result = clean_statcast(df)
        assert result["RELEASE_SPEED"].dtype == "float64"

    def test_handles_extra_columns_gracefully(self):
        """Extra columns pybaseball might add should be silently dropped."""
        df = make_sample_df()
        df["some_future_column"] = "surprise"
        result = clean_statcast(df)
        assert "SOME_FUTURE_COLUMN" not in result.columns

    def test_returns_dataframe(self):
        df = make_sample_df()
        result = clean_statcast(df)
        assert isinstance(result, pd.DataFrame)


class TestValidateDataframe:
    def test_passes_valid_data(self):
        df = make_sample_df()
        df = clean_statcast(df)
        validate_dataframe(df)  # should not raise

    def test_fails_on_empty_dataframe(self):
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="No rows"):
            validate_dataframe(df)

    def test_fails_on_missing_game_pk(self):
        df = make_sample_df()
        df = clean_statcast(df)
        df = df.drop(columns=["GAME_PK"])
        with pytest.raises(ValueError, match="GAME_PK"):
            validate_dataframe(df)

    def test_fails_on_impossible_launch_angle(self):
        df = make_sample_df()
        df = clean_statcast(df)
        df["LAUNCH_ANGLE"] = 95.0  # impossible
        with pytest.raises(ValueError, match="launch angle"):
            validate_dataframe(df)

    def test_fails_on_low_pitch_speed(self):
        df = make_sample_df()
        df = clean_statcast(df)
        df["RELEASE_SPEED"] = 20.0  # way too slow
        with pytest.raises(ValueError, match="40 mph"):
            validate_dataframe(df)
