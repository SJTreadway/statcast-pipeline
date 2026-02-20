-- Run this in Snowflake once to set up your environment.
-- Snowflake free trial gives you $400 of credits - more than enough for this project.

-- 1. Create the database and schema
CREATE DATABASE IF NOT EXISTS BASEBALL;
CREATE SCHEMA IF NOT EXISTS BASEBALL.STATCAST;

-- 2. Create the raw pitches table
--    Column names mirror pybaseball's statcast() output exactly,
--    so we can load without any renaming.
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.RAW_PITCHES (
    -- Identifiers
    game_pk             INTEGER,
    game_date           DATE,
    game_year           INTEGER,
    game_type           VARCHAR(5),     -- R=Regular, P=Postseason, etc.

    -- Pitch metadata
    pitch_number        INTEGER,
    inning              INTEGER,
    inning_topbot       VARCHAR(3),
    at_bat_number       INTEGER,
    pitch_name          VARCHAR(50),
    pitch_type          VARCHAR(5),

    -- Pitcher / batter
    pitcher             INTEGER,        -- MLBAM player ID
    batter              INTEGER,
    player_name         VARCHAR(100),
    stand               VARCHAR(1),     -- L or R (batter)
    p_throws            VARCHAR(1),     -- L or R (pitcher)
    home_team           VARCHAR(5),
    away_team           VARCHAR(5),

    -- Pitch physics
    release_speed       FLOAT,
    release_spin_rate   FLOAT,
    release_extension   FLOAT,
    release_pos_x       FLOAT,
    release_pos_y       FLOAT,
    release_pos_z       FLOAT,
    pfx_x               FLOAT,          -- Horizontal movement (inches)
    pfx_z               FLOAT,          -- Vertical movement (inches)
    plate_x             FLOAT,          -- Horizontal location at plate
    plate_z             FLOAT,          -- Vertical location at plate
    vx0                 FLOAT,
    vy0                 FLOAT,
    vz0                 FLOAT,
    ax                  FLOAT,
    ay                  FLOAT,
    az                  FLOAT,
    effective_speed     FLOAT,
    spin_axis           FLOAT,

    -- Outcome
    description         VARCHAR(100),
    events              VARCHAR(100),
    type                VARCHAR(1),     -- B=Ball, S=Strike, X=InPlay
    zone                INTEGER,        -- Strike zone region (1-14)
    hit_location        INTEGER,
    bb_type             VARCHAR(50),
    hc_x                FLOAT,          -- Hit coordinate x
    hc_y                FLOAT,          -- Hit coordinate y
    hit_distance_sc     FLOAT,
    launch_speed        FLOAT,
    launch_angle        FLOAT,
    launch_speed_angle  INTEGER,

    -- Expected stats (Statcast xStats)
    estimated_ba_using_speedangle       FLOAT,  -- xBA
    estimated_woba_using_speedangle     FLOAT,  -- xwOBA
    woba_value          FLOAT,
    woba_denom          INTEGER,
    babip_value         FLOAT,
    iso_value           FLOAT,
    delta_home_win_exp  FLOAT,
    delta_run_exp       FLOAT,

    -- Count state
    balls               INTEGER,
    strikes             INTEGER,
    outs_when_up        INTEGER,
    on_1b               INTEGER,
    on_2b               INTEGER,
    on_3b               INTEGER,
    post_home_score     INTEGER,
    post_away_score     INTEGER,

    -- Pipeline metadata
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source             VARCHAR(50) DEFAULT 'pybaseball'
);

-- 3. Create a deduplicated view (game_pk + at_bat_number + pitch_number = unique pitch)
CREATE OR REPLACE VIEW BASEBALL.STATCAST.PITCHES AS
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY game_pk, at_bat_number, pitch_number
               ORDER BY _loaded_at DESC
           ) AS rn
    FROM BASEBALL.STATCAST.RAW_PITCHES
)
WHERE rn = 1;

-- 4. Aggregated pitcher arsenal view - useful for quick analysis
CREATE OR REPLACE VIEW BASEBALL.STATCAST.PITCHER_ARSENAL AS
SELECT
    game_year,
    pitcher,
    player_name,
    pitch_type,
    pitch_name,
    COUNT(*)                            AS pitches_thrown,
    ROUND(AVG(release_speed), 1)        AS avg_velo,
    ROUND(MAX(release_speed), 1)        AS max_velo,
    ROUND(AVG(release_spin_rate), 0)    AS avg_spin,
    ROUND(AVG(pfx_x) * 12, 1)          AS avg_h_break_in,   -- convert to inches
    ROUND(AVG(pfx_z) * 12, 1)          AS avg_v_break_in,
    ROUND(AVG(release_extension), 1)    AS avg_extension,
    ROUND(
        SUM(CASE WHEN type = 'S' OR description = 'hit_into_play' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                   AS strike_pct,
    ROUND(AVG(estimated_woba_using_speedangle), 3) AS avg_xwoba
FROM BASEBALL.STATCAST.PITCHES
GROUP BY 1, 2, 3, 4, 5;
