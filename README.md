# MLB Pipeline

A data pipeline that ingests daily MLB Statcast data into Snowflake and maintains historical team logs. Powers the ML models in [mlb_py](https://github.com/SJTreadway/mlb_py).

---

## Architecture

```
MLB Stats API (boxscores)          Statcast API (pybaseball)
        │                                    │
        ▼                                    ▼
  Game Results                    Batter + Pitcher
  (outcomes, scores)              pitch-level data
        │                                    │
        └──────────────┬─────────────────────┘
                       ▼
              GitHub Actions
              (self-hosted EC2)
              jobs/daily_statcast_ingest.py
                       │
                       ▼
                   Snowflake
        ┌──────────────────────────────────┐
        │  BASEBALL.STATCAST               │
        │  ├── RAW_BATTER_GAMES            │
        │  ├── RAW_PITCHER_GAMES           │
        │  ├── GAME_RESULTS                │
        │  ├── BATTER_ROLLING_FEATURES     │
        │  └── PITCHER_ROLLING_FEATURES    │
        │                                  │
        │  BASEBALL.HISTORICAL             │
        │  ├── RETROSHEET_EVENTS           │
        │  ├── TEAM_BATTING_LOGS           │
        │  └── TEAM_PITCHING_LOGS          │
        └──────────────────────────────────┘
```

---

## Project Structure

```
mlb-pipeline/
├── .github/
│   └── workflows/
│       ├── daily_ingest.yml        # Runs 3x daily (11am, 1pm, 3pm CST)
│       └── weekly_retrain.yml      # Mondays 7am CST — model retraining
├── dags/
│   ├── daily_statcast_features.py  # Daily Statcast ingestion DAG
│   ├── historical_retrosheet_events_backfill.py
│   ├── historical_team_batting_logs_backfill.py
│   ├── historical_team_pitching_logs_backfill.py
│   └── utils/
│       ├── bref.py
│       ├── historical_team_utils.py
│       ├── odds.py
│       ├── retrosheet.py
│       ├── snowflake_utils.py
│       └── transform_utils.py
├── include/
│   └── sql/
│       ├── setup_snowflake_batter.sql
│       ├── setup_snowflake_pitcher.sql
│       ├── setup_statcast_features.sql
│       ├── setup_snowflake_historical_team_game_logs.sql
│       └── setup_snowflake_historical_retrosheet_events.sql
├── jobs/
│   ├── statcast_pipeline.py        # Core ingestion logic (no Airflow dependency)
│   ├── daily_statcast_ingest.py    # GitHub Actions entry point
│   ├── backfill_statcast.py        # Historical backfill (2015-2026)
│   └── retrain_models.py           # Weekly model retraining
├── logs/                           # Airflow scheduler & DAG processor logs
├── plugins/                        # Airflow plugins (empty)
├── tests/
│   └── test_transform_utils.py
├── docker-compose.yml              # Airflow + Postgres (local development)
├── Dockerfile
├── .env.example
├── .gitignore
├── Notes.md
└── requirements.txt
```

---

## Daily Pipeline (GitHub Actions)

Runs automatically 3x daily on a self-hosted AWS EC2 runner with a static IP whitelisted in Snowflake.

**Schedule:** 11am, 1pm, 3pm CST — multiple runs ensure lineup confirmation coverage before first pitch.

**Each run:**
1. Fetches yesterday's boxscores from MLB Stats API → extracts all player IDs
2. Pulls Statcast pitch-level data per batter → transforms to game-level rows → upserts to `RAW_BATTER_GAMES`
3. Pulls Statcast pitch-level data per pitcher → transforms to game-level rows → upserts to `RAW_PITCHER_GAMES`
4. Fetches game outcomes → upserts to `GAME_RESULTS`
5. Recomputes rolling features for all players → upserts to `BATTER_ROLLING_FEATURES` and `PITCHER_ROLLING_FEATURES`
6. Logs today's lineup confirmation status

**Rolling windows:**
- Batters: 7, 14, 30, 75, 162, 350 games
- Pitchers: 10, 35, 75 games

**Features computed:**
- Batters: barrel%, EV, hard hit%, sweet spot%, HR/PA, SLG, OBP, OBS, est. wOBA, est. SLG, platoon splits (HR/PA vs RHP/LHP)
- Pitchers: HR/BF, FB%

---

## Snowflake Schema

### BASEBALL.STATCAST (current — Statcast era)

| Table | Description |
|-------|-------------|
| `RAW_BATTER_GAMES` | One row per batter per game — raw counting stats |
| `RAW_PITCHER_GAMES` | One row per pitcher per game — raw counting stats |
| `GAME_RESULTS` | Game outcomes — scores, home victory, run diff |
| `BATTER_ROLLING_FEATURES` | Pre-computed rolling features per batter |
| `PITCHER_ROLLING_FEATURES` | Pre-computed rolling features per pitcher |

### BASEBALL.HISTORICAL (legacy — Retrosheet era)

| Table | Description |
|-------|-------------|
| `RETROSHEET_EVENTS` | 98,000+ games (1980-2025) with team rolling stats |
| `TEAM_BATTING_LOGS` | Team batting game logs (Baseball Reference) |
| `TEAM_PITCHING_LOGS` | Team pitching game logs (Baseball Reference) |

---

## Setup

### Prerequisites
- Docker + Docker Compose (for local Airflow)
- Snowflake account
- AWS EC2 instance (self-hosted GitHub Actions runner)

### 1. Clone and configure

```bash
git clone https://github.com/SJTreadway/mlb-pipeline.git
cd mlb-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 2. Set up Snowflake tables

Run in your Snowflake worksheet:

```bash
include/sql/setup_snowflake_statcast.sql
include/sql/setup_snowflake_team_game_logs.sql
include/sql/setup_snowflake_historical_retrosheet_events.sql
```

### 3. Add GitHub Secrets

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PRIVATE_KEY    # RSA key pair authentication
SNOWFLAKE_DATABASE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_ROLE
YEAR
```

### 4. Set up self-hosted runner (EC2)

```bash
# on EC2 instance
mkdir actions-runner && cd actions-runner
# follow GitHub repo → Settings → Actions → Runners → New self-hosted runner
./config.sh --url https://github.com/SJTreadway/mlb-pipeline --token YOUR_TOKEN
sudo ./svc.sh install
sudo ./svc.sh start
```

Whitelist your EC2 static IP in your Snowflake network policy.

### 5. Local Airflow (optional)

```bash
docker compose up airflow-init
docker compose up -d
# Airflow UI → http://localhost:8080
```

---

## Historical Backfill

To backfill Statcast data for 2015-2026, run on EC2 inside a tmux session:

```bash
tmux new -s backfill
python3.11 jobs/backfill_statcast.py
# detach: Ctrl+B D
# reattach: tmux attach -t backfill
```

Checkpoints every 50 players and after each year — safe to interrupt and resume.

---

## Querying the Data

```sql
-- recent batter rolling features
SELECT mlbam_id, game_date, barrel_162, ev_162, hr_per_pa_162
FROM BASEBALL.STATCAST.BATTER_ROLLING_FEATURES
WHERE game_date >= CURRENT_DATE - 7
ORDER BY hr_per_pa_162 DESC
LIMIT 20;

-- pitcher HR/BF over last 10 starts
SELECT mlbam_id, game_date, hr_per_bf_10, fb_perc_10
FROM BASEBALL.STATCAST.PITCHER_ROLLING_FEATURES
WHERE game_date = CURRENT_DATE - 1
ORDER BY hr_per_bf_10 DESC;

-- recent game results
SELECT team_h, team_v, runs_h, runs_v, home_victory
FROM BASEBALL.STATCAST.GAME_RESULTS
WHERE game_date >= CURRENT_DATE - 7
ORDER BY game_date DESC;

-- historical team OBP impact on win rate
SELECT
    CASE WHEN OBP_30_H >= 0.340 THEN 'Hot' ELSE 'Cold' END as home_obp_status,
    COUNT(*) as games,
    ROUND(AVG(HOME_VICTORY) * 100, 1) as home_win_pct
FROM BASEBALL.HISTORICAL.RETROSHEET_EVENTS
WHERE SEASON >= 2015
GROUP BY 1;
```

---

## Authentication

Uses RSA key pair authentication for Snowflake — no MFA bypass required. Generate keys:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Assign public key to your Snowflake user:
```sql
ALTER USER your_username SET RSA_PUBLIC_KEY='<contents of rsa_key.pub>';
```

Store private key as `SNOWFLAKE_PRIVATE_KEY` GitHub secret.

---

## License

MIT License — see [LICENSE](LICENSE)

---

*Part of the [MoneyballVo](https://x.com/MoneyballVo) MLB analytics project.*

