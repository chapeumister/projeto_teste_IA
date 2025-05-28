# Comprehensive Sports Data Collection System

## Overview

This project aims to collect historical and real-time sports data from a variety of sources into a centralized SQLite database. The collected data is structured to be suitable for sports analytics, building predictive models, and general AI model training purposes. It emphasizes robust data fetching, idempotent data storage, and configurable data source management.

## Features

*   **Multiple Data Sources**:
    *   Kaggle Datasets (e.g., International Football Results)
    *   Soccer-Data.co.uk (Historical CSVs for various leagues)
    *   FiveThirtyEight (e.g., Soccer SPI ratings and predictions)
    *   Football-Data.org API (Historical and daily match data)
    *   OpenFootball (football.json - various leagues and competitions)
    *   TheSportsDB API (Events and results for multiple sports)
    *   API-Sports (via Football-Data.org or direct for daily match data)
*   **Centralized SQLite Database**: All data is stored in a local SQLite database (`sports_data.sqlite`).
*   **Automated Data Updates**: The `update_data_all.py` script provides a centralized way to run selected or all data collectors.
*   **Configurable Data Sources**: Data collection parameters (leagues, seasons, datasets, API settings) are managed via `config.yaml`.
*   **Resilient Data Fetching**: Implements retry mechanisms (using `tenacity`) for API calls and robust error handling.
*   **Rate Limiting**: Respects API rate limits using the `ratelimiter` library for relevant sources.
*   **Idempotent Data Storage**: Uses UPSERT (Update or Insert) logic to avoid duplicate entries and ensure data consistency when scripts are re-run.
*   **Column Name Normalization**: DataFrames from CSVs have their column names standardized to snake_case for consistency.
*   **Logging**: Comprehensive logging for data collection processes, stored in rotating log files.

## Project Structure

```
sports_prediction_ai/
├── config.yaml
├── README.md
├── requirements.txt
├── database/
│   └── sports_data.sqlite  # Created after running database_setup.py
├── logs/
│   └── data_update.log     # Created after running update_data_all.py
├── data/                   # (Example, might contain mock data or other resources)
│   └── mock_matches.json
├── notebooks/              # (Example, for experimental work)
│   └── 01_data_collection_and_preprocessing.ipynb
├── src/
│   ├── __init__.py
│   ├── database_setup.py
│   ├── utils.py
│   ├── data_collection.py  # Daily data from football-data.org & API-Sports
│   ├── collect_soccer_data_co_uk.py
│   ├── collect_fivethirtyeight.py
│   ├── collect_openfootball.py
│   ├── collect_football_data_org_history.py
│   ├── collect_kaggle.py
│   ├── connector_thesportsdb.py
│   └── update_data_all.py  # Main orchestrator script
└── tests/                  # (Example, for unit/integration tests)
    └── __init__.py
```

## Setup and Installation

1.  **Clone Repository**:
    ```bash
    git clone <repository_url>
    cd sports_prediction_ai
    ```

2.  **Create Virtual Environment and Activate**:
    ```bash
    python -m venv venv
    # On Windows
    # venv\Scripts\activate
    # On macOS/Linux
    # source venv/bin/activate
    ```

3.  **Install Dependencies**:
    ```bash
    pip install -r sports_prediction_ai/requirements.txt
    ```

4.  **API Key Configuration**:
    Several data sources require API keys. These should be set as environment variables:
    *   `FOOTBALL_DATA_API_KEY`: For `football-data.org`.
    *   `APISPORTS_API_KEY`: For `api-sports.io` (used by `data_collection.py`).
    *   `THESPORTSDB_API_KEY`: For `thesportsdb.com`.

    For Kaggle data:
    *   Download your `kaggle.json` API token from your Kaggle account page (`Account` -> `API` -> `Create New API Token`).
    *   Place it in the default Kaggle configuration directory: `~/.kaggle/kaggle.json`.
    *   Alternatively, you can set the `KAGGLE_USERNAME` and `KAGGLE_KEY` environment variables, or point the `KAGGLE_CONFIG_PATH` environment variable to your `kaggle.json` file's directory.

5.  **Data Source Configuration (`config.yaml`)**:
    *   The main configuration file for data collectors is `sports_prediction_ai/config.yaml`.
    *   This file allows you to specify:
        *   API base URLs.
        *   Rate limiting parameters (requests per minute for different sources).
        *   Leagues, seasons, and specific datasets to download for each collector script.
    *   Review the comments within `config.yaml` for detailed settings and how to customize which data is fetched.

6.  **Initialize Database**:
    Run the database setup script to create the necessary tables and indexes:
    ```bash
    python sports_prediction_ai/src/database_setup.py
    ```
    This will create the `sports_prediction_ai/database/sports_data.sqlite` file.

## Running Data Collection

The primary way to collect and update data is through the `update_data_all.py` script.

*   **To run all configured data collectors**:
    ```bash
    python sports_prediction_ai/src/update_data_all.py
    ```

*   **To run specific collectors**:
    Use the `--sources` argument with a comma-separated list of source names.
    ```bash
    python sports_prediction_ai/src/update_data_all.py --sources soccer_data_co_uk,fivethirtyeight
    ```
    Available source names are defined in `update_data_all.py` (e.g., `daily_live`, `soccer_data_co_uk`, `fivethirtyeight`, `openfootball`, `fd_org_history`, `kaggle_intl`, `thesportsdb`).

*   **Full Historical Refresh**:
    For collectors that support it (primarily historical data collectors), you can request a full refresh:
    ```bash
    python sports_prediction_ai/src/update_data_all.py --sources fd_org_history --full
    ```
    This typically re-fetches all available data for the configured seasons/leagues. For daily/current data sources, `--full` might fetch a broader range of recent data if applicable.

*   **Incremental Updates (Since Date)**:
    Some collectors might support fetching data since a specific date:
    ```bash
    python sports_prediction_ai/src/update_data_all.py --sources daily_live --since YYYY-MM-DD
    ```

*   **Individual Scripts**:
    While `update_data_all.py` is recommended, individual collector scripts located in `sports_prediction_ai/src/` can be run directly for debugging or very specific updates. However, they might not all support the same command-line arguments as the main orchestrator.

## Logging

*   The `update_data_all.py` script and individual collectors (when run via the orchestrator or if they implement similar logging) generate logs.
*   Logs are stored in the `sports_prediction_ai/logs/` directory. The main log file is `data_update.log`.
*   Log files are rotated to manage size (10MB per file, 3 backup files).

## Database Schema Overview

The SQLite database (`sports_data.sqlite`) contains the following main tables:

*   **`leagues`**: Stores information about different sports leagues.
    *   `league_id` (INTEGER, PK): Internal unique ID.
    *   `name` (TEXT): Name of the league (e.g., "Premier League").
    *   `sport` (TEXT): Sport type (e.g., "football").
    *   `country` (TEXT): Country of the league.
    *   `source_league_id` (TEXT): League ID from the original data source.
    *   `UNIQUE (name, sport)`

*   **`teams`**: Stores information about teams.
    *   `team_id` (INTEGER, PK): Internal unique ID.
    *   `name` (TEXT): Name of the team.
    *   `sport` (TEXT): Sport type.
    *   `country` (TEXT): Country of the team.
    *   `source_team_id` (TEXT): Team ID from the original data source.
    *   `UNIQUE (name, sport)`

*   **`matches`**: Stores details for individual matches.
    *   `match_id` (INTEGER, PK): Internal unique ID.
    *   `league_id` (INTEGER, FK): Links to `leagues.league_id`.
    *   `home_team_id` (INTEGER, FK): Links to `teams.team_id`.
    *   `away_team_id` (INTEGER, FK): Links to `teams.team_id`.
    *   `match_datetime_utc` (TEXT): Match start time in UTC (ISO 8601 format).
    *   `status` (TEXT): Match status (e.g., "SCHEDULED", "FINISHED", "LIVE").
    *   `home_score` (INTEGER): Full-time home team score.
    *   `away_score` (INTEGER): Full-time away team score.
    *   `winner` (TEXT): Winner of the match ("HOME_TEAM", "AWAY_TEAM", "DRAW").
    *   `stage` (TEXT): Competition stage (e.g., "REGULAR_SEASON", "FINAL", "Group A").
    *   `matchday` (INTEGER): Matchday or round number.
    *   `source_match_id` (TEXT): Match ID from the original data source.
    *   `source_name` (TEXT): Identifier for the data source (e.g., "football-data.org").
    *   `is_mock` (BOOLEAN): Indicates if the data is from a mock source (DEFAULT 0).
    *   `UNIQUE (source_match_id, source_name)`
    *   Indexes on: `match_datetime_utc`, `home_team_id`, `away_team_id`.

*   **`odds`**: Stores betting odds for matches.
    *   `odd_id` (INTEGER, PK): Internal unique ID.
    *   `match_id` (INTEGER, FK): Links to `matches.match_id`.
    *   `bookmaker` (TEXT): Name of the bookmaker.
    *   `market_type` (TEXT): Type of market (e.g., "1X2", "TOTAL_GOALS_OVER_2.5").
    *   `home_odds`, `draw_odds`, `away_odds` (REAL): Odds values.
    *   `timestamp_utc` (TEXT): Timestamp of when the odds were recorded.
    *   `UNIQUE (match_id, bookmaker, market_type)`

*   **`stats`**: Stores various statistics for matches, teams, or players.
    *   `stat_id` (INTEGER, PK): Internal unique ID.
    *   `match_id` (INTEGER, FK): Links to `matches.match_id`.
    *   `team_id` (INTEGER, FK, Optional): Links to `teams.team_id`.
    *   `player_id` (INTEGER, FK, Optional): Links to a future `players` table.
    *   `stat_type` (TEXT): Type of statistic (e.g., "shots_total", "spi_rating").
    *   `stat_value` (TEXT): Value of the statistic.
    *   `period` (TEXT, Optional): Game period for the stat (e.g., "FULL_TIME", "1ST_HALF").
    *   `UNIQUE (match_id, team_id, stat_type, period)`

## Data Collector Scripts (`sports_prediction_ai/src/`)

*   `data_collection.py`: Fetches daily match data from Football-Data.org and API-Sports.
*   `collect_soccer_data_co_uk.py`: Collects historical football data from CSVs provided by soccer-data.co.uk.
*   `collect_fivethirtyeight.py`: Downloads and processes datasets from FiveThirtyEight (e.g., Soccer SPI).
*   `collect_openfootball.py`: Processes data from football.json collections (various leagues).
*   `collect_football_data_org_history.py`: Fetches historical match data for specified competitions and seasons from Football-Data.org.
*   `collect_kaggle.py`: Downloads and processes datasets from Kaggle (e.g., historical international results).
*   `connector_thesportsdb.py`: Fetches current and past events for various sports leagues from TheSportsDB API.

## Future Enhancements

*   Add more data sources and sports.
*   Implement more detailed player-level statistics.
*   Integrate with a data visualization dashboard.
*   Develop and train predictive models using the collected data.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details (if one is added).
