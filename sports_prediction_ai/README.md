# Sports Prediction AI - Data Pipeline

## General Project Description

The primary objective of this project is to establish a robust and automated data pipeline for collecting, storing, and preprocessing sports data. This pipeline integrates both historical data from various offline sources and real-time data via APIs. The ultimate goal is to provide clean, structured, and feature-rich datasets suitable for training machine learning models aimed at sports prediction tasks, such as forecasting match outcomes.

## Quick Start / TL;DR

For those familiar with Python development environments:

```bash
# Ensure Python 3.11/3.12 and Git are installed
git clone <repository_url>
cd <repository_directory_name> # e.g., sports_prediction_ai_project
python -m venv .venv
# Activate environment (Windows CMD/PowerShell: .venv\\Scripts\\activate | Git Bash/Linux/macOS: source .venv/bin/activate)
python -m pip install --upgrade pip
pip install -r requirements.txt
# Create .env file with API keys (see "Configuration of Environment Variables" section)
python sports_prediction_ai/src/database_setup.py
# Run desired downloaders and importer (see "How to Execute the Scripts" section)
# Run tests: pytest
```
(Replace `<repository_url>` and `<repository_directory_name>` as appropriate.)

## Prerequisites and Installation (Windows)

This guide assumes you are working on a Windows environment.

### Prerequisites

*   **Supported Windows Version:** Windows 10 or later is recommended.
*   **Python:** Python 3.11 or 3.12. Ensure Python is added to your PATH during installation. You can download Python from [python.org](https://www.python.org/).
*   **Git:** Required for cloning the repository. You can download Git from [git-scm.com](https://git-scm.com/).
*   **Visual Studio Build Tools (Optional but Recommended):** Some Python packages might have C/C++ dependencies that require compilation. Installing the Visual Studio Build Tools (available from the Visual Studio website, ensure "C++ build tools" workload is selected) can prevent installation issues for such packages.

### Installation Steps

1.  **Clone the Repository:**
    Open Git Bash or Command Prompt and run:
    ```bash
    git clone <repository_url>
    cd <repository_directory_name>
    ```
    (Replace `<repository_url>` and `<repository_directory_name>` with the actual URL and directory name.)

2.  **Create and Activate a Virtual Environment:**
    It's highly recommended to use a virtual environment to manage project dependencies.
    ```bash
    # Navigate to the project root directory (e.g., sports_prediction_ai)
    python -m venv .venv
    ```
    To activate the virtual environment:
    ```bash
    # In Command Prompt or PowerShell
    .venv\\Scripts\\activate
    ```
    Or if using Git Bash:
    ```bash
    source .venv/Scripts/activate
    ```
    For Linux/macOS, the activation command is typically:
    ```bash
    source .venv/bin/activate
    ```
    You should see `(.venv)` prefixed to your command prompt line.

3.  **Upgrade Pip:**
    Ensure you have the latest version of pip:
    ```bash
    python -m pip install --upgrade pip
    ```

4.  **Install Requirements:**
    Install all necessary Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration of Environment Variables

This project requires API keys for several external data sources. These keys should be stored in a `.env` file located in the root directory of the project (i.e., in the same directory as the `sports_prediction_ai` folder, not inside it).

**1. Create the `.env` file:**
   Create a file named `.env` in the project's root directory.

**2. Add API Keys:**
   The `.env` file should follow this format (replace `your_actual_key_here` with your real API keys):

   ```ini
   KAGGLE_USERNAME=your_kaggle_username
   KAGGLE_KEY=your_kaggle_api_key

   FOOTBALL_DATA_API_KEY=your_football_data_org_api_key

   APISPORTS_API_KEY=your_api-sports_api_key
   # (Note: API-SPORTS is also known as api-football.com)

   THESPORTSDB_API_KEY=your_thesportsdb_api_key
   # (TheSportsDB provides a free key "1" with limitations, but a personal key is recommended for more extensive use)
   ```

**Explanation of Keys:**

*   **`KAGGLE_USERNAME` / `KAGGLE_KEY`:**
    *   Your Kaggle account username and API key.
    *   To get your Kaggle API key, go to your Kaggle account page, and click "Create New API Token". This will download a `kaggle.json` file. Your username is your Kaggle username, and the key is the value associated with the "key" field in `kaggle.json`.
*   **`FOOTBALL_DATA_API_KEY`:**
    *   API key for [football-data.org](https://www.football-data.org/). You'll need to register on their website to get a key.
    *   Note: Free tier keys for football-data.org are often limited (e.g., to about 10 requests per minute). For extensive historical data fetching, you might encounter these limits.
*   **`APISPORTS_API_KEY`:**
    *   API key for [API-SPORTS (api-football.com)](https://www.api-football.com/). Register on their website to obtain a key. The project uses this for some data collection tasks.
    *   Note: The free tier for API-SPORTS may have limitations, such as not providing live odds or access to all historical data endpoints.
*   **`THESPORTSDB_API_KEY`:**
    *   API key for [TheSportsDB.com](https://www.thesportsdb.com/api.php). A key "1" is available for free public use but has limitations (e.g., rate limits, potentially less data). It's recommended to register for a personal key if you plan more extensive use.

The Python scripts are configured to load these variables from the `.env` file using a library like `python-dotenv` (which should be listed in `requirements.txt`). Ensure the `.env` file is correctly formatted and placed in the root directory.

## Database

The pipeline uses an SQLite database by default to store all collected and processed sports data.

### Default Configuration

*   **Database Type:** SQLite
*   **Location:** The database file is named `sports_data.db` and is located in the `sports_prediction_ai/data/` directory by default.
*   **Schema Creation:** To create the database and all necessary tables with the correct schema, run the setup script from the project root directory:
    ```bash
    python sports_prediction_ai/src/database_setup.py
    ```
    This command will initialize the `sports_data.db` file with the required table structures if it doesn't already exist. It's safe to run this command multiple times; it will not recreate tables that are already present. For connecting to the database in your own scripts, you will typically use `sqlite3.connect('sports_prediction_ai/data/sports_data.db')`.

### Using Other Database Systems (e.g., PostgreSQL, MySQL)

To change the default SQLite database file path (`sports_prediction_ai/data/sports_data.db`), you would need to update the path definition in several scripts:
*   The `create_database` function in `sports_prediction_ai/src/database_setup.py` defines the default path when creating the database.
*   Scripts like `sports_prediction_ai/src/database_importer.py` and `sports_prediction_ai/src/data_preprocessing.py` often define a `DB_PATH` variable pointing to this default location for their connection logic (as seen in the example connection snippets).
Consolidating the database path into a single, shared configuration variable or module would be a good practice for easier modification.

While the project is configured to use SQLite out-of-the-box for ease of setup, the SQL queries used are generally standard. Adapting the pipeline for PostgreSQL or MySQL would involve the following conceptual steps (note: specific connector libraries and DDL adjustments would be required):

1.  **Install appropriate Python database connector:**
    *   For PostgreSQL: `psycopg2-binary`
    *   For MySQL: `mysql-connector-python`
2.  **Modify Database Connection Logic:**
    *   Update `sports_prediction_ai/src/database_setup.py` to connect to your chosen database server and create the schema using its specific DDL syntax.
    *   Update any scripts that directly connect to the database to use the new database connector and connection parameters.
3.  **Adjust SQL Syntax:** Specific features like SQLite's `ON CONFLICT` clauses may need translation to their equivalents in PostgreSQL or MySQL.

For now, SQLite is the directly supported and tested database.

## How to Execute the Scripts

Ensure your virtual environment is activated and the `.env` file is correctly configured with your API keys before running any scripts. All commands should generally be run from the root directory of the project.

### 1. Setup the Database Schema

First, ensure the database schema is created:
```bash
python sports_prediction_ai/src/database_setup.py
```

### 2. Historical Data Collection & Importation

This involves running individual downloader scripts first, then running the main database importer script.

**a. Download Raw Data from Sources:**

*   **Kaggle Datasets:**
    ```bash
    python sports_prediction_ai/src/kaggle_downloader.py
    ```
    This script downloads datasets into `sports_prediction_ai/data/kaggle_datasets/`. The exact subfolder depends on the dataset slug.
*   **Soccer-Data.co.uk CSVs:**
    ```bash
    python sports_prediction_ai/src/soccer_data_downloader.py
    ```
    This downloads CSVs into `sports_prediction_ai/data/soccer_data_co_uk/`.
*   **FiveThirtyEight Data Repository:**
    ```bash
    python sports_prediction_ai/src/fivethirtyeight_downloader.py
    ```
    This clones/updates data into `sports_prediction_ai/data/fivethirtyeight_data/`.
*   **OpenFootball Data Repositories:**
    ```bash
    python sports_prediction_ai/src/openfootball_downloader.py
    ```
    This clones repositories into `sports_prediction_ai/data/openfootball_data/`.
*   **Football-Data.org (Historical Seasons):**
    ```bash
    python sports_prediction_ai/src/data_collection.py
    ```
    (The main execution block in this script demonstrates fetching historical data from football-data.org among others). When fetching historical data, this script saves JSON files into `sports_prediction_ai/data/football_data_org_historical/` (e.g., `football_data_org_historical/PL/2022/matches.json`).

**b. Import All Downloaded Data into the Database:**

**Important:** Ensure you have run the relevant downloader scripts from step '2.a' *before* executing the importer. The `database_importer.py` script will attempt to find data in the local paths populated by those downloaders. If data for a specific source is missing, that part of the import will be skipped.
```bash
python sports_prediction_ai/src/database_importer.py
```
This script processes data from the download locations and loads it into the SQLite database. Its main execution block also includes a demo for fetching and importing from TheSportsDB.

### 3. Real-Time Data Collection (TheSportsDB)

The `sports_prediction_ai/src/database_importer.py` script, when run directly, includes a demonstration of fetching and importing data from TheSportsDB. For more customized real-time collection:

*   Utilize functions like `search_league_thesportsdb`, `get_future_events_thesportsdb`, and `get_event_details_thesportsdb` from `sports_prediction_ai/src/data_collection.py`.
*   Pass the retrieved data to `import_thesportsdb_leagues` and `import_thesportsdb_events` from `sports_prediction_ai/src/database_importer.py`.

*Example conceptual workflow in a custom Python script:*
```python
import sqlite3
import os # For path construction
from sports_prediction_ai.src import data_collection, database_importer

# Define path to the database
# Assuming script is run from project root
db_path = os.path.join("sports_prediction_ai", "data", "sports_data.db")

# 1. Get DB connection
db_conn = None
try:
    db_conn = sqlite3.connect(db_path)
    print(f"Successfully connected to database at {db_path}")
except sqlite3.Error as e:
    print(f"Error connecting to database at {db_path}: {e}")
    exit() # Or handle error appropriately

# Example: Fetch and import events for a known league ID
epl_id = "4328" # English Premier League
print(f"Fetching events for league ID: {epl_id}")
events = data_collection.get_future_events_thesportsdb(league_id=epl_id)
if events:
    print(f"Found {len(events)} events. Importing into database...")
    database_importer.import_thesportsdb_events(db_conn, events, default_league_name="English Premier League") # Corrected argument name
    print("Import process complete.")
else:
    print("No events found to import.")

if db_conn:
    db_conn.close()
```

### 4. ML Data Preprocessing

To generate a DataFrame suitable for machine learning, call the `get_ml_ready_dataframe_from_db` function from `sports_prediction_ai/src/data_preprocessing.py`. This is typically done within a Jupyter notebook or a custom Python script.

*Example Python snippet:*
```python
import sqlite3
import os # For path construction
from sports_prediction_ai.src.data_preprocessing import get_ml_ready_dataframe_from_db

# Define path to the database
db_path = os.path.join("sports_prediction_ai", "data", "sports_data.db")

db_conn = None
try:
    db_conn = sqlite3.connect(db_path)
    print(f"Successfully connected to database at {db_path}")
except sqlite3.Error as e:
    print(f"Error connecting to database at {db_path}: {e}")
    exit() # Or handle error appropriately

ml_df_filtered = get_ml_ready_dataframe_from_db(
    db_conn,
    league_names=["Premier League", "Bundesliga"], # Example
    date_from="2022-08-01",
    date_to="2023-05-31"
)
if not ml_df_filtered.empty:
    print(ml_df_filtered.info())

if db_conn:
    db_conn.close()
```

## Test Suite (pytest)

The project includes a comprehensive test suite using `pytest`. Ensure development dependencies are installed. Tests are in `sports_prediction_ai/tests/`.

### Running Tests

All test commands should be run from the **root directory** of the project.

**1. Running All Tests with Coverage:**
To run all tests, generate coverage reports, and fail if coverage is below 85%:
```bash
pytest
```
This uses settings from `pytest.ini`. View the HTML report in `htmlcov/index.html`.
(This 85% threshold can be adjusted by changing the `--cov-fail-under` value in the `addopts` line within the `pytest.ini` file.)

**2. Running Specific Categories of Tests:**
*   **All tests *except* `slow` ones:**
    ```bash
    pytest -m "not slow"
    ```
*   **Only `slow` tests:**
    ```bash
    pytest -m slow
    ```
*   **Running only integration tests (by path):**
    ```bash
    pytest sports_prediction_ai/tests/integration/
    ```
    (Optionally, you can add: "If you've defined an 'integration' marker for these tests in `pytest.ini` and applied it, you could also use `pytest -m integration`.")
*   **By path (e.g., unit tests):**
    ```bash
    pytest sports_prediction_ai/tests/unit/
    ```

### Note on Test Environment
*   A dummy `kaggle.json` file and its configuration directory are programmatically created by the test setup (in `conftest.py`) specifically for the test session. This is to prevent the Kaggle library from raising errors during test collection if actual Kaggle API credentials are not configured locally. This dummy file is temporary and should not be committed.

## Database Schema Summary

Key tables in `sports_data.db`:

*   **`leagues`**: Stores league information.
    *   Constraint: `UNIQUE (name, sport, source)`
*   **`teams`**: Stores team information.
    *   Constraint: `name` is UNIQUE.
*   **`matches`**: Stores individual match details.
    *   Constraint: `UNIQUE (datetime, home_team_id, away_team_id, source)`
*   **`stats`**: Stores various match-related statistics (e.g., expected goals).
*   **`odds`**: Stores betting odds for matches.

(Refer to `sports_prediction_ai/src/database_setup.py` for full schema details.)

## Known Issues / Limitations

*   **API Rate Limits:** Free tiers of APIs (especially TheSportsDB) are rate-limited. Paid keys are recommended for extensive use.
*   **TheSportsDB Importer Counters:** The `added_count` vs. `updated_count` for TheSportsDB events in `database_importer.py` might not precisely distinguish new rows from updated rows due to the UPSERT logic.
*   **Kaggle Test Setup:** Tests use a dummy `kaggle.json` created programmatically. Real credentials in `.env` are needed for actual data collection.
*   **Data Consistency:** Team/league names may vary slightly across sources.
*   **Historical Data Granularity:** Varies by source; detailed stats are not universally available.
*   **CLI Arguments:** Scripts currently lack extensive command-line arguments for fine-grained control.

## License

This project is currently licensed under the [Specify License, e.g., MIT License]. Please see the `LICENSE` file for more details (if one exists, or to be created).

## Contributing

Contributions are welcome! Please refer to `CONTRIBUTING.md` for guidelines on how to contribute to this project (this file may need to be created with contribution guidelines).
```
