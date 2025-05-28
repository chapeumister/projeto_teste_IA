import sqlite3
import pandas as pd
import os
# import zipfile # zipfile is implicitly used by kaggle.api.dataset_download_files with unzip=True
import io
import shutil 
import kaggle 
import yaml 
import requests # For tenacity on potential underlying requests issues if KaggleApiError is not enough
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type # For tenacity
from kaggle.api.kaggle_api_extended import KaggleApiError # Specific Kaggle API error

# Assuming database_setup.py is in the same directory or accessible via python path
try:
    from database_setup import DATABASE_FILE, create_connection
except ImportError:
    # Fallback if running script directly and database_setup is one level up
    from sports_prediction_ai.src.database_setup import DATABASE_FILE, create_connection

# Attempt to import _get_or_create_entity_id, can be copied if not found
try:
    from data_collection import _get_or_create_entity_id
except ImportError:
    # Fallback _get_or_create_entity_id if data_collection.py is not in the same path
    def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
        if pd.isna(name) or not name or not sport:
            print(f"Warning (fallback _get_or_create_entity_id): Name or sport is missing or invalid for {entity_type} (Name: {name}, Sport: {sport}). Skipping.")
            return None
        cursor = conn.cursor()
        table_name = "leagues" if entity_type == "league" else "teams"
        id_column = "league_id" if entity_type == "league" else "team_id"
        source_id_column = "source_league_id" if entity_type == "league" else "source_team_id"
        try:
            cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
            row = cursor.fetchone()
            if row:
                entity_id = row[0]
                if source_id:
                    cursor.execute(f"SELECT {source_id_column} FROM {table_name} WHERE {id_column} = ?", (entity_id,))
                    current_source_id_row = cursor.fetchone()
                    if current_source_id_row and current_source_id_row[0] is None:
                        cursor.execute(f"UPDATE {table_name} SET {source_id_column} = ? WHERE {id_column} = ?", (source_id, entity_id))
                conn.commit()
                return entity_id
            else:
                if entity_type == "league":
                    sql = f"INSERT INTO {table_name} (name, sport, country, {source_id_column}) VALUES (?, ?, ?, ?)"
                    params = (name, sport, country, source_id)
                else: 
                    sql = f"INSERT INTO {table_name} (name, sport, {source_id_column}) VALUES (?, ?, ?)"
                    params = (name, sport, source_id)
                cursor.execute(sql, params)
                conn.commit()
                return cursor.lastrowid
        except sqlite3.IntegrityError:
            cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
            row = cursor.fetchone()
            if row: return row[0]
            else: return None
        except sqlite3.Error as e:
            print(f"Database error in fallback _get_or_create_entity_id for {entity_type} '{name}': {e}")
            conn.rollback() # Rollback on error
            return None

# Global config variable
CONFIG = {}

def load_config(config_path='sports_prediction_ai/config.yaml'):
    """Loads the YAML configuration file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir) 
    actual_config_path = os.path.join(project_root, 'config.yaml')

    if not os.path.exists(actual_config_path):
        actual_config_path = 'config.yaml' 
        if not os.path.exists(actual_config_path):
             print(f"ERROR: Configuration file not found at {os.path.join(project_root, 'config.yaml')} or {actual_config_path}")
             return None
    try:
        with open(actual_config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        print(f"INFO: Configuration loaded successfully from {actual_config_path}")
        return config_data
    except Exception as e:
        print(f"ERROR: Failed to load or parse YAML configuration from {actual_config_path}: {e}")
        return None

KAGGLE_CONFIG_DIR = os.path.expanduser("~/.kaggle/")
KAGGLE_JSON_PATH = os.path.join(KAGGLE_CONFIG_DIR, "kaggle.json")
KAGGLE_JSON_ENV_VAR = "KAGGLE_CONFIG_PATH" # Custom env var to point to kaggle.json

# KAGGLE_DATASETS_TO_DOWNLOAD will be loaded from CONFIG in main()

def setup_kaggle_api():
    """
    Sets up the Kaggle API. Checks for kaggle.json or KAGGLE_CONFIG_PATH env var.
    Authenticates the Kaggle API client.
    """
    custom_path = os.getenv(KAGGLE_JSON_ENV_VAR)
    
    if custom_path and os.path.isfile(custom_path):
        print(f"INFO: Using Kaggle configuration from KAGGLE_CONFIG_PATH: {custom_path}")
        # Kaggle API typically loads from default or env vars KAGGLE_USERNAME, KAGGLE_KEY
        # If KAGGLE_CONFIG_PATH points to a full kaggle.json, we might need to set env vars from it
        # For simplicity, assume if KAGGLE_CONFIG_PATH is set, it points to a directory like ~/.kaggle
        # and kaggle.json is inside it, or KAGGLE_USERNAME/KAGGLE_KEY are set.
        # The kaggle library handles KAGGLE_CONFIG_DIR environment variable by default.
        # So, if KAGGLE_CONFIG_PATH is a directory, set KAGGLE_CONFIG_DIR.
        if os.path.isdir(custom_path):
            os.environ['KAGGLE_CONFIG_DIR'] = custom_path
        elif os.path.isfile(custom_path) and custom_path.endswith("kaggle.json"):
             # if it's a direct path to kaggle.json, set KAGGLE_CONFIG_DIR to its directory
            os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(custom_path)

    elif not os.path.isfile(KAGGLE_JSON_PATH):
        print("ERROR: Kaggle API configuration file 'kaggle.json' not found.")
        print(f"Please place your kaggle.json (downloaded from your Kaggle account page > API section) at '{KAGGLE_JSON_PATH}'")
        print(f"Alternatively, set the KAGGLE_USERNAME and KAGGLE_KEY environment variables,")
        print(f"or point the '{KAGGLE_JSON_ENV_VAR}' environment variable to your kaggle.json file's directory.")
        return False
    
    try:
        kaggle.api.authenticate()
        print("INFO: Kaggle API authenticated successfully.")
        return True
    except Exception as e:
        print(f"ERROR: Kaggle API authentication failed: {e}")
        print("Please ensure your kaggle.json is correctly configured or KAGGLE_USERNAME/KAGGLE_KEY environment variables are set.")
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30), # Slightly longer waits for Kaggle
    retry=retry_if_exception_type((KaggleApiError, requests.exceptions.RequestException, Exception)) # Broad catch for Kaggle
)
def download_kaggle_dataset(dataset_id, download_path="temp_kaggle_data"):
    """
    Downloads a dataset from Kaggle using its identifier, with retries.
    Returns the path where files were downloaded, or None if error.
    """
    print(f"INFO: Attempting to download dataset '{dataset_id}' to '{download_path}'...")
    # Ensure the directory exists and is empty for a clean download attempt
    if os.path.exists(download_path):
        shutil.rmtree(download_path) # Remove if exists to avoid issues with partial downloads
    os.makedirs(download_path, exist_ok=True)
    
    kaggle.api.dataset_download_files(dataset_id, path=download_path, unzip=True, quiet=False)
    print(f"INFO: Dataset '{dataset_id}' downloaded and unzipped to '{download_path}'.")
    return download_path
    # No explicit return None on exception, tenacity handles retry or re-raises

def parse_martj42_intl_football_results(conn, file_path, sport, default_league_name):
    """
    Parses data from martj42/international-football-results-from-1872-to-2017 (results.csv).
    Returns tuple: (inserted_matches, updated_matches)
    """
    if not os.path.exists(file_path):
        print(f"ERROR: File not found for parsing: {file_path}")
        return 0, 0
        
    inserted_matches, updated_matches = 0, 0
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"ERROR: Failed to read or parse CSV {file_path}: {e}")
        return 0

    cursor = conn.cursor()
    
    # League - common for all matches in this dataset
    # Country for "International Matches" league can be None or a placeholder like "International"
    league_id = _get_or_create_entity_id(conn, "league", default_league_name, sport, country="International")
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {default_league_name}. Skipping this dataset.")
        return 0

    for index, row in df.iterrows():
        try:
            home_team_name = row.get('home_team')
            away_team_name = row.get('away_team')

            if pd.isna(home_team_name) or pd.isna(away_team_name):
                print(f"WARNING: Skipping row {index+2} due to missing HomeTeam or AwayTeam name.")
                continue

            home_team_id = _get_or_create_entity_id(conn, "team", home_team_name, sport)
            away_team_id = _get_or_create_entity_id(conn, "team", away_team_name, sport)

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match: {home_team_name} vs {away_team_name}. Skipping row {index+2}.")
                continue

            raw_date = row.get('date')
            if pd.isna(raw_date):
                print(f"WARNING: Skipping row {index+2} due to missing Date.")
                continue
            try:
                match_datetime = pd.to_datetime(raw_date)
                match_datetime_utc_str = match_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"WARNING: Could not parse date '{raw_date}' for row {index+2}: {e}. Skipping match.")
                continue
            
            home_score = row.get('home_score')
            away_score = row.get('away_score')
            winner = None
            status = "FINISHED" # All matches in this dataset are historical

            if pd.notna(home_score) and pd.notna(away_score):
                home_score = int(home_score)
                away_score = int(away_score)
                if home_score > away_score: winner = 'HOME_TEAM'
                elif away_score > home_score: winner = 'AWAY_TEAM'
                else: winner = 'DRAW'
            else: # Should not happen for this dataset, but good practice
                home_score, away_score = None, None
                status = "UNKNOWN_SCORE"


            # source_match_id: Using row index as a proxy for uniqueness within the dataset import
            source_match_id = f"kgl_martj42_{index}_{match_datetime.strftime('%Y%m%d')}_{home_team_name[:3]}_{away_team_name[:3]}"
            source_name = 'Kaggle_martj42_intl_football_results'
            
            stage = row.get('tournament') # e.g., "FIFA World Cup", "Friendly"
            # country_of_match = row.get('country') # This is the host country of the match

            cursor.execute("""
                INSERT OR IGNORE INTO matches 
                (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, stage, source_match_id, source_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (league_id, home_team_id, away_team_id, match_datetime_utc_str, status, home_score, away_score, winner, stage, source_match_id, source_name))
            
            if cursor.rowcount > 0 or conn.execute("SELECT 1 FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name)).fetchone():
                processed_matches_count += 1
            else:
                 print(f"WARNING: Match IGNORED but not found: {source_match_id}. This is unexpected.")

        except sqlite3.Error as e:
            print(f"ERROR: Database error processing row {index+2} ({row.get('home_team')} vs {row.get('away_team')}): {e}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing row {index+2} ({row.get('home_team')} vs {row.get('away_team')}): {e}")
            
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing CSV {file_path}: {e}")
        return 0

    print(f"INFO: Successfully parsed and attempted to store {processed_matches_count} matches from {file_path}.")
    return processed_matches_count

def main():
    print("Starting data collection from Kaggle...")

    global CONFIG
    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        return

    if not setup_kaggle_api():
        print("CRITICAL: Kaggle API setup failed. Exiting.")
        return

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        return

    kaggle_datasets_to_download_config = CONFIG.get('kaggle', {}).get('datasets', [])
    if not kaggle_datasets_to_download_config:
        print("INFO: No datasets configured for download in kaggle section of config.yaml.")
        if conn: conn.close()
        return

    total_matches_all_datasets = 0

    for dataset_config in kaggle_datasets_to_download_config:
        dataset_id = dataset_config.get("dataset_id")
        sport = dataset_config.get("sport")
        parser_func_name = dataset_config.get("parser_function")
        files_to_process = dataset_config.get("files_to_process")
        default_league_name = dataset_config.get("default_league_name")

        # Sanitize dataset_id for directory creation
        safe_dataset_id_for_path = dataset_id.replace('/', '_')
        download_path = f"temp_kaggle_{safe_dataset_id_for_path}"
        
        print(f"\n--- Processing dataset: {dataset_id} ---")
        
        actual_download_path = download_kaggle_dataset(dataset_id, download_path)
        
        if actual_download_path:
            parser_function = globals().get(parser_func_name)
            if not parser_function:
                print(f"ERROR: Parser function '{parser_func_name}' not found. Skipping dataset.")
            else:
                if not files_to_process: # If specific files aren't listed, try to find CSVs
                    files_to_process = [f for f in os.listdir(actual_download_path) if f.lower().endswith('.csv')]
                    if not files_to_process:
                        print(f"WARNING: No CSV files found in {actual_download_path} and no specific files listed. Skipping dataset.")
                
                for file_name in files_to_process:
                    full_file_path = os.path.join(actual_download_path, file_name)
                    if os.path.isfile(full_file_path):
                        print(f"INFO: Parsing file: {full_file_path}")
                        matches_in_file = parser_function(conn, full_file_path, sport, default_league_name)
                        total_matches_all_datasets += matches_in_file
                        print(f"INFO: {matches_in_file} matches processed from file {file_name}.")
                    else:
                        print(f"WARNING: Specified file '{file_name}' not found in downloaded content at '{actual_download_path}'.")
            
            # Cleanup
            try:
                print(f"INFO: Cleaning up downloaded files at {actual_download_path}...")
                shutil.rmtree(actual_download_path)
                print(f"INFO: Successfully removed {actual_download_path}.")
            except Exception as e:
                print(f"ERROR: Could not clean up directory {actual_download_path}: {e}")
        else:
            print(f"INFO: Skipping processing for dataset {dataset_id} due to download failure.")

    print(f"\n--- Kaggle Data Collection Summary ---")
    print(f"Total matches processed across all selected Kaggle datasets: {total_matches_all_datasets}")

    # Verification query
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name LIKE 'Kaggle_%'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from Kaggle sources in the database.")
    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished Kaggle data collection.")

if __name__ == "__main__":
    temp_conn_for_setup = create_connection(DATABASE_FILE)
    if temp_conn_for_setup:
        print("INFO: (Assuming database tables already exist. Run database_setup.py if not.)")
        temp_conn_for_setup.close()
    else:
        print("CRITICAL: Failed to connect to DB for pre-check. Ensure DB path is correct.")
        exit(1)
    main()

```
