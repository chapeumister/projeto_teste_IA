import sqlite3
import requests
import json 
import pandas as pd 
import os
import yaml 
import requests # For tenacity
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type # For tenacity
from datetime import datetime

# Assuming database_setup.py is in the same directory or accessible via python path
try:
    from database_setup import DATABASE_FILE, create_connection
except ImportError:
    # Fallback if running script directly and database_setup is one level up
    from sports_prediction_ai.src.database_setup import DATABASE_FILE, create_connection

# Global config variable, loaded in main()
CONFIG = {}

# Define load_config function
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
            config = yaml.safe_load(f)
        print(f"INFO: Configuration loaded successfully from {actual_config_path}")
        return config
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {actual_config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"ERROR: Error parsing YAML configuration file {actual_config_path}: {e}")
        return None

# Adapted from collect_fivethirtyeight.py
def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
    """
    Gets or creates a league/team ID from the database.
    Returns the internal database ID (league_id or team_id).
    """
    if pd.isna(name) or not name or not sport:
        print(f"Warning: Name or sport is missing or invalid for {entity_type} (Name: {name}, Sport: {sport}). Skipping.")
        return None

    cursor = conn.cursor()
    table_name = "leagues" if entity_type == "league" else "teams"
    id_column = "league_id" if entity_type == "league" else "team_id"
    source_id_column = "source_league_id" if entity_type == "league" else "source_team_id"

    try:
        cursor.execute(f"SELECT {id_column}, {source_id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
        row = cursor.fetchone()
        if row:
            entity_id = row[0]
            existing_source_id = row[1]
            if source_id and (existing_source_id is None or existing_source_id != source_id):
                cursor.execute(f"UPDATE {table_name} SET {source_id_column} = ? WHERE {id_column} = ?", (source_id, entity_id))
            conn.commit()
            return entity_id
        else:
            insert_sql = f"INSERT INTO {table_name} (name, sport, {source_id_column}"
            params_list = [name, sport, source_id]
            if entity_type == "league":
                insert_sql += ", country) VALUES (?, ?, ?, ?)"
                params_list.append(country)
            else: # team
                insert_sql += ") VALUES (?, ?, ?)"
            
            cursor.execute(insert_sql, tuple(params_list))
            conn.commit()
            return cursor.lastrowid
            
    except sqlite3.IntegrityError:
        print(f"INFO: IntegrityError on insert for {entity_type} '{name}', trying to fetch existing.")
        cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            print(f"ERROR: Critical - Failed to insert and then fetch {entity_type} '{name}' after IntegrityError.")
            return None
    except sqlite3.Error as e:
        print(f"Database error in _get_or_create_entity_id for {entity_type} '{name}': {e}")
        conn.rollback() # Rollback on error
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def download_json_data(path_on_github: str):
    """
    Downloads JSON data from the football.json GitHub repository with retries.
    Returns the parsed Python dictionary/list or None if error.
    """
    raw_football_json_base_url = CONFIG.get('api_base_urls', {}).get('raw_github_football_json')
    if not raw_football_json_base_url:
        print("ERROR: raw_github_football_json base URL not found in configuration.")
        raise ValueError("Configuration error: raw_github_football_json base URL missing.")


    url = f"{raw_football_json_base_url}{path_on_github}"
    print(f"INFO: Attempting to download data from {url}")
    
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json() 

def _extract_team_name(team_data):
    """ Helper to extract team name whether it's a string or a dict. """
    if isinstance(team_data, dict):
        return team_data.get('name')
    elif isinstance(team_data, str):
        return team_data
    return None


def parse_and_store_football_json(conn, data: dict, default_country: str, sport: str = 'football'):
    """
    Parses data from football.json format and stores it in the database using UPSERT logic.
    Returns tuple: (inserted_matches, updated_matches)
    """
    if not data:
        return 0, 0

    inserted_matches, updated_matches = 0, 0
    cursor = conn.cursor()

    league_name_json = data.get('name')
    if not league_name_json:
        print("WARNING: League name not found in JSON data. Skipping this dataset.")
        return 0, 0
    
    league_id = _get_or_create_entity_id(conn, "league", league_name_json, sport, country=default_country)
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {league_name_json}. Skipping matches in this dataset.")
        return 0, 0

    matches_list = data.get('matches', [])
    if not matches_list:
        print(f"INFO: No matches found in the dataset for league '{league_name_json}'.")
        return 0, 0

    for match_data in matches_list:
        try:
            team1_name = _extract_team_name(match_data.get('team1'))
            team2_name = _extract_team_name(match_data.get('team2'))

            if not team1_name or not team2_name:
                print(f"WARNING: Missing team name(s) for match in {league_name_json}. Row: {match_data}. Skipping.")
                continue

            home_team_id = _get_or_create_entity_id(conn, "team", team1_name, sport)
            away_team_id = _get_or_create_entity_id(conn, "team", team2_name, sport)

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match: {team1_name} vs {team2_name} in {league_name_json}. Skipping.")
                continue

            raw_date = match_data.get('date')
            if not raw_date:
                print(f"WARNING: Missing date for match {team1_name} vs {team2_name} in {league_name_json}. Skipping.")
                continue
            try:
                # Dates are usually YYYY-MM-DD, sometimes with time YYYY-MM-DDTHH:MM:SSZ
                if 'T' in raw_date:
                    match_datetime = datetime.fromisoformat(raw_date.replace('Z', '+00:00'))
                else:
                    match_datetime = datetime.strptime(raw_date, '%Y-%m-%d')
                match_datetime_utc_str = match_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"WARNING: Could not parse date '{raw_date}' for match {team1_name} vs {team2_name} in {league_name_json}: {e}. Skipping.")
                continue
            
            score_data = match_data.get('score', {}).get('ft') # Full Time score, usually a list [home, away]
            home_score, away_score, winner, status = None, None, None, "SCHEDULED"

            if score_data and isinstance(score_data, list) and len(score_data) == 2:
                try:
                    home_score = int(score_data[0])
                    away_score = int(score_data[1])
                    if home_score > away_score: winner = 'HOME_TEAM'
                    elif away_score > home_score: winner = 'AWAY_TEAM'
                    else: winner = 'DRAW'
                    status = "FINISHED"
                except (ValueError, TypeError): # Handle if scores are not integers
                    print(f"WARNING: Could not parse scores {score_data} for match {team1_name} vs {team2_name}. Treating as unscheduled/unknown score.")
                    home_score, away_score, winner, status = None, None, None, "SCHEDULED" # Or some other status
            else: # No score or malformed score
                 status = "SCHEDULED" if match_datetime > datetime.now() else "UNKNOWN_SCORE"


            # Construct a unique source_match_id
            # Using team names and date. League name can be long, so maybe a key from it.
            # For football.json, team codes (e.g., match_data['team1_code']) might be more stable if available.
            # For now, using names.
            team1_repr = team1_name.replace(' ', '')
            team2_repr = team2_name.replace(' ', '')
            league_repr = league_name_json.split(' ')[0].replace(' ', '') # First word of league name
            source_match_id_val = f"ofj_{match_datetime.strftime('%Y%m%d')}_{team1_repr}_{team2_repr}_{league_repr}"
            source_name_val = 'OpenFootball_football.json'
            is_mock_val = 0 # This script processes historical data
            
            stage_val = match_data.get('round') or match_data.get('group')

            # UPSERT logic for matches
            cursor.execute("SELECT match_id, status, home_score, away_score, match_datetime_utc FROM matches WHERE source_match_id = ? AND source_name = ?", 
                           (source_match_id_val, source_name_val))
            existing_match = cursor.fetchone()
            current_match_db_id = None

            match_payload = {
                "league_id": league_id, "home_team_id": home_team_id, "away_team_id": away_team_id,
                "match_datetime_utc": match_datetime_utc_str, "status": status,
                "home_score": home_score, "away_score": away_score, "winner": winner,
                "stage": stage_val, "source_match_id": source_match_id_val, 
                "source_name": source_name_val, "is_mock": is_mock_val
            }

            if existing_match:
                current_match_db_id = existing_match[0]
                if (existing_match[1] != status or existing_match[2] != home_score or 
                    existing_match[3] != away_score or existing_match[4] != match_datetime_utc_str):
                    update_cols = {k: v for k, v in match_payload.items() if k not in ['source_match_id', 'source_name']}
                    set_clause = ", ".join([f"{col} = ?" for col in update_cols.keys()])
                    params = list(update_cols.values()) + [current_match_db_id]
                    cursor.execute(f"UPDATE matches SET {set_clause} WHERE match_id = ?", params)
                    if cursor.rowcount > 0: updated_matches += 1
            else:
                cols = ", ".join(match_payload.keys())
                placeholders = ", ".join(["?"] * len(match_payload))
                cursor.execute(f"INSERT INTO matches ({cols}) VALUES ({placeholders})", list(match_payload.values()))
                current_match_db_id = cursor.lastrowid
                if current_match_db_id: inserted_matches += 1
                else: print(f"ERROR: Failed to insert match {source_match_id_val} or get lastrowid."); continue
            
            # No odds or detailed stats typically in football.json, so no UPSERT for those here.

        except sqlite3.Error as e:
            print(f"ERROR: Database error processing match in {league_name_json}: {e}. Match data: {match_data}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing match in {league_name_json}: {e}. Match data: {match_data}")
            
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing dataset for league {league_name_json}: {e}")
        return 0, 0

    print(f"INFO: For league '{league_name_json}': {inserted_matches} new matches, {updated_matches} updated.")
    return inserted_matches, updated_matches

def main():
    print("Starting data collection from football.json sources...")

    global CONFIG
    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        return

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        return

    football_json_datasets_config = CONFIG.get('openfootball_football_json', {}).get('datasets', [])
    if not football_json_datasets_config:
        print("INFO: No datasets configured for download in openfootball_football_json section of config.yaml.")
        if conn: conn.close()
        return

    total_matches_processed_all_files = 0

    for dataset_config in football_json_datasets_config:
        path = dataset_config.get("path")
        sport = dataset_config.get("sport")
        default_country = dataset_config.get("default_country") 

        if not path or not sport: # Basic validation for required fields from config
            print(f"WARNING: Invalid dataset configuration entry: {dataset_config}. Missing 'path' or 'sport'. Skipping.")
            continue
        
        print(f"\n--- Processing dataset: {path} ---")
        
        json_data = download_json_data(path) # Base URL is now used inside download_json_data via CONFIG
        
        if json_data:
            matches_in_file = parse_and_store_football_json(conn, json_data, default_country, sport) # Pass sport from config
            total_matches_processed_all_files += matches_in_file
            print(f"INFO: {matches_in_file} matches processed from {path}.")
        else:
            print(f"INFO: No data processed for {path} (download or parsing failed).")

    print(f"\n--- football.json Data Collection Summary ---")
    print(f"Total matches processed across all selected files: {total_matches_processed_all_files}")

    # Verification query
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name = 'OpenFootball_football.json'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from 'OpenFootball_football.json' in the database.")
    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished football.json data collection.")

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
