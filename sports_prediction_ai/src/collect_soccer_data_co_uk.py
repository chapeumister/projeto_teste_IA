import sqlite3
import requests
import pandas as pd
import io
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
    # Adjust path to be relative to the script's location if necessary,
    # or use an absolute path / environment variable.
    # For this project structure, sports_prediction_ai/ is the root.
    # If this script is in sports_prediction_ai/src/, config.yaml is one level up.
    
    # Correct path assuming script is in sports_prediction_ai/src/
    # and config.yaml is in sports_prediction_ai/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir) # This should be sports_prediction_ai/
    actual_config_path = os.path.join(project_root, 'config.yaml')

    if not os.path.exists(actual_config_path):
        # Fallback for cases where the script might be run from a different CWD
        # e.g. if sports_prediction_ai/ is the CWD
        actual_config_path = 'config.yaml' 
        if not os.path.exists(actual_config_path):
             print(f"ERROR: Configuration file not found at {os.path.join(project_root, 'config.yaml')} or {actual_config_path}")
             return None # Or raise an error

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

# Adapted from data_collection.py
def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
    """
    Gets or creates a league/team ID from the database.
    Returns the internal database ID (league_id or team_id).
    """
    if pd.isna(name) or not name or not sport: # Handle potential NaN from pandas
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
            # Update source_id if a new one is provided and it was null or different
            if source_id and (existing_source_id is None or existing_source_id != source_id):
                cursor.execute(f"UPDATE {table_name} SET {source_id_column} = ? WHERE {id_column} = ?", (source_id, entity_id))
                # print(f"INFO: Updated {source_id_column} for {entity_type} '{name}' (ID: {entity_id}) to '{source_id}'.")
            conn.commit()
            return entity_id
        else:
            # Insert new entity
            insert_sql = f"INSERT INTO {table_name} (name, sport, {source_id_column}"
            params_list = [name, sport, source_id]
            if entity_type == "league":
                insert_sql += ", country) VALUES (?, ?, ?, ?)"
                params_list.append(country) # country can be None
            else: # team
                insert_sql += ") VALUES (?, ?, ?)"
            
            cursor.execute(insert_sql, tuple(params_list))
            conn.commit()
            # print(f"INFO: Inserted new {entity_type} '{name}' with source_id '{source_id}'.")
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
def download_csv(season_path: str, league_code: str):
    """
    Downloads CSV data for a given season and league code from soccer-data.co.uk.
    Returns content as string or None if error. Retries on failure.
    """
    base_url = CONFIG.get('api_base_urls', {}).get('soccer_data_co_uk')
    if not base_url:
        print("ERROR: soccer_data_co_uk base URL not found in configuration.")
        # This error is critical enough to not retry, or tenacity needs to handle it if it's transient somehow
        raise ValueError("Configuration error: soccer_data_co_uk base URL missing.") 
        
    url = f"{base_url}/{season_path}/{league_code}.csv"
    print(f"INFO: Attempting to download data from {url}")
    
    response = requests.get(url, timeout=20)
    response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)
    
    # Try decoding with utf-8 first, then common alternatives for these CSVs
    try:
        return response.content.decode('utf-8')
    except UnicodeDecodeError:
        print(f"INFO: UTF-8 decoding failed for {url}, trying 'cp1252'.")
        try:
            return response.content.decode('cp1252')
        except UnicodeDecodeError:
            print(f"INFO: cp1252 decoding failed for {url}, trying 'latin1'.")
            return response.content.decode('latin1') # Can still raise if truly unknown

def parse_and_store_data(conn, csv_content: str, league_name_default: str, country_default: str, sport: str = 'football'):
    """
    Parses CSV data and stores match, odds, and stats information in the database.
    Returns tuple: (inserted_matches, updated_matches, inserted_odds, updated_odds, inserted_stats, updated_stats)
    """
    if not csv_content:
        return 0, 0, 0, 0, 0, 0

    inserted_matches, updated_matches = 0, 0
    inserted_odds, updated_odds = 0, 0
    inserted_stats, updated_stats = 0, 0
    try:
        # Using StringIO to treat the string as a file
        df = pd.read_csv(io.StringIO(csv_content))
    except pd.errors.EmptyDataError:
        print("WARNING: CSV content was empty or unparseable (EmptyDataError).")
        return 0
    except Exception as e: # Catch other potential pandas parsing errors
        print(f"ERROR: Failed to parse CSV content: {e}")
        return 0

    if df.empty:
        print("INFO: CSV parsed into an empty DataFrame.")
        return 0

    cursor = conn.cursor()

    # Infer league name from CSV 'Div' column if available, else use default
    # soccer-data.co.uk uses 'Div' for division/league code, not full name.
    # League name needs to be derived from league_name_default passed in based on league_code.
    # country_default is also passed in.

    league_id = _get_or_create_entity_id(conn, "league", league_name_default, sport, None, country_default)
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {league_name_default}. Skipping this CSV.")
        return 0

    for index, row in df.iterrows():
        try:
            home_team_name = row.get('HomeTeam')
            away_team_name = row.get('AwayTeam')

            if pd.isna(home_team_name) or pd.isna(away_team_name):
                print(f"WARNING: Skipping row {index+2} due to missing HomeTeam or AwayTeam name.")
                continue

            home_team_id = _get_or_create_entity_id(conn, "team", home_team_name, sport)
            away_team_id = _get_or_create_entity_id(conn, "team", away_team_name, sport)

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match: {home_team_name} vs {away_team_name}. Skipping row {index+2}.")
                continue

            # Match Date
            raw_date = row.get('Date')
            if pd.isna(raw_date):
                print(f"WARNING: Skipping row {index+2} due to missing Date.")
                continue
            try:
                # soccer-data.co.uk dates are usually dd/mm/yy or dd/mm/yyyy
                match_datetime = pd.to_datetime(raw_date, format='%d/%m/%y', errors='coerce')
                if pd.isna(match_datetime): # Try with 4-digit year if previous failed
                    match_datetime = pd.to_datetime(raw_date, format='%d/%m/%Y', errors='raise')
                match_datetime_utc_str = match_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"WARNING: Could not parse date '{raw_date}' for row {index+2}: {e}. Skipping match.")
                continue
            
            # Scores
            fthg = row.get('FTHG') # Full Time Home Goals
            ftag = row.get('FTAG') # Full Time Away Goals

            if pd.isna(fthg) or pd.isna(ftag):
                home_score = None
                away_score = None
                winner = None # If scores are not final, winner is undetermined from this data
                status = "SCHEDULED" # Or placeholder, as scores are missing
            else:
                home_score = int(fthg)
                away_score = int(ftag)
                if home_score > away_score:
                    winner = 'HOME_TEAM'
                elif away_score > home_score:
                    winner = 'AWAY_TEAM'
                else:
                    winner = 'DRAW'
                status = "FINISHED"

            source_match_id = f"sdcuk_{match_datetime.strftime('%Y%m%d')}_{home_team_name}_{away_team_name}"
            source_name = 'soccer-data.co.uk'

            # Insert match
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO matches 
                    (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, source_match_id, source_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (league_id, home_team_id, away_team_id, match_datetime_utc_str, status, home_score, away_score, winner, source_match_id, source_name))
                
                current_match_db_id = None
                if cursor.lastrowid != 0: # New row was inserted
                    current_match_db_id = cursor.lastrowid
                else: # Row was ignored, likely exists. Fetch its ID.
                    cursor.execute("SELECT match_id FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
                    existing_match_row = cursor.fetchone()
                    if existing_match_row:
                        current_match_db_id = existing_match_row[0]
                    else:
                        print(f"WARNING: Match was IGNORED but could not be found: {source_match_id}. Odds/Stats will be skipped.")
                        # This case is unlikely with proper INSERT OR IGNORE and subsequent fetch.
                
                if current_match_db_id:
                    processed_matches_count += 1
                    # TODO: Add Odds and Stats processing here, using current_match_db_id
                    
                    # Example for Bet365 Odds (common in these files)
                    bookmaker = "Bet365"
                    b365h, b365d, b365a = row.get('B365H'), row.get('B365D'), row.get('B365A')
                    if not pd.isna(b365h) and not pd.isna(b365d) and not pd.isna(b365a):
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO odds
                                (match_id, bookmaker, market_type, home_odds, draw_odds, away_odds, timestamp_utc)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (current_match_db_id, bookmaker, "1X2", float(b365h), float(b365d), float(b365a), match_datetime_utc_str))
                        except (ValueError, TypeError) as ve:
                            print(f"WARNING: Could not parse Bet365 odds for match {source_match_id}: {ve}. Odds: H={b365h}, D={b365d}, A={b365a}")
                        except sqlite3.Error as sqle:
                            print(f"WARNING: DB error inserting Bet365 odds for match {source_match_id}: {sqle}")
                            
                    # Example for William Hill Odds
                    bookmaker_wh = "WilliamHill"
                    whh, whd, wha = row.get('WHH'), row.get('WHD'), row.get('WHA')
                    if not pd.isna(whh) and not pd.isna(whd) and not pd.isna(wha):
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO odds
                                (match_id, bookmaker, market_type, home_odds, draw_odds, away_odds, timestamp_utc)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (current_match_db_id, bookmaker_wh, "1X2", float(whh), float(whd), float(wha), match_datetime_utc_str))
                        except (ValueError, TypeError) as ve:
                            print(f"WARNING: Could not parse William Hill odds for match {source_match_id}: {ve}. Odds: H={whh}, D={whd}, A={wha}")
                        except sqlite3.Error as sqle:
                            print(f"WARNING: DB error inserting William Hill odds for match {source_match_id}: {sqle}")


                    # Example for Stats (Home/Away Shots)
                    hs, as_ = row.get('HS'), row.get('AS') # pandas df.AS is a keyword, so pandas might rename column to AS_ or use row.get('AS')
                    if 'AS' not in row and 'AS_' in row: # common pandas renaming for keyword-like column names
                        as_ = row.get('AS_')
                    
                    if not pd.isna(hs) and home_team_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO stats
                                (match_id, team_id, stat_type, stat_value)
                                VALUES (?, ?, ?, ?)
                            """, (current_match_db_id, home_team_id, "shots_total", int(hs)))
                        except (ValueError, TypeError) as ve:
                             print(f"WARNING: Could not parse Home Shots for match {source_match_id}: {ve}. Value: {hs}")
                        except sqlite3.Error as sqle:
                            print(f"WARNING: DB error inserting Home Shots for match {source_match_id}: {sqle}")

                    if not pd.isna(as_) and away_team_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO stats
                                (match_id, team_id, stat_type, stat_value)
                                VALUES (?, ?, ?, ?)
                            """, (current_match_db_id, away_team_id, "shots_total", int(as_)))
                        except (ValueError, TypeError) as ve:
                             print(f"WARNING: Could not parse Away Shots for match {source_match_id}: {ve}. Value: {as_}")
                        except sqlite3.Error as sqle:
                            print(f"WARNING: DB error inserting Away Shots for match {source_match_id}: {sqle}")


            except sqlite3.Error as e:
                print(f"ERROR: Database error processing match row {index+2} ({home_team_name} vs {away_team_name}): {e}")
            except Exception as e: # Catch other unexpected errors for this row
                print(f"ERROR: Unexpected error processing match row {index+2} ({home_team_name} vs {away_team_name}): {e}")
        
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing CSV: {e}")
        # If commit fails, the processed_matches_count might not accurately reflect stored data.
        # Depending on error, might need rollback or other handling.
        return 0 # Or a specific error code

    print(f"INFO: Successfully parsed and attempted to store {processed_matches_count} matches from the CSV for league {league_name_default}.")
    return processed_matches_count

def main():
    print("Starting historical data collection from soccer-data.co.uk...")
    
    global CONFIG
    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        return

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        return

    leagues_to_download_config = CONFIG.get('soccer_data_co_uk', {}).get('leagues_to_download', [])
    if not leagues_to_download_config:
        print("INFO: No leagues configured for download in soccer_data_co_uk section of config.yaml.")
        if conn: conn.close()
        return

    total_matches_processed_all_leagues = 0

    for league_info in leagues_to_download_config:
        season_path = league_info.get("season_path")
        # country field in config can be null, handle appropriately if needed for league creation
        country = league_info.get("country") 
        
        if not season_path:
            print(f"WARNING: Missing 'season_path' in league configuration: {league_info}. Skipping.")
            continue

        for league_code in league_info.get("league_codes", []):
            # league_names is a dict mapping code to name, e.g. {"E0": "Premier League"}
            league_name = league_info.get("league_names", {}).get(league_code, f"Unknown League ({league_code})")
            
            print(f"\n--- Processing: {league_name} (Country: {country if country else 'N/A'}) - Season {season_path} - Code {league_code} ---")
            
            csv_content = download_csv(season_path, league_code) # BASE_URL is now accessed via CONFIG within download_csv
            if csv_content:
                # Pass country from the specific league_info if available, otherwise it remains None
                matches_in_csv = parse_and_store_data(conn, csv_content, league_name, country)
                total_matches_processed_all_leagues += matches_in_csv
                print(f"INFO: {matches_in_csv} matches processed for {league_name} from {season_path}/{league_code}.csv")
            else:
                print(f"INFO: No data processed for {league_name} from {season_path}/{league_code}.csv (download failed or empty).")

    print(f"\n--- Historical Data Collection Summary ---")
    print(f"Total matches processed across all selected leagues and seasons: {total_matches_processed_all_leagues}")

    # Verification query (optional, for quick check)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name = 'soccer-data.co.uk'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from 'soccer-data.co.uk' in the database.")
        
        cursor.execute("SELECT COUNT(*) FROM odds o JOIN matches m ON o.match_id = m.match_id WHERE m.source_name = 'soccer-data.co.uk'")
        odds_count = cursor.fetchone()[0]
        print(f"Verification: Found {odds_count} odds entries linked to 'soccer-data.co.uk' matches.")

        cursor.execute("SELECT COUNT(*) FROM stats s JOIN matches m ON s.match_id = m.match_id WHERE m.source_name = 'soccer-data.co.uk'")
        stats_count = cursor.fetchone()[0]
        print(f"Verification: Found {stats_count} stats entries linked to 'soccer-data.co.uk' matches.")

    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished historical data collection.")

if __name__ == "__main__":
    # Ensure the database exists and has tables.
    # This could be a separate setup step, but for standalone script execution,
    # it's good to ensure tables are there.
    temp_conn_for_setup = create_connection(DATABASE_FILE)
    if temp_conn_for_setup:
        # We need create_tables from database_setup, but it's not directly imported in this plan.
        # For now, assuming tables exist. A more robust solution would be:
        # from database_setup import create_tables as setup_tables_func
        # setup_tables_func(temp_conn_for_setup)
        print("INFO: (Assuming database tables already exist. Run database_setup.py if not.)")
        temp_conn_for_setup.close()
    else:
        print("CRITICAL: Failed to connect to DB for pre-check. Ensure DB path is correct.")
        exit(1) # Exit if we can't even connect for a pre-check

    main()

```
