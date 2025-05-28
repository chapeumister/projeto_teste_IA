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

# Define load_config function (similar to collect_soccer_data_co_uk.py)
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

# Adapted from collect_soccer_data_co_uk.py / data_collection.py
def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
    """
    Gets or creates a league/team ID from the database.
    Returns the internal database ID (league_id or team_id).
    """
    if pd.isna(name) or not name or not sport: # Check for pd.isna for pandas Series values
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
            if source_id and (existing_source_id is None or existing_source_id != source_id): # Update if new source_id provided and was null or different
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
def download_csv(file_path_on_github: str):
    """
    Downloads CSV data from the FiveThirtyEight GitHub repository with retries.
    Returns content as io.StringIO or None if error.
    """
    raw_github_base_url = CONFIG.get('api_base_urls', {}).get('raw_github_fivethirtyeight')
    if not raw_github_base_url:
        print("ERROR: raw_github_fivethirtyeight base URL not found in configuration.")
        raise ValueError("Configuration error: raw_github_fivethirtyeight base URL missing.")


    url = f"{raw_github_base_url}{file_path_on_github}"
    print(f"INFO: Attempting to download data from {url}")
    
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return io.StringIO(response.text)

def parse_and_store_soccer_spi_matches(conn, csv_content_stream, dataset_config_from_yaml: dict):
    """
    Parses soccer-spi/spi_matches.csv data and stores it in the database using UPSERT logic.
    Uses column names from the dataset_config_from_yaml.
    Returns tuple: (inserted_matches, updated_matches, inserted_odds, updated_odds, inserted_stats, updated_stats)
    """
    if not csv_content_stream:
        return 0, 0, 0, 0, 0, 0

    inserted_matches, updated_matches = 0,0
    inserted_odds, updated_odds = 0,0
    inserted_stats, updated_stats = 0,0
    try:
        df = pd.read_csv(csv_content_stream)
    except pd.errors.EmptyDataError:
        print("WARNING: CSV content was empty or unparseable (EmptyDataError).")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to parse CSV content: {e}")
        return 0

    if df.empty:
        print("INFO: CSV parsed into an empty DataFrame.")
        return 0

    cursor = conn.cursor()
    sport = dataset_config_from_yaml["sport"] # Sport is defined in the config for this dataset

    # Column names from config
    league_col = dataset_config_from_yaml.get("league_col", "league")
    home_team_col = dataset_config_from_yaml.get("home_team_col", "team1")
    away_team_col = dataset_config_from_yaml.get("away_team_col", "team2")
    date_col = dataset_config_from_yaml.get("date_col", "date")
    home_score_col = dataset_config_from_yaml.get("home_score_col", "score1")
    away_score_col = dataset_config_from_yaml.get("away_score_col", "score2")
    home_prob_col = dataset_config_from_yaml.get("home_prob_col", "prob1")
    draw_prob_col = dataset_config_from_yaml.get("draw_prob_col", "probtie") # Corrected key from config
    away_prob_col = dataset_config_from_yaml.get("away_prob_col", "prob2")
    home_spi_col = dataset_config_from_yaml.get("home_spi_col", "spi1")
    away_spi_col = dataset_config_from_yaml.get("away_spi_col", "spi2")
    home_importance_col = dataset_config_from_yaml.get("home_importance_col", "importance1")
    away_importance_col = dataset_config_from_yaml.get("away_importance_col", "importance2")
    league_id_col_csv = dataset_config_from_yaml.get("league_id_col_csv") # Optional, might not be in all configs

    for index, row in df.iterrows():
        try:
            league_name = row.get(league_col)
            home_team_name = row.get(home_team_col)
            away_team_name = row.get(away_team_col)
            
            source_league_api_id = None
            if league_id_col_csv and league_id_col_csv in row:
                 source_league_api_id = str(row.get(league_id_col_csv))

            league_id = _get_or_create_entity_id(conn, "league", league_name, sport, source_id=source_league_api_id)
            home_team_id = _get_or_create_entity_id(conn, "team", home_team_name, sport)
            away_team_id = _get_or_create_entity_id(conn, "team", away_team_name, sport)

            if not league_id or not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create league/team IDs for row {index+2}. Skipping match.")
                continue

            raw_date = row.get(date_col)
            if pd.isna(raw_date):
                print(f"WARNING: Skipping row {index+2} due to missing Date.")
                continue
            try:
                match_datetime = pd.to_datetime(raw_date) 
                match_datetime_utc_str = match_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                print(f"WARNING: Could not parse date '{raw_date}' for row {index+2}: {e}. Skipping match.")
                continue
            
            home_score = row.get(home_score_col)
            away_score = row.get(away_score_col)

            winner = None
            status = "SCHEDULED" 
            if pd.notna(home_score) and pd.notna(away_score):
                home_score = int(home_score)
                away_score = int(away_score)
                if home_score > away_score: winner = 'HOME_TEAM'
                elif away_score > home_score: winner = 'AWAY_TEAM'
                else: winner = 'DRAW'
                status = "FINISHED"
            else: 
                home_score = None
                away_score = None

            source_match_id = f"538_{match_datetime.strftime('%Y%m%d')}_{home_team_name}_{away_team_name}_{source_league_api_id or 'NOLID'}"
            source_name = f"FiveThirtyEight_{dataset_config_from_yaml['file_path'].split('/')[0]}"

            cursor.execute("""
                INSERT OR IGNORE INTO matches 
                (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, source_match_id, source_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (league_id, home_team_id, away_team_id, match_datetime_utc_str, status, home_score, away_score, winner, source_match_id, source_name))
            
            current_match_db_id = None
            if cursor.rowcount > 0 : 
                 current_match_db_id = cursor.lastrowid
            else: 
                cursor.execute("SELECT match_id FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
                existing_match_row = cursor.fetchone()
                if existing_match_row:
                    current_match_db_id = existing_match_row[0]
                else:
                    print(f"WARNING: Match IGNORED but not found: {source_match_id}. Odds/Stats will be skipped.")
            
            if current_match_db_id:
                processed_matches_count += 1

                prob_h = row.get(home_prob_col)
                prob_a = row.get(away_prob_col)
                prob_d = row.get(draw_prob_col) # Using corrected key
                if pd.notna(prob_h) and pd.notna(prob_a) and pd.notna(prob_d):
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO odds
                            (match_id, bookmaker, market_type, home_odds, draw_odds, away_odds, timestamp_utc)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (current_match_db_id, 'FiveThirtyEight_SPI', '1X2_probabilities', float(prob_h), float(prob_d), float(prob_a), match_datetime_utc_str))
                    except (ValueError, TypeError) as ve:
                        print(f"WARNING: Could not parse probabilities for match {source_match_id}: {ve}.")
                    except sqlite3.Error as sqle:
                        print(f"WARNING: DB error inserting probabilities for match {source_match_id}: {sqle}")
                
                spi_h = row.get(home_spi_col)
                spi_a = row.get(away_spi_col)
                imp_h = row.get(home_importance_col)
                imp_a = row.get(away_importance_col)

                stats_to_insert = []
                if pd.notna(spi_h): stats_to_insert.append({'team_id': home_team_id, 'type': 'spi_rating', 'value': spi_h})
                if pd.notna(spi_a): stats_to_insert.append({'team_id': away_team_id, 'type': 'spi_rating', 'value': spi_a})
                if pd.notna(imp_h): stats_to_insert.append({'team_id': home_team_id, 'type': 'match_importance', 'value': imp_h})
                if pd.notna(imp_a): stats_to_insert.append({'team_id': away_team_id, 'type': 'match_importance', 'value': imp_a})
                
                for stat_item in stats_to_insert:
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO stats
                            (match_id, team_id, stat_type, stat_value)
                            VALUES (?, ?, ?, ?)
                        """, (current_match_db_id, stat_item['team_id'], stat_item['type'], str(stat_item['value'])))
                    except (ValueError, TypeError) as ve: 
                        print(f"WARNING: Could not parse {stat_item['type']} for team {stat_item['team_id']} in match {source_match_id}: {ve}.")
                    except sqlite3.Error as sqle:
                        print(f"WARNING: DB error inserting {stat_item['type']} for match {source_match_id}: {sqle}")
            
        except sqlite3.Error as e:
            print(f"ERROR: Database error processing row {index+2} for match between {row.get(home_team_col)} and {row.get(away_team_col)}: {e}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing row {index+2} for match between {row.get(home_team_col)} and {row.get(away_team_col)}: {e}")

    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing CSV {dataset_config_from_yaml['file_path']}: {e}")
        return 0 

    print(f"INFO: Successfully parsed and attempted to store {processed_matches_count} matches from {dataset_config_from_yaml['file_path']}.")
    return processed_matches_count

def main():
    print("Starting data collection from FiveThirtyEight...")

    global CONFIG
    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        return

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        return

    fivethirtyeight_datasets_config = CONFIG.get('fivethirtyeight', {}).get('datasets', [])
    if not fivethirtyeight_datasets_config:
        print("INFO: No datasets configured for download in fivethirtyeight section of config.yaml.")
        if conn: conn.close()
        return

    total_matches_processed_all_datasets = 0

    for dataset_config_entry in fivethirtyeight_datasets_config: # Iterate over list from YAML
        file_path = dataset_config_entry.get("file_path")
        parser_func_name = dataset_config_entry.get("parser_function")

        if not file_path or not parser_func_name:
            print(f"WARNING: Invalid dataset configuration entry: {dataset_config_entry}. Missing 'file_path' or 'parser_function'. Skipping.")
            continue
        
        print(f"\n--- Processing dataset: {file_path} ---")
        
        csv_content_stream = download_csv(file_path) # RAW_GITHUB_BASE_URL is now used inside download_csv via CONFIG
        
        if csv_content_stream:
            parser_function = globals().get(parser_func_name)
            if parser_function:
                # Pass the specific dataset_config_entry to the parser
                matches_in_csv = parser_function(conn, csv_content_stream, dataset_config_entry)
                total_matches_processed_all_datasets += matches_in_csv
                print(f"INFO: {matches_in_csv} matches processed from {file_path}.")
            else:
                print(f"WARNING: Parser function '{parser_func_name}' not found for {file_path}. Skipping.")
        else:
            print(f"INFO: No data processed for {file_path} (download failed or empty).")

    print(f"\n--- FiveThirtyEight Data Collection Summary ---")
    print(f"Total matches processed across all selected datasets: {total_matches_processed_all_datasets}")

    # Verification query
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name LIKE 'FiveThirtyEight_%'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from FiveThirtyEight sources in the database.")
        
        cursor.execute("""
            SELECT COUNT(*) FROM odds o 
            JOIN matches m ON o.match_id = m.match_id 
            WHERE m.source_name LIKE 'FiveThirtyEight_%' AND o.bookmaker = 'FiveThirtyEight_SPI'
        """)
        odds_count = cursor.fetchone()[0]
        print(f"Verification: Found {odds_count} 'FiveThirtyEight_SPI' odds entries linked to their matches.")

        cursor.execute("""
            SELECT COUNT(*) FROM stats s 
            JOIN matches m ON s.match_id = m.match_id 
            WHERE m.source_name LIKE 'FiveThirtyEight_%' AND (s.stat_type = 'spi_rating' OR s.stat_type = 'match_importance')
        """)
        stats_count = cursor.fetchone()[0]
        print(f"Verification: Found {stats_count} SPI/importance stats entries linked to their matches.")

    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished FiveThirtyEight data collection.")

if __name__ == "__main__":
    # Assuming tables exist. Run database_setup.py if not.
    temp_conn_for_setup = create_connection(DATABASE_FILE)
    if temp_conn_for_setup:
        print("INFO: (Assuming database tables already exist. Run database_setup.py if not.)")
        temp_conn_for_setup.close()
    else:
        print("CRITICAL: Failed to connect to DB for pre-check. Ensure DB path is correct.")
        exit(1)
    main()
```
