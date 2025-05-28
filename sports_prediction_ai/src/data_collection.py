# src/data_collection.py
import os
import requests
import warnings
import json
import sqlite3
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type # For tenacity
import requests.exceptions # For tenacity retry condition

# Import database constants and functions
from sports_prediction_ai.src.database_setup import DATABASE_FILE, create_connection
import yaml # Added for config loading

# Global config variable
CONFIG = {}
FOOTBALL_DATA_BASE_URL = "" # Will be loaded from CONFIG
APISPORTS_BASE_URL = ""     # Will be loaded from CONFIG

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

# Load configuration at script startup
CONFIG = load_config()
if CONFIG:
    FOOTBALL_DATA_BASE_URL = CONFIG.get('api_base_urls', {}).get('football_data_org', "https://api.football-data.org/v4/")
    APISPORTS_BASE_URL = CONFIG.get('api_base_urls', {}).get('api_sports', "https://v3.football.api-sports.io/")
else:
    print("CRITICAL: Failed to load configuration for data_collection.py. Using hardcoded fallbacks for base URLs.")
    FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4/"
    APISPORTS_BASE_URL = "https://v3.football.api-sports.io/"


FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")
APISPORTS_API_KEY = os.getenv("APISPORTS_API_KEY")
MOCK_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mock_matches.json')

def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
    """
    Gets or creates a league/team ID from the database.
    Returns the internal database ID (league_id or team_id).
    """
    if not name or not sport: 
        print(f"Error: Name and sport are required for {entity_type}.")
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
                # print(f"INFO: Updated {source_id_column} for {entity_type} '{name}' (ID: {entity_id}) from '{existing_source_id}' to '{source_id}'.")
            conn.commit()
            return entity_id
        else:
            # Not found by name and sport, so insert it
            insert_sql = f"""INSERT INTO {table_name} (name, sport, {source_id_column}"""
            params_list = [name, sport, source_id]
            if entity_type == "league":
                insert_sql += ", country) VALUES (?, ?, ?, ?)"
                params_list.append(country)
            else:
                insert_sql += ") VALUES (?, ?, ?)"
            
            cursor.execute(insert_sql, tuple(params_list))
            conn.commit()
            # print(f"INFO: Inserted new {entity_type} '{name}' with source_id '{source_id}'.")
            return cursor.lastrowid # Should be reliable for newly inserted row
            
    except sqlite3.Error as e:
        print(f"Database error in _get_or_create_entity_id for {entity_type} '{name}': {e}")
        conn.rollback() # Rollback on error
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException))
)
def _make_api_request_with_retry(url, method='GET', headers=None, params=None, data=None):
    response = requests.request(method, url, headers=headers, params=params, data=data, timeout=20)
    response.raise_for_status()
    return response

def get_matches_for_date(date_str: str, api_key: str = FOOTBALL_DATA_API_KEY) -> tuple[int, int]:
    """
    Fetches matches for a specific date from football-data.org API and stores them in the database.
    Returns a tuple: (inserted_matches_count, updated_matches_count).
    """
    if not api_key or api_key == "YOUR_API_TOKEN":
        print("Error: Invalid or missing API key for football-data.org.")
        return 0, 0

    headers = {"X-Auth-Token": api_key}
    current_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = current_date + timedelta(days=1)
    next_day_str = next_day.strftime("%Y-%m-%d")
    api_url = f"{FOOTBALL_DATA_BASE_URL}matches?dateFrom={date_str}&dateTo={next_day_str}"
    
    print(f"DEBUG: data_collection.py - get_matches_for_date - Requesting URL: {api_url} ...")

    conn = None
    inserted_count = 0
    updated_count = 0
    try:
        response = _make_api_request_with_retry(api_url, headers=headers)
        data = response.json()
        matches_api_data = data.get("matches", [])

        if not matches_api_data:
            print(f"INFO: No matches found for date {date_str} from football-data.org.")
            return 0, 0

        conn = create_connection(DATABASE_FILE)
        if conn is None:
            print("Error: Could not connect to the database. Aborting match processing.")
            return 0, 0
        
        cursor = conn.cursor()

        for match_data in matches_api_data:
            try:
                competition = match_data.get('competition', {})
                league_name = competition.get('name')
                league_source_id = str(competition.get('id'))
                league_country = competition.get('area', {}).get('name')
                league_id = _get_or_create_entity_id(conn, 'league', league_name, 'football', league_source_id, league_country)

                home_team_api = match_data.get('homeTeam', {})
                home_team_name = home_team_api.get('name')
                home_team_source_id = str(home_team_api.get('id'))
                home_team_id = _get_or_create_entity_id(conn, 'team', home_team_name, 'football', home_team_source_id)

                away_team_api = match_data.get('awayTeam', {})
                away_team_name = away_team_api.get('name')
                away_team_source_id = str(away_team_api.get('id'))
                away_team_id = _get_or_create_entity_id(conn, 'team', away_team_name, 'football', away_team_source_id)

                if not league_id or not home_team_id or not away_team_id:
                    print(f"Skipping match due to missing league/team ID. API Match ID: {match_data.get('id')}")
                    continue

                match_datetime_utc = match_data.get('utcDate')
                status = match_data.get('status')
                home_score = match_data.get('score', {}).get('fullTime', {}).get('home')
                away_score = match_data.get('score', {}).get('fullTime', {}).get('away')
                winner = match_data.get('score', {}).get('winner')
                stage = match_data.get('stage')
                matchday = match_data.get('matchday')
                source_match_id = str(match_data.get('id'))
                source_name = 'football-data.org'
                is_mock_value = 0 

                # UPSERT logic
                cursor.execute("SELECT match_id, status, home_score, away_score, match_datetime_utc FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
                existing_match = cursor.fetchone()

                payload = {
                    "league_id": league_id, "home_team_id": home_team_id, "away_team_id": away_team_id,
                    "match_datetime_utc": match_datetime_utc, "status": status,
                    "home_score": home_score, "away_score": away_score, "winner": winner,
                    "stage": stage, "matchday": matchday, "source_match_id": source_match_id,
                    "source_name": source_name, "is_mock": is_mock_value
                }
                
                if existing_match:
                    match_db_id, old_status, old_hs, old_as, old_dt = existing_match
                    if (old_status != status or old_hs != home_score or old_as != away_score or old_dt != match_datetime_utc):
                        update_cols = {k: v for k, v in payload.items() if k not in ['source_match_id', 'source_name']} # Don't update source identifiers
                        set_clause = ", ".join([f"{col} = ?" for col in update_cols.keys()])
                        params = list(update_cols.values()) + [match_db_id]
                        cursor.execute(f"UPDATE matches SET {set_clause} WHERE match_id = ?", params)
                        if cursor.rowcount > 0: updated_count += 1
                else:
                    cols = ", ".join(payload.keys())
                    placeholders = ", ".join(["?"] * len(payload))
                    cursor.execute(f"INSERT INTO matches ({cols}) VALUES ({placeholders})", list(payload.values()))
                    if cursor.lastrowid: inserted_count += 1
                    else: print(f"Warning: Inserted match for {source_match_id} but lastrowid is 0.")


            except sqlite3.Error as e:
                print(f"Database error processing match {match_data.get('id')} from football-data.org: {e}")
            except Exception as ex: 
                print(f"Error processing match data for {match_data.get('id')}: {ex}")
        
        conn.commit()
        print(f"football-data.org for {date_str}: {inserted_count} new, {updated_count} updated.")
        return inserted_count, updated_count

    except requests.exceptions.SSLError as e:
        print(f"ERROR: An SSL error occurred while contacting football-data.org: {e}")
        return 0, 0
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching data from football-data.org: {e}")
        if hasattr(e, 'response') and e.response is not None: # Check if response attribute exists
            if e.response.status_code == 401: print(f"ERROR: Invalid or unauthorized API key for football-data.org (HTTP 401).")
            elif e.response.status_code == 403: print(f"ERROR: Forbidden access to football-data.org API (HTTP 403).")
            else: print(f"API Error Details: Status Code: {e.response.status_code}, Response Text: {e.response.text}")
        return 0, 0
    except ValueError as e: 
        print(f"Error decoding JSON response: {e}")
        return 0, 0
    except Exception as e: 
        print(f"An unexpected error occurred in get_matches_for_date: {e}")
        return 0, 0
    finally:
        if conn:
            conn.close()

def _process_and_insert_matches(conn, matches_data_list, source_name_identifier: str, is_mock_data: bool = False) -> tuple[int, int]:
    """
    Helper function to process a list of match data (either from API or mock file) and insert/update into the database.
    `source_name_identifier` is used to determine how to parse the match_data.
    `is_mock_data` flag indicates if the data is mock.
    Returns a tuple: (inserted_matches_count, updated_matches_count).
    """
    inserted_count = 0
    updated_count = 0
    if not conn:
        print("Database connection not provided to _process_and_insert_matches.")
        return 0
    
    cursor = conn.cursor()

    for match_data in matches_data_list:
        try:
            league_id, home_team_id, away_team_id = None, None, None
            match_datetime_utc, status, home_score, away_score, winner, stage, matchday, source_match_id = \
                None, None, None, None, None, None, None, None

            if source_name_identifier == 'football-data.org' or source_name_identifier == 'mock-football-data.org': # Mock data is assumed to follow football-data.org structure
                competition = match_data.get('competition', {})
                league_name = competition.get('name')
                league_source_id = str(competition.get('id')) if competition.get('id') else None
                league_country = competition.get('area', {}).get('name')
                league_id = _get_or_create_entity_id(conn, 'league', league_name, 'football', league_source_id, league_country)

                home_team_api = match_data.get('homeTeam', {})
                home_team_name = home_team_api.get('name')
                home_team_source_id = str(home_team_api.get('id')) if home_team_api.get('id') else None
                home_team_id = _get_or_create_entity_id(conn, 'team', home_team_name, 'football', home_team_source_id)

                away_team_api = match_data.get('awayTeam', {})
                away_team_name = away_team_api.get('name')
                away_team_source_id = str(away_team_api.get('id')) if away_team_api.get('id') else None
                away_team_id = _get_or_create_entity_id(conn, 'team', away_team_name, 'football', away_team_source_id)

                match_datetime_utc = match_data.get('utcDate')
                status = match_data.get('status')
                home_score = match_data.get('score', {}).get('fullTime', {}).get('home')
                away_score = match_data.get('score', {}).get('fullTime', {}).get('away')
                winner = match_data.get('score', {}).get('winner')
                stage = match_data.get('stage')
                matchday = match_data.get('matchday')
                source_match_id = str(match_data.get('id')) if match_data.get('id') else f"mock_{match_datetime_utc}_{home_team_name}_{away_team_name}" # Ensure mock has a source_id
                # Actual source name for DB
                db_source_name = 'football-data.org' if source_name_identifier == 'football-data.org' else source_name_identifier
                # is_mock is determined by the source_name_identifier or passed param
                current_is_mock_value = 1 if 'mock' in source_name_identifier.lower() or is_mock_data else 0


            elif source_name_identifier == 'api-sports':
                league_api = match_data.get('league', {})
                league_name = league_api.get('name')
                league_source_id = str(league_api.get('id'))
                league_country = league_api.get('country')
                league_id = _get_or_create_entity_id(conn, 'league', league_name, 'football', league_source_id, league_country)

                home_team_api = match_data.get('teams', {}).get('home', {})
                home_team_name = home_team_api.get('name')
                home_team_source_id = str(home_team_api.get('id'))
                home_team_id = _get_or_create_entity_id(conn, 'team', home_team_name, 'football', home_team_source_id)

                away_team_api = match_data.get('teams', {}).get('away', {})
                away_team_name = away_team_api.get('name')
                away_team_source_id = str(away_team_api.get('id'))
                away_team_id = _get_or_create_entity_id(conn, 'team', away_team_name, 'football', away_team_source_id)
                
                fixture = match_data.get('fixture', {})
                timestamp = fixture.get('timestamp')
                match_datetime_utc = datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None
                status = fixture.get('status', {}).get('long')
                home_score = match_data.get('goals', {}).get('home')
                away_score = match_data.get('goals', {}).get('away')
                
                home_won = home_team_api.get('winner')
                away_won = away_team_api.get('winner')
                if home_won is True: winner = 'HOME_TEAM'
                elif away_won is True: winner = 'AWAY_TEAM'
                elif home_score is not None and away_score is not None and home_score == away_score : winner = 'DRAW'
                else: winner = None # Or 'SCHEDULED', 'PENDING' etc. based on status

                stage = league_api.get('round') # API-Sports calls it 'round'
                matchday = None # Not directly available in this part of api-sports fixture, might be part of round string.
                source_match_id = str(fixture.get('id'))
                db_source_name = 'api-sports'
                current_is_mock_value = 1 if is_mock_data else 0 # api-sports live data is not mock unless explicitly stated


            else:
                print(f"Unsupported source_name_identifier: {source_name_identifier} in _process_and_insert_matches.")
                continue

            if not league_id or not home_team_id or not away_team_id:
                print(f"Skipping match due to missing league/team ID. Source Match ID: {source_match_id} from {db_source_name}")
                continue
            if not source_match_id:
                print(f"Skipping match due to missing source_match_id. League: {league_name}, Home: {home_team_name}, Away: {away_team_name}")
                continue


            cursor.execute("""
                INSERT OR IGNORE INTO matches 
                (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, stage, matchday, source_match_id, source_name, is_mock)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, stage, matchday, source_match_id, db_source_name, current_is_mock_value))
            
            # Check if insert happened or if it was ignored because it exists.
            # For INSERT OR IGNORE, cursor.rowcount is 1 for successful insert, 0 if ignored.
            # So, we need to verify existence if rowcount is 0.
            if cursor.rowcount > 0:
                processed_count += 1
            else: # rowcount == 0, could be IGNORE or an issue
                # Verify if the match now exists (it should if IGNOREd due to duplication)
                check_cursor = conn.cursor()
                check_cursor.execute("SELECT 1 FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, db_source_name))
                if check_cursor.fetchone():
                    processed_count +=1 # Match exists (was ignored or inserted by another concurrent process), count it.
                    print(f"INFO: Match {source_match_id} from {db_source_name} was ignored, likely already exists.")
                else:
                    # This is unexpected: INSERT OR IGNORE resulted in rowcount 0 AND the row isn't there.
                    print(f"WARNING: Match {source_match_id} from {db_source_name} insert OR ignore failed, row not found after operation.")


        except sqlite3.Error as e:
            print(f"Database error processing match {source_match_id} from {source_name_identifier}: {e}")
        except Exception as ex:
            print(f"Error processing match data for {source_match_id} from {source_name_identifier}: {ex}")
            
    if processed_count > 0:
        try:
            conn.commit()
            print(f"Successfully committed {processed_count} matches from {source_name_identifier}.")
        except sqlite3.Error as e:
            print(f"Database commit error after processing matches from {source_name_identifier}: {e}")
            # Potentially rollback or handle, but commit failure is serious.
            return 0 # Indicate that commit failed, so effectively 0 were successfully processed and persisted.
            
    return processed_count


def get_matches_with_fallback(date_str: str, use_mock_data: bool = False, api_key: str = FOOTBALL_DATA_API_KEY) -> int:
    """
    Fetches matches for a specific date, stores them in DB. Falls back to mock data if live data fails.
    Returns the total number of matches processed (live or mock).
    """
    live_processed_count = get_matches_for_date(date_str, api_key)

    if live_processed_count > 0:
        return live_processed_count
    
    if use_mock_data:
        print(f"INFO: No live match data processed for {date_str} from football-data.org. Falling back to mock data.")
        conn = None
        try:
            with open(MOCK_DATA_PATH, 'r') as f:
                mock_data_content = json.load(f)
            
            mock_matches_list = mock_data_content.get("matches", [])
            if not mock_matches_list:
                print("INFO: Mock data file loaded, but no matches found within it.")
                return 0

            conn = create_connection(DATABASE_FILE)
            if conn is None:
                print("Error: Could not connect to the database for mock data processing.")
                return 0
            
            # Use the helper to process these mock matches
            # Assuming mock data follows football-data.org structure
            # Pass is_mock_data=True for mock data
            mock_processed_count = _process_and_insert_matches(conn, mock_matches_list, 'mock-football-data.org', is_mock_data=True)
            
            print(f"Processed {mock_processed_count} mock matches from mock data file.")
            return mock_processed_count

        except FileNotFoundError:
            print(f"ERROR: Mock data file not found at {MOCK_DATA_PATH}.")
            return 0
        except json.JSONDecodeError:
            print(f"ERROR: Error decoding mock data from {MOCK_DATA_PATH}.")
            return 0
        except Exception as e:
            print(f"An unexpected error occurred during mock data processing: {e}")
            return 0
        finally:
            if conn:
                conn.close()
    
    return 0 # No live data, and mock data not used or failed.

if __name__ == "__main__":
    today_str = datetime.now().strftime("%Y-%m-%d")
    # yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"--- Attempting to fetch and store matches for today: {today_str} using football-data.org ---")
    # Set use_mock_data to True if you want to use mock data on failure or if no real API key
    # For this example, let's assume we want to try live first, then mock if live fails or API key is placeholder
    use_mock = (FOOTBALL_DATA_API_KEY == "YOUR_API_TOKEN") 
    
    processed_football_data = get_matches_with_fallback(today_str, use_mock_data=True, api_key=FOOTBALL_DATA_API_KEY)
    print(f"Football-data.org (with fallback): Processed {processed_football_data} matches for {today_str}.\n")

    print(f"--- Attempting to fetch and store matches for today: {today_str} using API-SPORTS ---")
    processed_apisports = get_matches_from_apisports(today_str) # This function handles its own API key check
    print(f"API-SPORTS: Processed {processed_apisports} matches for {today_str}.\n")

    # Verify data insertion
    print("--- Verifying data insertion (first 5 matches from DB) ---")
    conn = None
    try:
        conn = create_connection(DATABASE_FILE)
        if conn:
            cursor = conn.cursor()
            # Include is_mock in the verification query
            cursor.execute("SELECT match_id, league_id, home_team_id, away_team_id, match_datetime_utc, status, source_name, source_match_id, is_mock FROM matches ORDER BY match_id DESC LIMIT 5")
            rows = cursor.fetchall()
            if rows:
                print("Last 5 inserted/updated matches (with is_mock flag):")
                for row in rows:
                    print(row)
            else:
                print("No matches found in the database.")
            
            # Example: Count total matches for the day from all sources
            cursor.execute("SELECT COUNT(*) FROM matches WHERE DATE(match_datetime_utc) = ?", (today_str,))
            count_today = cursor.fetchone()[0]
            print(f"\nTotal matches found in DB for {today_str} from all sources: {count_today}")

            cursor.execute("SELECT league_id, name, sport, country, source_league_id FROM leagues LIMIT 5")
            leagues = cursor.fetchall()
            if leagues:
                print("\nSample leagues from DB:")
                for league in leagues:
                    print(league)
            else:
                print("No leagues found in DB.")

            cursor.execute("SELECT team_id, name, sport, source_team_id FROM teams LIMIT 5")
            teams = cursor.fetchall()
            if teams:
                print("\nSample teams from DB:")
                for team in teams:
                    print(team)
            else:
                print("No teams found in DB.")

    except sqlite3.Error as e:
        print(f"Database error during verification: {e}")
    except Exception as e: # Catch-all for other unexpected errors
        print(f"An unexpected error occurred during verification: {e}")
    finally:
        if conn:
            conn.close()


def get_matches_from_apisports(date_str: str, api_key: str = None) -> int:
    """
    Fetches matches for a specific date from API-SPORTS and stores them in the database.
    Returns the number of matches processed.
    """
    resolved_api_key = api_key if api_key is not None else APISPORTS_API_KEY
    if not resolved_api_key:
        print("Warning: API key for API-SPORTS not provided or found in APISPORTS_API_KEY. Cannot fetch matches.")
        return 0

    headers = {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": resolved_api_key,
    }
    api_url = f"{APISPORTS_BASE_URL}fixtures?date={date_str}"
    print(f"DEBUG: data_collection.py - get_matches_from_apisports - Requesting URL: {api_url}")

    conn = None
    processed_count = 0
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 401: print(f"Error fetching data from API-SPORTS: Unauthorized (401). Check your API key."); return 0
        elif response.status_code == 403: print(f"Error fetching data from API-SPORTS: Forbidden (403)."); return 0
        elif response.status_code == 429: print(f"Error fetching data from API-SPORTS: Too Many Requests (429)."); return 0
        response.raise_for_status()
        
        data = response.json()
        matches_api_data = data.get("response", [])

        if not matches_api_data:
            api_errors = data.get("errors")
            if api_errors and (isinstance(api_errors, list) and api_errors or isinstance(api_errors, dict) and api_errors) :
                 print(f"API-SPORTS returned errors: {api_errors}. No matches fetched.")
            else:
                 print(f"INFO: No matches found for date {date_str} from API-SPORTS (API response list was empty).")
            return 0
        
        num_fetched = len(matches_api_data)
        api_reported_count = data.get("results", num_fetched) # Use num_fetched as fallback for "results"
        print(f"INFO: API-SPORTS API reported {api_reported_count} results. Fetched {num_fetched} matches for date {date_str}.")


        conn = create_connection(DATABASE_FILE)
        if conn is None:
            print("Error: Could not connect to the database. Aborting match processing for API-SPORTS.")
            return 0
        
        # Use the common processing function, is_mock_data defaults to False
        processed_count = _process_and_insert_matches(conn, matches_api_data, 'api-sports', is_mock_data=False)
        
        print(f"Successfully processed {processed_count} live matches from API-SPORTS for {date_str}.")
        return processed_count

    except requests.exceptions.RequestException as e:
        if e.response is not None:
            print(f"API Error Details (API-SPORTS): Status Code: {e.response.status_code}, Response Text: {e.response.text}")
        print(f"Error fetching data from API-SPORTS: {e}")
        return 0
    except ValueError as e:  # Handles JSON decoding errors
        print(f"Error decoding JSON response from API-SPORTS: {e}")
        return 0
    except Exception as e: # Catch-all for other unexpected errors
        print(f"An unexpected error occurred in get_matches_from_apisports: {e}")
        return 0
    finally:
        if conn: # Connection might not have been initialized if API call failed early
            conn.close()
