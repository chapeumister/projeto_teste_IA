import sqlite3
import requests
import json
import os
import yaml 
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type 
from ratelimiter import RateLimiter # Added for rate limiting
from datetime import datetime

# Assuming database_setup.py is in the same directory or accessible via python path
try:
    from database_setup import DATABASE_FILE, create_connection
except ImportError:
    # Fallback if running script directly and database_setup is one level up
    from sports_prediction_ai.src.database_setup import DATABASE_FILE, create_connection

# Attempt to import _get_or_create_entity_id from data_collection.py
# FOOTBALL_DATA_API_KEY will remain loaded from env or data_collection's fallback.
# FOOTBALL_DATA_BASE_URL will be loaded from config.yaml.
try:
    from data_collection import FOOTBALL_DATA_API_KEY, _get_or_create_entity_id
except ImportError:
    print("WARNING: Could not import _get_or_create_entity_id or FOOTBALL_DATA_API_KEY from data_collection.py. Using fallbacks.")
    FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_FALLBACK_API_TOKEN_HISTORY")
    # Fallback _get_or_create_entity_id definition
    def _get_or_create_entity_id(conn, entity_type: str, name: str, sport: str, source_id: str = None, country: str = None) -> int:
        if not name or not sport:
            print(f"Warning (fallback _get_or_create_entity_id): Name and sport are required for {entity_type}.")
            return None
        cursor = conn.cursor()
        table_name = "leagues" if entity_type == "league" else "teams"
        id_column = "league_id" if entity_type == "league" else "team_id"
        source_id_column = "source_league_id" if entity_type == "league" else "source_team_id"
        try:
            cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
            row = cursor.fetchone()
            if row:
                conn.commit() 
                return row[0]
            else:
                if entity_type == "league":
                    sql = f"INSERT OR IGNORE INTO {table_name} (name, sport, country, {source_id_column}) VALUES (?, ?, ?, ?)"
                    params = (name, sport, country, source_id)
                else:
                    sql = f"INSERT OR IGNORE INTO {table_name} (name, sport, {source_id_column}) VALUES (?, ?, ?)"
                    params = (name, sport, source_id)
                cursor.execute(sql, params)
                if cursor.lastrowid == 0: # IGNORE case
                    cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE name = ? AND sport = ?", (name, sport))
                    row = cursor.fetchone()
                    if row: conn.commit(); return row[0]
                conn.commit()
                return cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Database error in fallback _get_or_create_entity_id for {entity_type} '{name}': {e}")
            conn.rollback() 
            return None

# Global config variable
CONFIG = {}
FOOTBALL_DATA_BASE_URL = "" # Will be loaded from CONFIG

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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def _make_api_request_with_retry(url, method='GET', headers=None, params=None, data=None):
    response = requests.request(method, url, headers=headers, params=params, data=data, timeout=20)
    response.raise_for_status() 
    return response

def fetch_historical_matches_for_competition_season(api_key, competition_code, season_year):
    """
    Fetches historical matches for a given competition and season using tenacity for retries.
    """
    if not FOOTBALL_DATA_BASE_URL:
        print("ERROR: FOOTBALL_DATA_BASE_URL not loaded from config.")
        return None
    url = f"{FOOTBALL_DATA_BASE_URL}competitions/{competition_code}/matches?season={season_year}"
    headers = {"X-Auth-Token": api_key}
    print(f"INFO: Fetching data from {url} for season {season_year}")

    try:
        response = _make_api_request_with_retry(url, headers=headers)
        data = response.json()
        return data.get('matches', [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
             print(f"ERROR: HTTP 403 Forbidden for {url} after retries. Check API key permissions or rate limits.")
        elif e.response.status_code == 404:
             print(f"ERROR: HTTP 404 Not Found for {url} after retries. Competition/season might not exist or not be available.")
        else:
            print(f"ERROR: HTTP error fetching data for {competition_code} season {season_year} after retries: {e}")
        return None
    except requests.exceptions.RequestException as e: 
        print(f"ERROR: Request error for {competition_code} season {season_year} after retries: {e}")
        return None
    except json.JSONDecodeError as e: 
        print(f"ERROR: JSON decoding error for {competition_code} season {season_year}: {e}")
        return None

def process_and_store_matches(conn, matches_api_data, default_league_name, default_league_api_code, default_country, sport='football'):
    """
    Processes a list of match data from the API and stores it in the database using UPSERT logic.
    Returns counts of (newly_inserted_matches, updated_matches, new_odds, updated_odds).
    """
    if not matches_api_data:
        return 0, 0, 0, 0

    inserted_matches_count = 0
    updated_matches_count = 0
    inserted_odds_count = 0
    updated_odds_count = 0
    cursor = conn.cursor()

    league_id = _get_or_create_entity_id(conn, "league", default_league_name, sport, source_id=default_league_api_code, country=default_country)
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {default_league_name}. Skipping all matches for this batch.")
        return 0, 0, 0, 0
    
    for match_data in matches_api_data:
        try:
            home_team_api = match_data.get('homeTeam', {})
            away_team_api = match_data.get('awayTeam', {})
            
            home_team_name = home_team_api.get('name')
            home_team_source_id = str(home_team_api.get('id')) if home_team_api.get('id') else None
            
            away_team_name = away_team_api.get('name')
            away_team_source_id = str(away_team_api.get('id')) if away_team_api.get('id') else None

            if not home_team_name or not away_team_name:
                print(f"WARNING: Missing team name(s) for match ID {match_data.get('id')}. Skipping.")
                continue

            home_team_id = _get_or_create_entity_id(conn, 'team', home_team_name, sport, source_id=home_team_source_id)
            away_team_id = _get_or_create_entity_id(conn, 'team', away_team_name, sport, source_id=away_team_source_id)

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match {home_team_name} vs {away_team_name} (API Match ID: {match_data.get('id')}). Skipping.")
                continue

            match_datetime_utc_str = match_data.get('utcDate')
            status = match_data.get('status')
            
            home_score, away_score, winner = None, None, None
            score_data = match_data.get('score', {})
            if score_data and score_data.get('fullTime'):
                home_score = score_data['fullTime'].get('home') 
                away_score = score_data['fullTime'].get('away')
                if home_score is not None: home_score = int(home_score)
                if away_score is not None: away_score = int(away_score)

            if home_score is not None and away_score is not None:
                 if home_score > away_score: winner = 'HOME_TEAM'
                 elif away_score > home_score: winner = 'AWAY_TEAM'
                 else: winner = 'DRAW'
            elif status == "FINISHED": 
                 print(f"WARNING: Match {match_data.get('id')} is FINISHED but has no scores. Winner set to None.")

            stage = match_data.get('stage')
            matchday = match_data.get('matchday')
            source_match_id = str(match_data.get('id'))
            source_name = 'football-data.org-history'
            is_mock_value = 0 

            cursor.execute("SELECT match_id, status, home_score, away_score, match_datetime_utc FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
            existing_match_row = cursor.fetchone()
            current_match_db_id = None

            match_fields_to_update = {
                "league_id": league_id, "home_team_id": home_team_id, "away_team_id": away_team_id,
                "match_datetime_utc": match_datetime_utc_str, "status": status,
                "home_score": home_score, "away_score": away_score, "winner": winner,
                "stage": stage, "matchday": matchday, "is_mock": is_mock_value
            }

            if existing_match_row:
                current_match_db_id = existing_match_row[0]
                needs_update = (
                    existing_match_row[1] != status or
                    existing_match_row[2] != home_score or 
                    existing_match_row[3] != away_score or
                    existing_match_row[4] != match_datetime_utc_str
                )
                if needs_update:
                    update_sql = "UPDATE matches SET " + ", ".join([f"{field} = ?" for field in match_fields_to_update.keys()]) + " WHERE match_id = ?"
                    update_params = list(match_fields_to_update.values()) + [current_match_db_id]
                    cursor.execute(update_sql, update_params)
                    if cursor.rowcount > 0: updated_matches_count += 1
            else:
                insert_fields = list(match_fields_to_update.keys()) + ["source_match_id", "source_name"]
                insert_placeholders = ", ".join(["?"] * len(insert_fields))
                insert_values = list(match_fields_to_update.values()) + [source_match_id, source_name]
                insert_sql = f"INSERT INTO matches ({', '.join(insert_fields)}) VALUES ({insert_placeholders})"
                cursor.execute(insert_sql, insert_values)
                current_match_db_id = cursor.lastrowid
                if current_match_db_id:
                    inserted_matches_count += 1
                else:
                    cursor.execute("SELECT match_id FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
                    refetched_row = cursor.fetchone()
                    if refetched_row: current_match_db_id = refetched_row[0]; inserted_matches_count +=1
                    else: print(f"ERROR: Failed to get match_id for new match {source_match_id}"); continue

            if current_match_db_id:
                odds_data = match_data.get('odds')
                if odds_data and isinstance(odds_data, dict) and odds_data.get('msg') != 'Activate Odds-Package in User-Panel to retrieve odds.':
                    home_odds_val = odds_data.get('homeWin') 
                    draw_odds_val = odds_data.get('draw')   
                    away_odds_val = odds_data.get('awayWin') 
                    bookmaker = 'football-data.org_API'
                    market_type = '1X2'

                    if home_odds_val is not None and draw_odds_val is not None and away_odds_val is not None:
                        try:
                            home_odds_float, draw_odds_float, away_odds_float = float(home_odds_val), float(draw_odds_val), float(away_odds_val)
                            cursor.execute("SELECT odd_id, home_odds, draw_odds, away_odds FROM odds WHERE match_id = ? AND bookmaker = ? AND market_type = ?",
                                           (current_match_db_id, bookmaker, market_type))
                            existing_odd_row = cursor.fetchone()
                            if existing_odd_row:
                                odd_db_id, old_h, old_d, old_a = existing_odd_row
                                if old_h != home_odds_float or old_d != draw_odds_float or old_a != away_odds_float: 
                                    cursor.execute("""UPDATE odds SET home_odds = ?, draw_odds = ?, away_odds = ?, timestamp_utc = ? 
                                                      WHERE odd_id = ?""", 
                                                   (home_odds_float, draw_odds_float, away_odds_float, match_datetime_utc_str, odd_db_id))
                                    if cursor.rowcount > 0: updated_odds_count +=1
                            else:
                                cursor.execute("""INSERT INTO odds (match_id, bookmaker, market_type, home_odds, draw_odds, away_odds, timestamp_utc)
                                                  VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                               (current_match_db_id, bookmaker, market_type, home_odds_float, draw_odds_float, away_odds_float, match_datetime_utc_str))
                                if cursor.lastrowid: inserted_odds_count += 1
                        except (ValueError, TypeError) as ve:
                            print(f"WARNING: Could not parse or process odds for match {source_match_id}: {ve}. Odds data: {odds_data}")
                        except sqlite3.Error as sqle:
                             print(f"WARNING: DB error processing odds for match {source_match_id}: {sqle}")
            else:
                 print(f"WARNING: No valid match_db_id for match {source_match_id}, cannot process odds.")
        except sqlite3.Error as e:
            print(f"ERROR: Database error processing API match ID {match_data.get('id')}: {e}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing API match ID {match_data.get('id')}: {e}. Data: {match_data}")
            
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing batch for league {default_league_name}: {e}")
        return 0,0,0,0 

    return inserted_matches_count, updated_matches_count, inserted_odds_count, updated_odds_count

if __name__ == "__main__":
    print("Starting historical data collection from football-data.org...")

    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        exit(1)

    FOOTBALL_DATA_BASE_URL = CONFIG.get('api_base_urls', {}).get('football_data_org')
    historical_competitions_config = CONFIG.get('football_data_org_history', {}).get('competitions', [])
    # api_call_delay_seconds is no longer needed from config, RateLimiter handles this.
    # Default RateLimiter settings, can be overridden by config if desired later.
    # For football-data.org free tier (10 calls/minute), 9 calls per 60s is safer.
    rate_limiter_max_calls = CONFIG.get('api_delays', {}).get('football_data_org_max_calls', 9)
    rate_limiter_period = CONFIG.get('api_delays', {}).get('football_data_org_period', 60)


    if not FOOTBALL_DATA_BASE_URL:
        print("CRITICAL: football_data_org base URL not found in configuration. Exiting.")
        exit(1)
    if not historical_competitions_config:
        print("INFO: No historical competitions configured in football_data_org_history section. Exiting.")
        exit(0)

    if not FOOTBALL_DATA_API_KEY or FOOTBALL_DATA_API_KEY == "YOUR_FALLBACK_API_TOKEN_HISTORY" or FOOTBALL_DATA_API_KEY == "YOUR_API_TOKEN":
        print("CRITICAL: Valid FOOTBALL_DATA_API_KEY not found or is set to a placeholder. Please set it as an environment variable or in data_collection.py.")
        exit(1)

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        exit(1)

    total_new_matches_all = 0
    total_updated_matches_all = 0
    total_new_odds_all = 0
    total_updated_odds_all = 0

    # Initialize RateLimiter
    rate_limiter = RateLimiter(max_calls=rate_limiter_max_calls, period=rate_limiter_period)

    for competition in historical_competitions_config:
        comp_code = competition.get("code")
        comp_name = competition.get("name")
        comp_country = competition.get("country")
        seasons = competition.get("seasons", [])

        if not comp_code or not comp_name:
            print(f"WARNING: Invalid competition configuration entry: {competition}. Missing 'code' or 'name'. Skipping.")
            continue
        
        print(f"\n--- Processing Competition: {comp_name} ({comp_code}) ---")
        for season in seasons:
            api_matches_data = None
            with rate_limiter:
                print(f"Fetching matches for season: {season} (Rate limit: {rate_limiter_max_calls} calls / {rate_limiter_period}s)")
                api_matches_data = fetch_historical_matches_for_competition_season(FOOTBALL_DATA_API_KEY, comp_code, season)
            
            if api_matches_data is not None:
                if api_matches_data: 
                    inserted_m, updated_m, inserted_o, updated_o = process_and_store_matches(conn, api_matches_data, comp_name, comp_code, comp_country)
                    print(f"INFO: For {comp_name} season {season}: {inserted_m} new matches, {updated_m} updated matches, {inserted_o} new odds, {updated_o} updated odds.")
                    total_new_matches_all += inserted_m
                    total_updated_matches_all += updated_m
                    total_new_odds_all += inserted_o
                    total_updated_odds_all += updated_o
                else: 
                    print(f"INFO: No matches found by API for {comp_name} season {season}.")
            else: 
                print(f"ERROR: Failed to fetch matches for {comp_name} season {season}. See previous errors.")
            # time.sleep(api_call_delay_seconds) # Removed, RateLimiter handles this.

    print(f"\n--- Historical Data Collection Summary (football-data.org) ---")
    print(f"Total new matches inserted: {total_new_matches_all}")
    print(f"Total existing matches updated: {total_updated_matches_all}")
    print(f"Total new odds inserted: {total_new_odds_all}")
    print(f"Total existing odds updated: {total_updated_odds_all}")

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name = 'football-data.org-history'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from 'football-data.org-history' in the database.")
        
        cursor.execute("""
            SELECT COUNT(*) FROM odds o
            JOIN matches m ON o.match_id = m.match_id
            WHERE m.source_name = 'football-data.org-history' AND o.bookmaker = 'football-data.org_API'
        """)
        odds_count = cursor.fetchone()[0]
        print(f"Verification: Found {odds_count} odds entries from 'football-data.org_API' linked to historical matches.")

    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished historical data collection from football-data.org.")
