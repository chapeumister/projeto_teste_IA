import sqlite3
import requests
import json
import os
import time
import yaml 
import requests # For tenacity retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type # For tenacity
from datetime import datetime, timezone
import pandas as pd # For pd.isna in _get_or_create_entity_id

# Assuming database_setup.py is in the same directory or accessible via python path
try:
    from database_setup import DATABASE_FILE, create_connection
except ImportError:
    # Fallback if running script directly and database_setup is one level up
    from sports_prediction_ai.src.database_setup import DATABASE_FILE, create_connection

# Attempt to import _get_or_create_entity_id, can be copied if not found
try:
    # Assuming data_collection.py is in the same src directory
    from data_collection import _get_or_create_entity_id
except ImportError:
    print("WARNING: Could not import _get_or_create_entity_id from data_collection.py. Using fallback.")
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
            else: 
                print(f"ERROR (fallback): Failed to get ID for {entity_type} '{name}' after IntegrityError.")
                return None
        except sqlite3.Error as e:
            print(f"Database error in fallback _get_or_create_entity_id for {entity_type} '{name}': {e}")
            return None

# Global config variables, loaded in main() or at script start
CONFIG = {}
API_KEY_THESPORTSDB = os.getenv("THESPORTSDB_API_KEY", "1") # "1" is a common test key
BASE_API_URL_TEMPLATE = "" # Loaded from config, should include {api_key} placeholder
THE_SPORTS_DB_API_URL = "" # Constructed URL with API key
API_CALL_DELAY_SECONDS = 3   # Default, will be loaded from config

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
def _make_api_request_with_retry(url: str, method: str = 'GET', params: dict = None, data: dict = None, headers: dict = None):
    """
    Makes an API request with retry logic using tenacity.
    """
    if not THE_SPORTS_DB_API_URL: # This check might be redundant if url is always constructed with it
        print("ERROR: THE_SPORTS_DB_API_URL is not configured.")
        raise ValueError("THE_SPORTS_DB_API_URL is not configured.") # Raise error to stop execution if basic config missing

    # The `url` parameter passed to this function should be the full URL.
    # If it's just an endpoint, it needs to be prefixed with THE_SPORTS_DB_API_URL.
    # Assuming `url` is already the full path here.
    
    response = requests.request(method, url, params=params, json=data, headers=headers, timeout=20)
    response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
    return response


def fetch_events(league_id: str, past_or_future: str = 'future'):
    """
    Fetches events for a league using tenacity for retries.
    past_or_future: 'future' for eventsnextleague.php, 'past' for eventspastleague.php
    """
    if past_or_future == 'future':
        endpoint = "eventsnextleague.php"
    elif past_or_future == 'past':
        endpoint = "eventspastleague.php" # Fetches last 15 events
    else:
        print(f"ERROR: Invalid 'past_or_future' value: {past_or_future}. Must be 'future' or 'past'.")
        return None
    
    params = {"id": league_id}
    full_url = f"{THE_SPORTS_DB_API_URL}{endpoint}" # Construct full URL here
    print(f"INFO: Fetching {past_or_future} events for league ID {league_id} from endpoint {full_url}")
    
    try:
        response = _make_api_request_with_retry(url=full_url, params=params)
        data = response.json()
        # Key is 'events' for future, 'results' for past events in TheSportsDB
        return data.get('events') if past_or_future == 'future' else data.get('results')
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP error for {full_url} with params {params} after retries: {e}")
    except requests.exceptions.RequestException as e: # Catch other request exceptions
        print(f"ERROR: Request error for {full_url} with params {params} after retries: {e}")
    except json.JSONDecodeError as e: # If response is not JSON
        print(f"ERROR: JSON decoding error for {full_url} with params {params}. Response text: {response.text if 'response' in locals() else 'N/A'}")
    return None

def map_status_thesportsdb(api_status: str, home_score: str, away_score: str) -> str:
    """Maps TheSportsDB status and scores to internal DB status."""
    if api_status == "Match Finished":
        return "FINISHED"
    elif api_status == "Not Started" or api_status == "Match Postponed" or api_status == "Match Cancelled": # Consider how to handle Postponed/Cancelled
        return "SCHEDULED" # Or specific statuses like POSTPONED, CANCELLED if DB schema supports
    elif api_status == "Live":
        return "LIVE"
    # If status is unclear but scores are present, assume finished
    if pd.notna(home_score) and pd.notna(away_score) and home_score is not None and away_score is not None:
        return "FINISHED"
    return "SCHEDULED" # Default or if status is something like "Time To Be Defined"

def process_and_store_events(conn, events_list, sport_name, default_league_name, default_league_id_api, default_country):
    """
    Processes a list of event data from TheSportsDB API and stores it.
    Returns counts of (newly_inserted_matches, updated_matches, new_stats, updated_stats).
    Odds are not typically in these event list endpoints for TheSportsDB.
    """
    if not events_list: # Can be None or empty list
        print(f"INFO: No events provided to process for league {default_league_name}.")
        return 0, 0, 0, 0

    inserted_matches_count = 0
    updated_matches_count = 0
    inserted_stats_count = 0
    updated_stats_count = 0
    
    cursor = conn.cursor()

    # Sport name from TheSportsDB ("Soccer") needs to be mapped to our internal "football" if needed
    db_sport_name = "football" if sport_name.lower() == "soccer" else sport_name.lower()

    league_id = _get_or_create_entity_id(conn, "league", default_league_name, db_sport_name, 
                                         source_id=str(default_league_id_api), country=default_country)
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {default_league_name}. Skipping events for this league.")
        return 0, 0, 0, 0

    for event in events_list:
        try:
            home_team_name = event.get('strHomeTeam')
            away_team_name = event.get('strAwayTeam')
            home_team_source_id = event.get('idHomeTeam')
            away_team_source_id = event.get('idAwayTeam')

            if not home_team_name or not away_team_name or not home_team_source_id or not away_team_source_id:
                print(f"WARNING: Missing team names or API team IDs for event ID {event.get('idEvent')}. Skipping.")
                continue

            home_team_id = _get_or_create_entity_id(conn, "team", home_team_name, db_sport_name, source_id=str(home_team_source_id))
            away_team_id = _get_or_create_entity_id(conn, "team", away_team_name, db_sport_name, source_id=str(away_team_source_id))

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match {home_team_name} vs {away_team_name}. Skipping.")
                continue

            event_date_str = event.get('dateEvent')
            event_time_str = event.get('strTime', "00:00:00") # Default time if missing
            
            if not event_date_str:
                print(f"WARNING: Missing date for event ID {event.get('idEvent')}. Skipping.")
                continue
            
            try:
                if any(c.isalpha() for c in event_time_str): event_time_str = "00:00:00"
                dt_str = f"{event_date_str} {event_time_str}"
                match_datetime_obj = None
                possible_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%y %H:%M:%S"]
                for fmt in possible_formats:
                    try:
                        match_datetime_obj = datetime.strptime(dt_str, fmt)
                        break
                    except ValueError:
                        continue
                if not match_datetime_obj:
                    match_datetime_obj = datetime.strptime(event_date_str, "%Y-%m-%d")
                match_datetime_utc_str = match_datetime_obj.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError as ve:
                print(f"WARNING: Could not parse date/time '{event_date_str} {event_time_str}' for event ID {event.get('idEvent')}: {ve}. Skipping.")
                continue

            home_score_val = event.get('intHomeScore') 
            away_score_val = event.get('intAwayScore')
            if home_score_val is not None: home_score_val = int(home_score_val)
            if away_score_val is not None: away_score_val = int(away_score_val)

            status_api = event.get('strStatus', "Not Started")
            status_db_val = map_status_thesportsdb(status_api, home_score_val, away_score_val)
            
            winner_val = None
            if status_db_val == "FINISHED" and home_score_val is not None and away_score_val is not None:
                if home_score_val > away_score_val: winner_val = 'HOME_TEAM'
                elif away_score_val > home_score_val: winner_val = 'AWAY_TEAM'
                else: winner_val = 'DRAW'

            source_match_id_val = str(event.get('idEvent'))
            source_name_val = 'TheSportsDB'
            stage_val = event.get('strRound')
            is_mock_val = 0 # This script fetches live/historical, not mock

            # UPSERT logic for matches
            cursor.execute("SELECT match_id, status, home_score, away_score, winner, match_datetime_utc FROM matches WHERE source_match_id = ? AND source_name = ?", 
                           (source_match_id_val, source_name_val))
            existing_match_row = cursor.fetchone()
            current_match_db_id = None

            match_payload = {
                "league_id": league_id, "home_team_id": home_team_id, "away_team_id": away_team_id,
                "match_datetime_utc": match_datetime_utc_str, "status": status_db_val,
                "home_score": home_score_val, "away_score": away_score_val, "winner": winner_val,
                "stage": stage_val, "source_match_id": source_match_id_val, "source_name": source_name_val,
                "is_mock": is_mock_val
            }

            if existing_match_row:
                current_match_db_id = existing_match_row[0]
                # Check if update is needed (simplified check, can be more granular)
                if (existing_match_row[1] != status_db_val or 
                    existing_match_row[2] != home_score_val or 
                    existing_match_row[3] != away_score_val or
                    existing_match_row[4] != winner_val or # Compare winner
                    existing_match_row[5] != match_datetime_utc_str): # Compare datetime

                    update_fields_sql = ", ".join([f"{key} = ?" for key in match_payload.keys() if key not in ["source_match_id", "source_name"]]) # Exclude PK-like source fields from SET
                    update_sql = f"UPDATE matches SET {update_fields_sql} WHERE match_id = ?"
                    
                    # Create params list in correct order for match_payload keys, then add match_id
                    update_params = [match_payload[k] for k in match_payload if k not in ["source_match_id", "source_name"]] + [current_match_db_id]
                    
                    cursor.execute(update_sql, update_params)
                    if cursor.rowcount > 0: updated_matches_count += 1
            else:
                insert_cols = ", ".join(match_payload.keys())
                insert_placeholders = ", ".join(["?"] * len(match_payload))
                cursor.execute(f"INSERT INTO matches ({insert_cols}) VALUES ({insert_placeholders})", list(match_payload.values()))
                current_match_db_id = cursor.lastrowid
                if current_match_db_id: inserted_matches_count += 1
                else: print(f"ERROR: Failed to insert new match {source_match_id_val} or get lastrowid."); continue
            
            if current_match_db_id:
                # UPSERT for Stats (e.g., shots)
                stats_to_upsert = []
                home_shots = event.get('intHomeShots')
                away_shots = event.get('intAwayShots')
                if home_shots is not None: stats_to_upsert.append({'team_id': home_team_id, 'type': 'shots_total', 'value': str(home_shots)})
                if away_shots is not None: stats_to_upsert.append({'team_id': away_team_id, 'type': 'shots_total', 'value': str(away_shots)})

                for stat_item in stats_to_upsert:
                    cursor.execute("SELECT stat_id, stat_value FROM stats WHERE match_id = ? AND team_id = ? AND stat_type = ?",
                                   (current_match_db_id, stat_item['team_id'], stat_item['type']))
                    existing_stat = cursor.fetchone()
                    if existing_stat:
                        stat_db_id, old_value = existing_stat
                        if old_value != stat_item['value']:
                            cursor.execute("UPDATE stats SET stat_value = ? WHERE stat_id = ?", (stat_item['value'], stat_db_id))
                            if cursor.rowcount > 0: updated_stats_count += 1
                    else:
                        cursor.execute("INSERT INTO stats (match_id, team_id, stat_type, stat_value) VALUES (?, ?, ?, ?)",
                                       (current_match_db_id, stat_item['team_id'], stat_item['type'], stat_item['value']))
                        if cursor.lastrowid: inserted_stats_count += 1
                
                # Odds UPSERT would go here if odds data were reliably available in these endpoints.
                # For TheSportsDB, odds are typically in event details, not event lists.

        except sqlite3.Error as e:
            print(f"ERROR: Database error processing event ID {event.get('idEvent')}: {e}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing event ID {event.get('idEvent')}: {e}. Data: {event}")
            
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing events for league {default_league_name}: {e}")
        return 0, 0, 0, 0

    print(f"INFO: For league '{default_league_name}': {inserted_matches_count} new matches, {updated_matches_count} updated matches, {inserted_stats_count} new stats, {updated_stats_count} updated stats.")
    return inserted_matches_count, updated_matches_count, inserted_stats_count, updated_stats_count

if __name__ == "__main__":
    print("Starting data collection from TheSportsDB...")
    total_new_matches = 0
    total_updated_matches = 0
    total_new_stats = 0
    total_updated_stats = 0
    # Add total_new_odds, total_updated_odds if odds processing is implemented

    for league_config in leagues_to_track_config:
        league_api_id = league_config.get("id")
        league_name = league_config.get("name")
        sport = league_config.get("sport") # "Soccer", "Basketball" etc.
        country = league_config.get("country")

        if not league_api_id or not league_name or not sport:
            print(f"WARNING: Invalid league configuration entry: {league_config}. Missing 'id', 'name', or 'sport'. Skipping.")
            continue
        
        print(f"\n--- Processing League: {league_name} (ID: {league_api_id}, Sport: {sport}) ---")

        # Fetch and process future events
        print(f"INFO: Fetching future events for {league_name}...")
        future_events_list = fetch_events(league_api_id, 'future')
        if future_events_list:
            im, um, ins, us = process_and_store_events(conn, future_events_list, sport, league_name, league_api_id, country)
            total_new_matches += im
            total_updated_matches += um
            total_new_stats += ins
            total_updated_stats += us
        else:
            print(f"INFO: No future events data returned or error for {league_name}.")
        
        time.sleep(API_CALL_DELAY_SECONDS)

        # Fetch and process past events
        print(f"INFO: Fetching past events for {league_name}...")
        past_events_list = fetch_events(league_api_id, 'past')
        if past_events_list:
            im, um, ins, us = process_and_store_events(conn, past_events_list, sport, league_name, league_api_id, country)
            total_new_matches += im
            total_updated_matches += um
            total_new_stats += ins
            total_updated_stats += us
        else:
            print(f"INFO: No past events data returned or error for {league_name}.")
            
        time.sleep(API_CALL_DELAY_SECONDS) # Delay after each league's full processing too

    print(f"\n--- TheSportsDB Collection Summary ---")
    print(f"Total new matches processed: {total_new_matches}")
    print(f"Total existing matches updated: {total_updated_matches}")
    print(f"Total new stats processed: {total_new_stats}")
    print(f"Total existing stats updated: {total_updated_stats}")
    cursor = conn.cursor()

    # Sport name from TheSportsDB ("Soccer") needs to be mapped to our internal "football" if needed
    db_sport_name = "football" if sport_name.lower() == "soccer" else sport_name.lower()

    league_id = _get_or_create_entity_id(conn, "league", default_league_name, db_sport_name, 
                                         source_id=str(default_league_id_api), country=default_country)
    if not league_id:
        print(f"ERROR: Could not get or create league_id for {default_league_name}. Skipping events for this league.")
        return 0

    for event in events_list:
        try:
            home_team_name = event.get('strHomeTeam')
            away_team_name = event.get('strAwayTeam')
            home_team_source_id = event.get('idHomeTeam')
            away_team_source_id = event.get('idAwayTeam')

            if not home_team_name or not away_team_name or not home_team_source_id or not away_team_source_id:
                print(f"WARNING: Missing team names or API team IDs for event ID {event.get('idEvent')}. Skipping.")
                continue

            home_team_id = _get_or_create_entity_id(conn, "team", home_team_name, db_sport_name, source_id=str(home_team_source_id))
            away_team_id = _get_or_create_entity_id(conn, "team", away_team_name, db_sport_name, source_id=str(away_team_source_id))

            if not home_team_id or not away_team_id:
                print(f"WARNING: Could not get/create team IDs for match {home_team_name} vs {away_team_name}. Skipping.")
                continue

            event_date_str = event.get('dateEvent')
            event_time_str = event.get('strTime', "00:00:00") # Default time if missing
            
            if not event_date_str:
                print(f"WARNING: Missing date for event ID {event.get('idEvent')}. Skipping.")
                continue
            
            # Combine date and time, parse to datetime object
            # TheSportsDB times are often local to the event. Assume UTC if not specified, or parse timezone if available.
            # For simplicity, we'll parse date and time and assume it needs to be localized or is already UTC.
            # The API docs state times are UTC unless specified. Let's treat as UTC.
            try:
                # Handle "TBA" or similar in time string
                if any(c.isalpha() for c in event_time_str): event_time_str = "00:00:00" # Or log as TBA
                
                dt_str = f"{event_date_str} {event_time_str}"
                # Try parsing with common formats, including timezone if present
                match_datetime_obj = None
                possible_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%y %H:%M:%S"] # Add more if other formats appear
                for fmt in possible_formats:
                    try:
                        match_datetime_obj = datetime.strptime(dt_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if not match_datetime_obj: # If all parsing failed
                    # Fallback if time is missing/unparseable, just use date
                    match_datetime_obj = datetime.strptime(event_date_str, "%Y-%m-%d")
                
                # Assume UTC if no timezone info from API. TheSportsDB implies times are UTC.
                match_datetime_utc_str = match_datetime_obj.replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

            except ValueError as ve:
                print(f"WARNING: Could not parse date/time '{event_date_str} {event_time_str}' for event ID {event.get('idEvent')}: {ve}. Skipping.")
                continue

            home_score = event.get('intHomeScore') 
            away_score = event.get('intAwayScore')
            
            # Scores can be None if match not played/finished. Convert to int if not None.
            if home_score is not None: home_score = int(home_score)
            if away_score is not None: away_score = int(away_score)

            status_api = event.get('strStatus', "Not Started")
            status_db = map_status_thesportsdb(status_api, home_score, away_score)
            
            winner = None
            if status_db == "FINISHED" and home_score is not None and away_score is not None:
                if home_score > away_score: winner = 'HOME_TEAM'
                elif away_score > home_score: winner = 'AWAY_TEAM'
                else: winner = 'DRAW'

            source_match_id = str(event.get('idEvent'))
            source_name = 'TheSportsDB'
            stage = event.get('strRound')
            # matchday not directly available in basic event data.

            # Check if match exists
            cursor.execute("SELECT match_id, status FROM matches WHERE source_match_id = ? AND source_name = ?", (source_match_id, source_name))
            existing_match_row = cursor.fetchone()

            if existing_match_row:
                existing_match_db_id, existing_status_db = existing_match_row
                # Update if status has changed (e.g. from SCHEDULED to FINISHED)
                # or if scores are now available and previously weren't (implicit in status change)
                if existing_status_db != status_db or (status_db == "FINISHED" and ( # Check if scores need update
                    conn.execute("SELECT home_score, away_score FROM matches WHERE match_id = ?", (existing_match_db_id,)).fetchone() != (home_score, away_score)
                )):
                    cursor.execute("""
                        UPDATE matches 
                        SET status = ?, home_score = ?, away_score = ?, winner = ?, match_datetime_utc = ?
                        WHERE match_id = ?
                    """, (status_db, home_score, away_score, winner, match_datetime_utc_str, existing_match_db_id))
                    if cursor.rowcount > 0:
                        print(f"INFO: Updated match {source_match_id} (DB ID: {existing_match_db_id}) from {existing_status_db} to {status_db}.")
                        updated_count += 1
                    current_match_db_id = existing_match_db_id
                else: # No change needed
                    processed_count +=1 # Count as processed even if no update needed
                    current_match_db_id = existing_match_db_id

            else: # Insert new match
                cursor.execute("""
                    INSERT INTO matches 
                    (league_id, home_team_id, away_team_id, match_datetime_utc, status, home_score, away_score, winner, stage, source_match_id, source_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (league_id, home_team_id, away_team_id, match_datetime_utc_str, status_db, home_score, away_score, winner, stage, source_match_id, source_name))
                if cursor.lastrowid:
                    processed_count += 1
                    current_match_db_id = cursor.lastrowid
                else: # Should not happen with auto-increment PK if insert was successful
                    print(f"ERROR: Failed to insert new match {source_match_id} and get lastrowid.")
                    continue
            
            # --- Stats and Odds Processing (if current_match_db_id is valid) ---
            if current_match_db_id:
                # Stats (Example: Goal Details, Shots - if available and relevant)
                # TheSportsDB provides goal details as strings: "34':Player1;67':Player2"
                # This is complex to parse reliably into structured stats without more schema.
                # For now, let's try to get simple shot counts if fields exist.
                home_shots = event.get('intHomeShots')
                away_shots = event.get('intAwayShots')
                if home_shots is not None:
                    cursor.execute("INSERT OR IGNORE INTO stats (match_id, team_id, stat_type, stat_value) VALUES (?, ?, ?, ?)",
                                   (current_match_db_id, home_team_id, 'shots_total', str(home_shots)))
                if away_shots is not None:
                    cursor.execute("INSERT OR IGNORE INTO stats (match_id, team_id, stat_type, stat_value) VALUES (?, ?, ?, ?)",
                                   (current_match_db_id, away_team_id, 'shots_total', str(away_shots)))
                
                # Odds (Example - API structure for odds can vary greatly)
                # TheSportsDB might have strHomeTeamOdds, strDrawOdds, strAwayTeamOdds
                # These are not typically in eventsnextleague or eventspastleague. Usually in lookupevent.php
                # For now, this part will be minimal unless fields are confirmed in basic event data.
                # If odds were available, e.g. event.get('strHomeTeamOdds'), parse and insert.

        except sqlite3.Error as e:
            print(f"ERROR: Database error processing event ID {event.get('idEvent')}: {e}")
        except Exception as e:
            print(f"ERROR: Unexpected error processing event ID {event.get('idEvent')}: {e}. Data: {event}")
            
    try:
        conn.commit()
    except sqlite3.Error as e:
        print(f"ERROR: Database commit error after processing events for league {default_league_name}: {e}")
        return 0, 0 

    print(f"INFO: For league '{default_league_name}': {processed_count} new events processed, {updated_count} existing events updated.")
    return processed_count, updated_count

if __name__ == "__main__":
    print("Starting data collection from TheSportsDB...")

    CONFIG = load_config()
    if not CONFIG:
        print("CRITICAL: Failed to load configuration. Exiting.")
        exit(1)

    # Load TheSportsDB specific configurations
    base_url_template_from_config = CONFIG.get('api_base_urls', {}).get('the_sports_db')
    leagues_to_track_config = CONFIG.get('the_sports_db', {}).get('leagues_to_track', [])
    API_CALL_DELAY_SECONDS = CONFIG.get('api_delays', {}).get('the_sports_db', 3) # Use loaded or default

    if not base_url_template_from_config:
        print("CRITICAL: TheSportsDB base URL (the_sports_db) not found in api_base_urls configuration. Exiting.")
        exit(1)
    
    # Construct the final API URL with the key
    # TheSportsDB API key is part of the path, not a query parameter.
    # Example: "https://www.thesportsdb.com/api/v1/json/{API_KEY}/"
    # So, we replace {API_KEY} or just append if the config URL is "https://www.thesportsdb.com/api/v1/json/"
    if "{API_KEY}" in base_url_template_from_config: # If placeholder exists
        THE_SPORTS_DB_API_URL = base_url_template_from_config.replace("{API_KEY}", API_KEY_THESPORTSDB)
    else: # Assume the config URL is the part before the API key, and API key needs to be appended + /
        THE_SPORTS_DB_API_URL = f"{base_url_template_from_config.rstrip('/')}/{API_KEY_THESPORTSDB}/"


    if API_KEY_THESPORTSDB == "1" or API_KEY_THESPORTSDB == "2": # "1" is the public test key
        print("INFO: Using a default test API key for TheSportsDB. For more extensive access, consider registering for a dedicated key.")
    elif not API_KEY_THESPORTSDB or len(API_KEY_THESPORTSDB) < 2: # Basic check if key is empty or just "1"
        print("CRITICAL: THESPORTSDB_API_KEY environment variable not set or invalid, and no valid default provided. Exiting.")
        exit(1)
    
    if not leagues_to_track_config:
        print("INFO: No leagues configured for download in the_sports_db section of config.yaml. Exiting.")
        exit(0)

    conn = create_connection(DATABASE_FILE)
    if not conn:
        print("CRITICAL: Could not establish database connection. Exiting.")
        exit(1)

    total_new_matches = 0
    total_updated_matches = 0

    for league_config in leagues_to_track_config:
        league_api_id = league_config.get("id")
        league_name = league_config.get("name")
        sport = league_config.get("sport") # "Soccer", "Basketball" etc.
        country = league_config.get("country")

        if not league_api_id or not league_name or not sport:
            print(f"WARNING: Invalid league configuration entry: {league_config}. Missing 'id', 'name', or 'sport'. Skipping.")
            continue
        
        print(f"\n--- Processing League: {league_name} (ID: {league_api_id}, Sport: {sport}) ---")

        # Fetch and process future events
        print(f"INFO: Fetching future events for {league_name}...")
        future_events_list = fetch_events(league_api_id, 'future')
        if future_events_list:
            new, updated = process_and_store_events(conn, future_events_list, sport, league_name, league_api_id, country)
            total_new_matches += new
            total_updated_matches += updated
        else:
            print(f"INFO: No future events data returned or error for {league_name}.")
        
        time.sleep(API_CALL_DELAY_SECONDS)

        # Fetch and process past events
        print(f"INFO: Fetching past events for {league_name}...")
        past_events_list = fetch_events(league_api_id, 'past')
        if past_events_list:
            new, updated = process_and_store_events(conn, past_events_list, sport, league_name, league_api_id, country)
            total_new_matches += new
            total_updated_matches += updated
        else:
            print(f"INFO: No past events data returned or error for {league_name}.")
            
        time.sleep(API_CALL_DELAY_SECONDS) # Delay after each league's full processing too

    print(f"\n--- TheSportsDB Collection Summary ---")
    print(f"Total new matches processed: {total_new_matches}")
    print(f"Total existing matches updated: {total_updated_matches}")

    # Verification query
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches WHERE source_name = 'TheSportsDB'")
        count = cursor.fetchone()[0]
        print(f"Verification: Found {count} matches from 'TheSportsDB' in the database.")
    except sqlite3.Error as e:
        print(f"Database error during verification query: {e}")

    if conn:
        conn.close()
    print("Finished TheSportsDB data collection.")

```
