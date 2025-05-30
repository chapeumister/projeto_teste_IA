# src/data_collection.py
import os
import requests
import warnings
import json # Added json import
from datetime import datetime, timedelta # Imported timedelta

# It's good practice to load the API key from an environment variable
# or a configuration file, rather than hardcoding it.
# For now, we'll use a placeholder.
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")
APISPORTS_API_KEY = os.getenv("APISPORTS_API_KEY") # New API key
THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY", "1") # Default to "1" (free key)
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4/"
APISPORTS_BASE_URL = "https://v3.football.api-sports.io/" # New base URL
THESPORTSDB_BASE_URL = "https://www.thesportsdb.com/api/v1/json/"


def get_matches_for_date(date_str: str, api_key: str = FOOTBALL_DATA_API_KEY):
    """
    Fetches matches for a specific date from the football-data.org API.

    Args:
        date_str (str): The date in "YYYY-MM-DD" format.
        api_key (str): The API key for football-data.org.

    Returns:
        list: A list of match objects, or an empty list if an error occurs or no matches are found.
    """
    if not api_key or api_key == "YOUR_API_TOKEN":
        print("Error: Invalid or missing API key for football-data.org. Please set the FOOTBALL_DATA_API_KEY environment variable with a valid key.")
        # Return an empty list as per current behavior for placeholder/missing key.
        return []

    headers = {"X-Auth-Token": api_key}
    # Calculate next_day_str
    current_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = current_date + timedelta(days=1)
    next_day_str = next_day.strftime("%Y-%m-%d")

    # The API endpoint for matches can vary; v4 uses /matches.
    # Using dateFrom=date_str and dateTo=next_day_str to get matches for the specified day.
    api_url = f"{FOOTBALL_DATA_BASE_URL}matches?dateFrom={date_str}&dateTo={next_day_str}"
    print(f"DEBUG: data_collection.py - get_matches_for_date - Requesting URL: {api_url} with API Key: {api_key[:4]}...{api_key[-4:] if len(api_key) > 8 else ''}") # Print URL and part of API key

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        data = response.json()
        if data.get("count") == 0:
            print(f"INFO: No matches found for date {date_str} from football-data.org. The API returned a count of 0.")
        return data.get("matches", [])
    except requests.exceptions.SSLError as e:
        print(f"ERROR: An SSL error occurred while contacting football-data.org: {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching data from football-data.org: {e}")
        if e.response is not None:
            if e.response.status_code == 401:
                print(f"ERROR: Invalid or unauthorized API key for football-data.org (HTTP 401). Please check your FOOTBALL_DATA_API_KEY environment variable.")
            elif e.response.status_code == 403:
                print(f"ERROR: Forbidden access to football-data.org API (HTTP 403). Your API key may not have permissions for this endpoint or has been suspended.")
            else:
                # Print detailed API error information first, if available
                print(f"API Error Details: Status Code: {e.response.status_code}")
                print(f"Response Text: {e.response.text}")
        # Then print the general error message
        # This was originally printed before the conditional e.response check, moved it to be a general fallback.
        # However, the initial ERROR line covers the {e} part, so this specific print might be redundant now
        # print(f"Error fetching data from football-data.org: {e}") # Redundant if the first print includes {e}
        return []
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response: {e}")
        return []

MOCK_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mock_matches.json')

def get_matches_with_fallback(date_str: str, use_mock_data: bool = False, api_key: str = FOOTBALL_DATA_API_KEY) -> list:
    """
    Fetches matches for a specific date, with an option to fallback to mock data.

    Args:
        date_str (str): The date in "YYYY-MM-DD" format.
        use_mock_data (bool): If True, falls back to mock data if live data fetching fails or returns no matches.
        api_key (str): The API key for football-data.org.

    Returns:
        list: A list of match objects, or an empty list if an error occurs (and mock data also fails or is not used).
    """
    matches = get_matches_for_date(date_str, api_key)

    if not matches and use_mock_data:
        print(f"INFO: No real match data fetched for {date_str}. Falling back to mock data as requested.")
        try:
            with open(MOCK_DATA_PATH, 'r') as f:
                mock_data = json.load(f)
            return mock_data.get("matches", [])
        except FileNotFoundError:
            print(f"ERROR: Mock data file not found at {MOCK_DATA_PATH}. Returning empty list.")
            return []
        except json.JSONDecodeError:
            print(f"ERROR: Error decoding mock data from {MOCK_DATA_PATH}. Returning empty list.")
            return []
    
    return matches

if __name__ == "__main__":
    # Example usage:
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Fetching matches for today: {today_str}")

    # To actually run this, you would need a valid API key.
    # If FOOTBALL_DATA_API_KEY is not set in your environment, it will use "YOUR_API_TOKEN"
    # and the function will print a warning and return an empty list.
    # Using the new fallback function for the example:
    print("\n--- Example with get_matches_with_fallback (use_mock_data=False) ---")
    matches_live_only = get_matches_with_fallback(today_str, use_mock_data=False)

    if matches_live_only:
        print(f"Found {len(matches_live_only)} live matches:")
        for match in matches_live_only:
            home_team = match.get("homeTeam", {}).get("name", "N/A")
            away_team = match.get("awayTeam", {}).get("name", "N/A")
            competition = match.get("competition", {}).get("name", "N/A")
            match_time_utc = match.get("utcDate", "N/A")
            status = match.get("status", "N/A")
            print(f"- {home_team} vs {away_team} (Competition: {competition}, Time UTC: {match_time_utc}, Status: {status})")
    elif FOOTBALL_DATA_API_KEY != "YOUR_API_TOKEN": # Check if API key was real
        print("No live matches found for today from football-data.org, or there was an issue fetching them.")
    else:
        print("No live matches fetched from football-data.org (API key is placeholder or missing).")

    print("\n--- Example with get_matches_with_fallback (use_mock_data=True) ---")
    # Assuming FOOTBALL_DATA_API_KEY is "YOUR_API_TOKEN" or live call fails, this should use mock.
    matches_with_fallback = get_matches_with_fallback(today_str, use_mock_data=True)
    if matches_with_fallback:
        # Check if these are mock matches
        is_mock = any("Mock" in match.get("homeTeam", {}).get("name", "") for match in matches_with_fallback)
        source = "mock" if is_mock else "live" # Determine source based on content
        print(f"Found {len(matches_with_fallback)} matches (source: {source}):")
        for match in matches_with_fallback:
            home_team = match.get("homeTeam", {}).get("name", "N/A")
            away_team = match.get("awayTeam", {}).get("name", "N/A")
            competition = match.get("competition", {}).get("name", "N/A")
            match_time_utc = match.get("utcDate", "N/A")
            status = match.get("status", "N/A")
            print(f"- {home_team} vs {away_team} (Competition: {competition}, Time UTC: {match_time_utc}, Status: {status})")
    else: # This case should ideally not happen if mock data is present and use_mock_data=True
        print("No matches found from football-data.org, even after attempting fallback to mock data.")


    # Original example for get_matches_for_date (kept for reference or direct testing if needed)
    # print(f"\nFetching matches for today using get_matches_for_date directly: {today_str}")
    # matches = get_matches_for_date(today_str)
    # if matches:
    #     print(f"Found {len(matches)} matches:")
    #     for match in matches:
    #         home_team = match.get("homeTeam", {}).get("name", "N/A")
    #         away_team = match.get("awayTeam", {}).get("name", "N/A")
    #         competition = match.get("competition", {}).get("name", "N/A")
    #         match_time_utc = match.get("utcDate", "N/A")
    #         status = match.get("status", "N/A")
    #         print(f"- {home_team} vs {away_team} (Competition: {competition}, Time UTC: {match_time_utc}, Status: {status})")
    # elif FOOTBALL_DATA_API_KEY != "YOUR_API_TOKEN":
    #     print("No matches found for today, or there was an issue fetching them (and a valid API key seems to be present).")
    # else:
    #     print("No matches fetched. Ensure your FOOTBALL_DATA_API_KEY environment variable is set correctly.")


    # --- Historical Data Fetching Example ---
    print("\n--- Historical Match Data Fetching (football-data.org) ---")
    print("Note: This may be rate-limited or require a paid API plan for extensive use.")

    historical_competitions_seasons = [
        ('PL', 2022),  # English Premier League
        ('BL1', 2022), # German Bundesliga 1
        ('SA', 2022),  # Italian Serie A
        ('PD', 2022),  # Spanish Primera Division (La Liga)
        # ('FL1', 2022), # French Ligue 1 (uncomment to add)
        # ('CL', 2022),  # UEFA Champions League (uncomment to add)
    ]

    # Define base path for saving historical data
    # Assuming script is in sports_prediction_ai/src/
    # Data will be in sports_prediction_ai/data/football_data_org_historical/
    try:
        script_dir_path = os.path.dirname(os.path.abspath(__file__))
        project_root_path = os.path.dirname(script_dir_path)
        historical_data_base_path = os.path.join(project_root_path, 'data', 'football_data_org_historical')
    except NameError: # Fallback for environments where __file__ might not be defined
        print("Warning: __file__ not defined, using current working directory for historical data path relative calculations.")
        historical_data_base_path = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'football_data_org_historical')

    if FOOTBALL_DATA_API_KEY == "YOUR_API_TOKEN" or not FOOTBALL_DATA_API_KEY:
        print("Skipping historical data fetching as FOOTBALL_DATA_API_KEY is placeholder or not set.")
    else:
        for comp_code, season_year in historical_competitions_seasons:
            print(f"\nFetching historical data for {comp_code}, Season {season_year}...")
            matches_historical = get_historical_matches_for_competition(comp_code, season_year)

            if matches_historical:
                print(f"Found {len(matches_historical)} matches for {comp_code}, Season {season_year}.")
                # Save the data
                save_data_to_json(
                    data=matches_historical,
                    base_path=historical_data_base_path,
                    competition_code=comp_code,
                    season=season_year
                )
            else:
                print(f"No historical matches found or error fetching for {comp_code}, Season {season_year}.")

    print(f"\nFetching matches for today using API-SPORTS: {today_str}")
    apisports_matches = get_matches_from_apisports(today_str)

    if apisports_matches:
        print(f"Found {len(apisports_matches)} matches from API-SPORTS:")
        for match in apisports_matches:
            fixture = match.get("fixture", {})
            teams = match.get("teams", {})
            home_team = teams.get("home", {}).get("name", "N/A")
            away_team = teams.get("away", {}).get("name", "N/A")
            competition = match.get("league", {}).get("name", "N/A")
            match_time_unix = fixture.get("timestamp")
            match_time_utc = datetime.utcfromtimestamp(match_time_unix).strftime('%Y-%m-%dT%H:%M:%SZ') if match_time_unix else "N/A"
            status = fixture.get("status", {}).get("long", "N/A")
            print(f"- {home_team} vs {away_team} (Competition: {competition}, Time UTC: {match_time_utc}, Status: {status})")
    elif APISPORTS_API_KEY: # Only print this if the key was actually set
        print("No matches found from API-SPORTS for today, or there was an issue fetching them.")
    else:
        # Warning about missing API key is handled within get_matches_from_apisports
        pass


def get_matches_from_apisports(date_str: str, api_key: str = None):
    """
    Fetches matches for a specific date from the API-SPORTS (api-football.com) API.

    Args:
        date_str (str): The date in "YYYY-MM-DD" format.
        api_key (str, optional): The API key for api-football.com. 
                                 If None, tries to use APISPORTS_API_KEY environment variable.

    Returns:
        list: A list of match objects, or an empty list if an error occurs or no matches are found.
    """
    resolved_api_key = api_key if api_key is not None else APISPORTS_API_KEY

    if not resolved_api_key:
        print("Warning: API key for API-SPORTS not provided or found in APISPORTS_API_KEY environment variable.")
        return []

    headers = {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": resolved_api_key,
    }
    api_url = f"{APISPORTS_BASE_URL}fixtures?date={date_str}"

    try:
        response = requests.get(api_url, headers=headers)
        # Check for common API errors first
        if response.status_code == 401:
            print(f"Error fetching data from API-SPORTS: Unauthorized (401). Check your API key.")
            return []
        elif response.status_code == 403:
            print(f"Error fetching data from API-SPORTS: Forbidden (403). You might not have access to this resource or your API key is invalid/expired.")
            return []
        elif response.status_code == 429:
            print(f"Error fetching data from API-SPORTS: Too Many Requests (429). You may have exceeded your API quota.")
            return []
        
        response.raise_for_status()  # Raises an HTTPError for other bad responses (4XX or 5XX)
        
        data = response.json()

        matches_list = data.get("response", [])
        num_matches = len(matches_list)
        
        api_reported_count = data.get("results") # Primary check for count
        if not isinstance(api_reported_count, int): # Fallback check if "results" is not an int or not present
            paging_info = data.get("paging", {})
            if isinstance(paging_info, dict): # Ensure paging_info is a dictionary
                 api_reported_count = paging_info.get("total")
            else: # paging_info is not a dict, so cannot get "total"
                api_reported_count = None

        if isinstance(api_reported_count, int):
            print(f"INFO: API-SPORTS API reported {api_reported_count} results. Fetched {num_matches} matches for date {date_str}.")
            if api_reported_count == 0 and num_matches == 0: # Specific log if API confirms zero results
                 print(f"INFO: No matches found for date {date_str} from API-SPORTS (API confirmed 0 results).")
            elif num_matches == 0 and api_reported_count > 0:
                 print(f"WARNING: API-SPORTS API reported {api_reported_count} results, but {num_matches} were fetched for date {date_str}. This might indicate an issue with the response list or pagination (if not handled).")
        else: # api_reported_count could not be determined as an integer
            print(f"INFO: Fetched {num_matches} matches from API-SPORTS for date {date_str}. (API reported count not available or not an integer).")
            if num_matches == 0:
                 print(f"INFO: No matches found for date {date_str} from API-SPORTS.")
        
        # Check if the API returned an error message in the JSON body
        # API-SPORTS error responses often contain a 'errors' key which can be a list or dict
        if data.get("errors") and (isinstance(data["errors"], list) and data["errors"] or isinstance(data["errors"], dict) and data["errors"]):
            print(f"API-SPORTS returned errors: {data['errors']}")
            # If errors are present, it's possible 'response' is empty due to the error, not genuinely no matches.
            # So, we return empty list indicating error rather than potentially confusing "0 matches" log.
            return []
            
        return matches_list # Return the fetched list
    except requests.exceptions.RequestException as e:
        # Print detailed API error information first, if available
        if e.response is not None:
            print(f"API Error Details (API-SPORTS): Status Code: {e.response.status_code}")
            print(f"Response Text (API-SPORTS): {e.response.text}")
        # Then print the general error message
        print(f"Error fetching data from API-SPORTS: {e}")
        return []
    except ValueError as e:  # Handles JSON decoding errors
        print(f"Error decoding JSON response from API-SPORTS: {e}")
        return []

    # Placeholder for fetching data from other sources
    # def get_data_from_sportmonks():
    #     pass

# Placeholder for fetching data from SportMonks API
def get_matches_from_sportmonks(date_str: str, api_key: str = None):
    """
    Fetches matches for a specific date from the SportMonks API.
    (Conceptual placeholder - Not Implemented)

    Args:
        date_str (str): The date in "YYYY-MM-DD" format.
        api_key (str, optional): The API key for SportMonks. 
                                 If None, an attempt could be made to get it from an environment variable.

    Returns:
        list: A list of match objects, or an empty list if an error occurs or not implemented.
    """
    warnings.warn("get_matches_from_sportmonks is a placeholder and not yet implemented.")
    print(f"Attempted to call get_matches_from_sportmonks for date: {date_str}. This function is not implemented.")
    # To implement this, you would need:
    # 1. SportMonks API key mechanism (e.g., os.getenv("SPORTMONKS_API_KEY"))
    # 2. SportMonks API base URL and relevant endpoint for matches by date.
    # 3. HTTP request logic (e.g., using requests library) with appropriate headers.
    # 4. Data parsing logic to transform SportMonks API response into a common format (similar to other get_matches_* functions).
    # 5. Error handling.
    return []


# --- Historical Data Fetching for football-data.org ---
# Note: Accessing extensive historical data may be subject to rate limits or require
# a specific plan on football-data.org. Always consult their API documentation.

def get_historical_matches_for_competition(competition_code: str, season: int, api_key: str = FOOTBALL_DATA_API_KEY):
    """
    Fetches all matches for a specific competition and season from football-data.org.

    Args:
        competition_code (str): The competition code (e.g., 'PL', 'BL1').
        season (int): The starting year of the season (e.g., 2022 for 2022-2023 season).
        api_key (str): The API key for football-data.org.

    Returns:
        list: A list of match objects, or an empty list if an error occurs.
              Note: This function currently assumes all matches for a season are returned in a single API call.
              For very large datasets or APIs with strict pagination on historical data,
              pagination handling would need to be added.
    """
    if not api_key or api_key == "YOUR_API_TOKEN":
        print(f"Error: Invalid or missing API key for football-data.org. Cannot fetch historical data for {competition_code} season {season}.")
        return []

    headers = {"X-Auth-Token": api_key}
    api_url = f"{FOOTBALL_DATA_BASE_URL}competitions/{competition_code}/matches?season={season}"

    print(f"DEBUG: data_collection.py - get_historical_matches_for_competition - Requesting URL: {api_url} with API Key: {api_key[:4]}...{api_key[-4:] if len(api_key) > 8 else ''}")

    try:
        response = requests.get(api_url, headers=headers, timeout=60) # Increased timeout for potentially larger data
        response.raise_for_status()
        data = response.json()

        matches = data.get("matches", [])
        if not matches and data.get("count", 0) == 0:
            print(f"INFO: No matches found for {competition_code} season {season}. API returned count 0 or no 'matches' array.")
        elif not matches and data.get("count", 0) > 0:
             print(f"WARNING: API reported {data.get('count')} matches for {competition_code} season {season}, but 'matches' array is empty. Check API response.")

        # football-data.org v4 for this endpoint usually returns all matches without explicit pagination controls needed by the client.
        # If pagination were required, logic to check 'meta' or 'resultSet' for 'next' page links and loop would go here.
        return matches
    except requests.exceptions.SSLError as e:
        print(f"ERROR: An SSL error occurred while contacting football-data.org for historical data ({competition_code} {season}): {e}")
        return []
    except requests.exceptions.ReadTimeout as e:
        print(f"ERROR: Read timeout from football-data.org for historical data ({competition_code} {season}): {e}. The server did not send any data in the allotted amount of time.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching historical data for {competition_code} season {season} from football-data.org: {e}")
        if e.response is not None:
            if e.response.status_code == 401:
                print(f"ERROR: Invalid or unauthorized API key for football-data.org (HTTP 401).")
            elif e.response.status_code == 403:
                print(f"ERROR: Forbidden access (HTTP 403). Your API key may not have permissions for this competition/season or has been suspended.")
            elif e.response.status_code == 404:
                 print(f"ERROR: Competition or season not found (HTTP 404) for {competition_code} season {season}.")
            elif e.response.status_code == 429: # Rate limit
                print(f"ERROR: Rate limit exceeded (HTTP 429) for football-data.org. Please wait before trying again.")
            else:
                print(f"API Error Details: Status Code: {e.response.status_code}, Response Text: {e.response.text}")
        return []
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response for {competition_code} season {season}: {e}")
        return []

def save_data_to_json(data, base_path, competition_code, season):
    """
    Saves data to a JSON file in a structured directory.
    Example path: base_path/COMPETITION_CODE/SEASON/matches.json
    """
    try:
        target_dir = os.path.join(base_path, competition_code, str(season))
        os.makedirs(target_dir, exist_ok=True)
        filepath = os.path.join(target_dir, "matches.json")

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Successfully saved data to {filepath}")
        return True
    except IOError as e:
        print(f"Error saving data to {filepath}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during save_data_to_json: {e}")
    return False


# --- TheSportsDB API Functions ---
# Note: TheSportsDB free tier (API Key "1") is limited and may not return all data,
# or data might be heavily cached. A Patreon key is required for more comprehensive access.

def search_league_thesportsdb(league_name_query: str, api_key: str = THESPORTSDB_API_KEY) -> list:
    """
    Searches for a league by name using TheSportsDB API.
    URL: {THESPORTSDB_BASE_URL}{api_key}/searchleagues.php?l={league_name_query}
    (Note: The API documentation suggests ?s={sport_name} for all leagues in a sport,
     or ?c={country_name}&s={sport_name} for specific country leagues.
     The endpoint /searchleagues.php?l={league_name_query} is less documented for general use but might work for exact names.)
     Using ?l={league_name_query} which is more direct if the name is known.
     If this doesn't work well, consider search_all_leagues.php?s={sport_query} (e.g., sport_query="Soccer")

    Args:
        league_name_query (str): The name of the league to search for (e.g., "English Premier League").
        api_key (str): The API key for TheSportsDB.

    Returns:
        list: A list of league objects found, or an empty list if an error occurs or no leagues are found.
    """
    if not league_name_query:
        print("Error: League name query cannot be empty for search_league_thesportsdb.")
        return []

    # TheSportsDB search can be tricky. The endpoint searchleagues.php expects a league name, but also often a sport.
    # A more reliable way if sport is known is search_all_leagues.php?s={sport_name} then filter by name.
    # However, sticking to the specified direct league name query:
    api_url = f"{THESPORTSDB_BASE_URL}{api_key}/searchleagues.php?l={requests.utils.quote(league_name_query)}"
    # Alternative for all soccer leagues: f"{THESPORTSDB_BASE_URL}{api_key}/search_all_leagues.php?s=Soccer"

    print(f"DEBUG: data_collection.py - search_league_thesportsdb - Requesting URL: {api_url}")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        # The key for leagues might be 'countrys' (typo in API) or 'leagues'
        leagues = data.get("leagues", data.get("countrys")) # 'countrys' is often used for soccer leagues list
        if leagues is None: # If neither key is found or is explicitly null
            print(f"INFO: No 'leagues' or 'countrys' key found in response for '{league_name_query}', or it was null.")
            return []
        return leagues
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching data from TheSportsDB (search_league): {e}")
        return []
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response from TheSportsDB (search_league): {e}")
        return []

def get_future_events_thesportsdb(league_id: str, api_key: str = THESPORTSDB_API_KEY) -> list:
    """
    Fetches future events (matches) for a specific league ID from TheSportsDB API.

    Args:
        league_id (str): The league ID from TheSportsDB (e.g., "4328" for English Premier League).
        api_key (str): The API key for TheSportsDB.

    Returns:
        list: A list of event objects, or an empty list if an error occurs or no events are found.
    """
    if not league_id:
        print("Error: league_id must be provided for get_future_events_thesportsdb.")
        return []

    api_url = f"{THESPORTSDB_BASE_URL}{api_key}/eventsnextleague.php?id={league_id}"
    print(f"DEBUG: data_collection.py - get_future_events_thesportsdb - Requesting URL: {api_url}")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        events = data.get("events") # API returns "events": null if no upcoming events
        if events is None:
            print(f"INFO: No future events found for league ID {league_id} (API returned null for 'events').")
            return []
        return events
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching data from TheSportsDB (future_events for league {league_id}): {e}")
        return []
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response from TheSportsDB (future_events for league {league_id}): {e}")
        return []

def get_event_details_thesportsdb(event_id: str, api_key: str = THESPORTSDB_API_KEY) -> dict | None:
    """
    Fetches details for a specific event ID from TheSportsDB API.
    Useful for getting live scores, final results, and detailed stats.

    Args:
        event_id (str): The event ID from TheSportsDB.
        api_key (str): The API key for TheSportsDB.

    Returns:
        dict: The event object (usually a list with one event, so returns the first element),
              or None if an error occurs or the event is not found.
    """
    if not event_id:
        print("Error: event_id must be provided for get_event_details_thesportsdb.")
        return None

    api_url = f"{THESPORTSDB_BASE_URL}{api_key}/lookupevent.php?id={event_id}"
    print(f"DEBUG: data_collection.py - get_event_details_thesportsdb - Requesting URL: {api_url}")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        events = data.get("events") # API returns "events": null if event not found
        if events and len(events) > 0:
            return events[0] # Return the first event object
        else:
            print(f"INFO: No event details found for event ID {event_id} (API returned null or empty list for 'events').")
            return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching data from TheSportsDB (event_details for event {event_id}): {e}")
        return None
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response from TheSportsDB (event_details for event {event_id}): {e}")
        return None
