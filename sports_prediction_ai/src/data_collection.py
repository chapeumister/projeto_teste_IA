# src/data_collection.py
import os
import requests
import warnings
import json # Added json import
from datetime import datetime

# It's good practice to load the API key from an environment variable
# or a configuration file, rather than hardcoding it.
# For now, we'll use a placeholder.
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")
APISPORTS_API_KEY = os.getenv("APISPORTS_API_KEY") # New API key
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4/"
APISPORTS_BASE_URL = "https://v3.football.api-sports.io/" # New base URL

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
    # The API endpoint for matches can vary; v4 uses /matches.
    # The API also supports dateFrom and dateTo parameters for ranges.
    # For a single day, dateFrom and dateTo will be the same.
    api_url = f"{FOOTBALL_DATA_BASE_URL}matches?dateFrom={date_str}&dateTo={date_str}"

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
