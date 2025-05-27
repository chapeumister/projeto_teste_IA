# src/data_collection.py
import os
import requests
import warnings
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
    if api_key == "YOUR_API_TOKEN":
        print("Warning: Using a placeholder API key for football-data.org. Please set the FOOTBALL_DATA_API_KEY environment variable.")
        # Return an empty list or raise an error, as per desired behavior when no key is set.
        # For this example, we'll return an empty list to avoid breaking the flow
        # if the user hasn't set up an API key yet for initial testing.
        return []

    headers = {"X-Auth-Token": api_key}
    # The API endpoint for matches can vary; v4 uses /matches.
    # The API also supports dateFrom and dateTo parameters for ranges.
    # For a single day, dateFrom and dateTo will be the same.
    api_url = f"{FOOTBALL_DATA_BASE_URL}matches?dateFrom={date_str}&dateTo={date_str}"

    try:
        warnings.warn(
            "Bypassing SSL certificate verification for api.football-data.org. This is a potential security risk. Consider installing the appropriate SSL certificates for a more secure connection.",
            UserWarning
        )
        response = requests.get(api_url, headers=headers, verify=False)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        data = response.json()
        return data.get("matches", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from football-data.org: {e}")
        return []
    except ValueError as e: # Handles JSON decoding errors
        print(f"Error decoding JSON response: {e}")
        return []

if __name__ == "__main__":
    # Example usage:
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Fetching matches for today: {today_str}")

    # To actually run this, you would need a valid API key.
    # If FOOTBALL_DATA_API_KEY is not set in your environment, it will use "YOUR_API_TOKEN"
    # and the function will print a warning and return an empty list.
    matches = get_matches_for_date(today_str)

    if matches:
        print(f"Found {len(matches)} matches:")
        for match in matches:
            home_team = match.get("homeTeam", {}).get("name", "N/A")
            away_team = match.get("awayTeam", {}).get("name", "N/A")
            competition = match.get("competition", {}).get("name", "N/A")
            match_time_utc = match.get("utcDate", "N/A")
            status = match.get("status", "N/A")
            print(f"- {home_team} vs {away_team} (Competition: {competition}, Time UTC: {match_time_utc}, Status: {status})")
    elif FOOTBALL_DATA_API_KEY != "YOUR_API_TOKEN":
        print("No matches found for today, or there was an issue fetching them (and a valid API key seems to be present).")
    else:
        print("No matches fetched. Ensure your FOOTBALL_DATA_API_KEY environment variable is set correctly.")

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
        
        # Check if the API returned an error message in the JSON body
        # API-SPORTS error responses often contain a 'errors' key which can be a list or dict
        if data.get("errors") and (isinstance(data["errors"], list) and data["errors"] or isinstance(data["errors"], dict) and data["errors"]):
            print(f"API-SPORTS returned errors: {data['errors']}")
            return []
            
        return data.get("response", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API-SPORTS: {e}")
        return []
    except ValueError as e:  # Handles JSON decoding errors
        print(f"Error decoding JSON response from API-SPORTS: {e}")
        return []

    # Placeholder for fetching data from other sources
    # def get_data_from_sportmonks():
    #     pass
