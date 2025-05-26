# src/data_collection.py
import os
import requests
from datetime import datetime

# It's good practice to load the API key from an environment variable
# or a configuration file, rather than hardcoding it.
# For now, we'll use a placeholder.
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4/"

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
        response = requests.get(api_url, headers=headers)
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

    # Placeholder for fetching data from other sources
    # def get_data_from_api_sports():
    #     pass

    # def get_data_from_sportmonks():
    #     pass
