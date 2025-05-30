import requests
import os
import pandas # Imported for potential future use as requested

# --- Data Source Information ---
# This script downloads CSV data from football-data.co.uk.
# The data is typically updated seasonally, and URL structures may change.
# Please verify URLs on the website if you encounter issues:
# https://www.football-data.co.uk/data.php
# ---

def download_csv_from_url(url: str, path: str, filename: str):
    """
    Downloads a CSV file from a given URL and saves it to a specified path.

    Args:
        url (str): The URL to download the CSV from.
        path (str): The directory path to save the downloaded file.
        filename (str): The name for the saved CSV file.
    """
    filepath = None # Initialize filepath to None
    filepath = None # Initialize filepath to None
    try:
        # Ensure the download path exists
        os.makedirs(path, exist_ok=True)

        filepath = os.path.join(path, filename)

        print(f"Attempting to download: {filename} from {url}")
        response = requests.get(url, stream=True, timeout=30) # Added stream=True and timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Successfully downloaded and saved: {filepath}")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error downloading {url}: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error downloading {url}: {e}")
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error downloading {url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred downloading {url}: {e}")
    except IOError as e: # Catches errors from open() / write()
        print(f"Error writing file {filepath}: {e}")
    except OSError as e: # Catch OS errors, e.g., from os.makedirs if exist_ok=False or permission issues
        print(f"OS Error related to path {path} or file {filename}: {e}")
    return False

if __name__ == '__main__':
    print("--- Soccer-Data.co.uk CSV Downloader ---")
    print("This script downloads match data from football-data.co.uk.")
    print("URLs can change seasonally. Check https://www.football-data.co.uk/data.php if downloads fail.\n")

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_base_dir = os.path.dirname(script_dir)
        base_download_path = os.path.join(project_base_dir, 'data', 'soccer_data_co_uk')
    except NameError: # Fallback for environments where __file__ might not be defined
        print("Warning: __file__ not defined, using current working directory for data path relative calculations.")
        base_download_path = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'soccer_data_co_uk')

    # List of datasets to download.
    # Each dict contains: league_name, country, season_url_part (e.g., '2324'), division_code (e.g., 'E0')
    # The filename will be constructed as <division_code>_<season_url_part>.csv
    datasets_to_download = [
        {'league_name': 'Premier League', 'country': 'England', 'season_url_part': '2324', 'division_code': 'E0'},
        {'league_name': 'Bundesliga 1', 'country': 'Germany', 'season_url_part': '2324', 'division_code': 'D1'},
        {'league_name': 'La Liga', 'country': 'Spain', 'season_url_part': '2324', 'division_code': 'SP1'},
        {'league_name': 'Serie A', 'country': 'Italy', 'season_url_part': '2324', 'division_code': 'I1'},
        {'league_name': 'Ligue 1', 'country': 'France', 'season_url_part': '2324', 'division_code': 'F1'},
        {'league_name': 'Championship', 'country': 'England', 'season_url_part': '2324', 'division_code': 'E1'},
        {'league_name': 'Premier League', 'country': 'England', 'season_url_part': '2223', 'division_code': 'E0'}, # Past season example
        {'league_name': 'Bundesliga 1', 'country': 'Germany', 'season_url_part': '2223', 'division_code': 'D1'}, # Past season example
        {'league_name': 'Eredivisie', 'country': 'Netherlands', 'season_url_part': '2324', 'division_code': 'N1'},
        {'league_name': 'Primeira Liga', 'country': 'Portugal', 'season_url_part': '2324', 'division_code': 'P1'},
    ]

    base_url_template = "https://www.football-data.co.uk/mmz4281/{season_url_part}/{division_code}.csv"

    print(f"Base download directory: {base_download_path}\n")

    downloaded_count = 0
    failed_count = 0

    for dataset_info in datasets_to_download:
        season_part = dataset_info['season_url_part']
        division = dataset_info['division_code']

        url = base_url_template.format(season_url_part=season_part, division_code=division)

        # Construct filename, e.g., E0_2324.csv or D1_2223.csv
        filename = f"{division}_{season_part}.csv"

        print(f"--- Processing: {dataset_info['league_name']} ({dataset_info['country']}) Season {season_part} ---")
        if download_csv_from_url(url=url, path=base_download_path, filename=filename):
            downloaded_count += 1
        else:
            failed_count += 1
        print("-" * 30 + "\n")

    print(f"--- Download Process Finished ---")
    print(f"Successfully downloaded: {downloaded_count} file(s).")
    print(f"Failed to download: {failed_count} file(s).")
    if failed_count > 0:
        print("Please check the error messages above for details on failed downloads.")
        print("Verify URLs and your internet connection. Data for some leagues/seasons may not be available.")

    # Actual download execution depends on network access.
    # For this task, creating the script with the logic is the goal.
    # Example of how to run: python sports_prediction_ai/src/soccer_data_downloader.py
    # This would require the `requests` library to be installed (`pip install requests`).
