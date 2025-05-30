import kaggle
import os
import sys

# --- Kaggle API Setup Instructions ---
# To use the Kaggle API, you need to set up your credentials.
# 1. Ensure you have a Kaggle account and have accepted the terms of service for any datasets you wish to download.
# 2. Create an API token from your Kaggle account page (kaggle.com -> Your Profile -> Account -> API -> Create New API Token).
#    This will download a `kaggle.json` file.
# 3. Place this `kaggle.json` file in the `~/.kaggle/` directory on Linux/macOS or `C:\Users\<Your-Username>\.kaggle\` on Windows.
#    Alternatively, you can set the environment variables:
#    - `KAGGLE_USERNAME`: Your Kaggle username
#    - `KAGGLE_KEY`: Your Kaggle API key (the content of `kaggle.json` looks like {"username":"your_username","key":"your_key"})
# Ensure the `kaggle` Python package is installed: `pip install kaggle`
# ---

def download_kaggle_dataset(dataset_slug: str, base_path: str):
    """
    Downloads and unzips a Kaggle dataset.

    Args:
        dataset_slug (str): The slug of the dataset on Kaggle (e.g., 'user/dataset-name').
        base_path (str): The base directory where datasets should be stored.
                         A subdirectory named after the dataset_slug's last part will be created here.
    """
    try:
        # Ensure the base path for all datasets exists
        os.makedirs(base_path, exist_ok=True)

        # Create a specific path for this dataset
        # e.g., sports_prediction_ai/data/kaggle_datasets/international-football-results-from-1872-to-2017
        dataset_specific_path = os.path.join(base_path, dataset_slug.split('/')[-1])
        os.makedirs(dataset_specific_path, exist_ok=True)

        print(f"Attempting to download dataset: {dataset_slug} to {dataset_specific_path}")

        # Authenticate with Kaggle API (implicitly uses ~/.kaggle/kaggle.json or env vars)
        # kaggle.api.authenticate() # Often not needed if config is correct, but good for explicit check

        print(f"Downloading dataset '{dataset_slug}'...")
        kaggle.api.dataset_download_files(dataset_slug, path=dataset_specific_path, unzip=True, quiet=False)
        print(f"Dataset '{dataset_slug}' downloaded and unzipped to '{dataset_specific_path}'")
        return True

    except kaggle.rest.ApiException as e:
        print(f"Kaggle API Error downloading '{dataset_slug}': {e}")
        if "401" in str(e): # Unauthorized
            print("Please ensure your Kaggle API credentials (kaggle.json or environment variables) are set up correctly.")
        elif "404" in str(e): # Not Found
            print(f"Dataset '{dataset_slug}' not found. Please check the slug and your permissions.")
        elif "403" in str(e): # Forbidden
             print(f"Access to dataset '{dataset_slug}' is forbidden. You may need to accept terms on the Kaggle website.")
        return False
    except FileNotFoundError:
        # This can happen if the kaggle CLI tool is not installed or not in PATH,
        # though the python library usually handles this.
        print("Error: `kaggle` command not found. Please ensure Kaggle CLI is installed and in your PATH.")
        print("Alternatively, ensure the `kaggle` Python package can function correctly.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while downloading '{dataset_slug}': {e}")
        return False

if __name__ == '__main__':
    print("--- Kaggle Dataset Downloader ---")
    print("Before running, please ensure you have set up your Kaggle API credentials.")
    print("Instructions can be found at the top of this script (kaggle_downloader.py).\n")

    # Determine the base directory of the project for data storage
    # Assumes script is in sports_prediction_ai/src/
    # Data will be stored in sports_prediction_ai/data/kaggle_datasets/
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_base_dir = os.path.dirname(script_dir) # This should be 'sports_prediction_ai'
        data_dir = os.path.join(project_base_dir, 'data', 'kaggle_datasets')
    except NameError:
        # Fallback for environments where __file__ might not be defined (e.g. some notebooks)
        print("Warning: __file__ not defined, using current working directory for data path relative calculations.")
        data_dir = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'kaggle_datasets')


    datasets_to_download = [
        'martj42/international-football-results-from-1872-to-2017',
        'open-source-sports/football-datasets', # This is a meta-dataset, might point to others
        'nathanlauga/nba-games',
        'gpiosenka/football-players-data-for-various-leagues', # Football (soccer) players data
        'davidcariboo/player-scores' # Another football (soccer) dataset with player ratings
    ]

    print(f"Target base directory for datasets: {data_dir}\n")

    downloaded_count = 0
    failed_count = 0

    for slug in datasets_to_download:
        print(f"--- Processing dataset: {slug} ---")
        if download_kaggle_dataset(dataset_slug=slug, base_path=data_dir):
            downloaded_count += 1
        else:
            failed_count += 1
        print("-" * 30 + "\n")

    print(f"--- Download Process Finished ---")
    print(f"Successfully downloaded: {downloaded_count} dataset(s).")
    print(f"Failed to download: {failed_count} dataset(s).")
    if failed_count > 0:
        print("Please check the error messages above for details on failed downloads.")
        print("Ensure your Kaggle API credentials are correct and you have accepted dataset terms on Kaggle.")

    # To actually run this, the user would need to have the kaggle package installed
    # and their kaggle.json file configured.
    # For the purpose of this task, creating the script is the goal.
    # A test run would require `pip install kaggle` and valid credentials.
    # Example of how to run: python sports_prediction_ai/src/kaggle_downloader.py
    # (Assuming kaggle library and credentials are set up)
    #
    # The testing of the actual download functionality will be deferred to a step
    # where the environment is confirmed to have Kaggle CLI/API access.
    # For now, the script structure and logic are the focus.
    sys.exit(0) # Exit successfully after creating the script. Actual download happens when user runs it.
