import os
import subprocess
import shutil # For removing directory if needed
import yaml # PyYAML library

# --- PyYAML Installation ---
# This script requires the PyYAML library to parse YAML files.
# If you don't have it installed, please run:
# pip install PyYAML
# ---

# --- Data Source Information ---
# This script clones or updates data from various OpenFootball data repositories
# on GitHub (e.g., github.com/openfootball). It then performs a basic parse
# of YAML files found within these repositories.
# ---

def clone_or_update_repo(repo_url: str, path: str) -> bool:
    """
    Clones a Git repository if it doesn't exist locally, or updates it if it does.

    Args:
        repo_url (str): The URL of the Git repository.
        path (str): The local path where the repository should be cloned/updated.

    Returns:
        bool: True if the repository was successfully cloned or updated, False otherwise.
    """
    git_dir = os.path.join(path, '.git')

    try:
        if os.path.exists(path):
            if os.path.isdir(git_dir):
                print(f"Repository already exists at '{path}'. Attempting to update (git pull)...")
                process = subprocess.run(['git', 'pull'], cwd=path, capture_output=True, text=True, check=False, timeout=120)
                if process.returncode == 0:
                    if "Already up to date." in process.stdout:
                        print(f"Repository '{os.path.basename(path)}' is already up to date.")
                    else:
                        print(f"Repository '{os.path.basename(path)}' updated successfully.")
                    return True
                else:
                    print(f"Error updating repository '{os.path.basename(path)}': {process.stderr.strip()}")
                    return False
            else:
                print(f"Error: Path '{path}' exists but is not a Git repository.")
                print("Please remove or rename this directory, then re-run the script.")
                return False
        else:
            print(f"Repository not found at '{path}'. Attempting to clone '{repo_url}'...")
            parent_dir = os.path.dirname(path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
                print(f"Created directory: {parent_dir}")

            process = subprocess.run(['git', 'clone', repo_url, path], capture_output=True, text=True, check=False, timeout=180)
            if process.returncode == 0:
                print(f"Repository '{os.path.basename(path)}' cloned successfully to '{path}'.")
                return True
            else:
                print(f"Error cloning repository '{repo_url}': {process.stderr.strip()}")
                return False

    except FileNotFoundError:
        print("Error: 'git' command not found. Please ensure Git is installed and in your system's PATH.")
        return False
    except subprocess.TimeoutExpired:
        print(f"Error: Git operation timed out for '{repo_url}'.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during git operation for '{repo_url}': {e}")
        return False

def parse_yaml_file(filepath: str):
    """
    Parses a single YAML file.

    Args:
        filepath (str): The path to the YAML file.

    Returns:
        dict or list: The parsed data from the YAML file, or None if an error occurs.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        print(f"Error: YAML file not found at '{filepath}'.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{filepath}': {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while parsing YAML file '{filepath}': {e}")
        return None

def find_and_parse_yaml_files(root_dir: str):
    """
    Finds all YAML files in a directory and its subdirectories, then parses them.

    Args:
        root_dir (str): The root directory to search for YAML files.
    """
    print(f"\n--- Scanning for YAML files in: {root_dir} ---")
    found_yaml_files = 0
    parsed_yaml_files = 0

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith(('.yml', '.yaml')):
                found_yaml_files += 1
                filepath = os.path.join(dirpath, filename)
                print(f"\nFound YAML file: {filepath}")

                data = parse_yaml_file(filepath)

                if data is not None:
                    parsed_yaml_files +=1
                    print(f"Successfully parsed: {filepath}")
                    if isinstance(data, dict):
                        print(f"  Type: Dictionary, Top-level keys: {list(data.keys())[:5]}") # Show first 5 keys
                        if 'name' in data:
                             print(f"  Name: {data['name']}")
                        if 'matches' in data and isinstance(data['matches'], list):
                            print(f"  Contains {len(data['matches'])} matches.")
                    elif isinstance(data, list):
                        print(f"  Type: List, Number of items: {len(data)}")
                        if len(data) > 0 and isinstance(data[0], dict) and 'name' in data[0]:
                             print(f"  First item's name (if exists): {data[0]['name']}")
                    else:
                        print(f"  Type: {type(data)}, Data: {str(data)[:100]}") # Basic summary for other types
                    # Note: Full processing and database insertion of this data is a future task.
                    # This step is just to verify parsing and get a basic understanding of the content.
                else:
                    print(f"Failed to parse: {filepath}")

    if found_yaml_files == 0:
        print("No YAML files found in this directory.")
    else:
        print(f"\nFinished scanning {root_dir}. Found {found_yaml_files} YAML files, successfully parsed {parsed_yaml_files}.")
    print("--- End of YAML Scan ---")


if __name__ == '__main__':
    print("--- OpenFootball Data Downloader and Parser ---")
    print("This script clones/updates OpenFootball GitHub repositories and performs a basic parse of YAML files.")
    print("Ensure Git is installed and in your PATH.")
    print("This script also requires PyYAML: `pip install PyYAML`\n")

    openfootball_repos = [
        "https://github.com/openfootball/football.json.git",
        "https://github.com/openfootball/eng-england.git",
        "https://github.com/openfootball/de-deutschland.git",
        "https://github.com/openfootball/es-espana.git",
        "https://github.com/openfootball/it-italy.git",
        "https://github.com/openfootball/fr-france.git", # Added French football
        "https://github.com/openfootball/world-cup.json.git" # Added World Cup
    ]

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_base_dir = os.path.dirname(script_dir)
        base_local_path = os.path.join(project_base_dir, 'data', 'openfootball_data')
    except NameError: # Fallback
        print("Warning: __file__ not defined, using current working directory for data path relative calculations.")
        base_local_path = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'openfootball_data')

    print(f"Base local path for OpenFootball repositories: {base_local_path}\n")

    cloned_repo_paths = []

    for repo_url in openfootball_repos:
        repo_name = repo_url.split('/')[-1].replace('.git', '')
        clone_path = os.path.join(base_local_path, repo_name)

        print(f"Processing repository: {repo_url}")
        if clone_or_update_repo(repo_url, clone_path):
            cloned_repo_paths.append(clone_path)
        else:
            print(f"Skipping YAML parsing for {repo_name} due to clone/update issues.")
        print("-" * 20)

    if not cloned_repo_paths:
        print("\nNo repositories were successfully cloned or updated. Skipping YAML parsing.")
    else:
        print("\n--- Starting YAML File Parsing for Cloned Repositories ---")
        print("Note: The following is a basic scan and summary of YAML file contents.")
        print("Actual data transformation and loading into a database is a separate, more complex task.\n")
        for repo_path in cloned_repo_paths:
            find_and_parse_yaml_files(repo_path)

    print("\n--- OpenFootball Data Process Finished ---")
    print("Cloned/updated data should be in subdirectories under:", base_local_path)
    print("YAML parsing summary (if any) provided above.")
