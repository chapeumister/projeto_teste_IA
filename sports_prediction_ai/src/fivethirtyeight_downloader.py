import os
import subprocess
import shutil # For removing directory if needed

# --- Data Source Information ---
# This script clones or updates data from the official FiveThirtyEight data repository
# on GitHub: https://github.com/fivethirtyeight/data
# ---

def clone_or_update_repo(repo_url: str, path: str):
    """
    Clones a Git repository if it doesn't exist locally, or updates it if it does.

    Args:
        repo_url (str): The URL of the Git repository.
        path (str): The local path where the repository should be cloned/updated.
    """
    git_dir = os.path.join(path, '.git')

    try:
        if os.path.exists(path):
            if os.path.isdir(git_dir):
                print(f"Repository already exists at '{path}'. Attempting to update...")
                # For 'git pull', it's better to run it with 'path' as the cwd
                process = subprocess.run(['git', 'pull'], cwd=path, capture_output=True, text=True, check=False)
                if process.returncode == 0:
                    if "Already up to date." in process.stdout:
                        print("Repository is already up to date.")
                    else:
                        print("Repository updated successfully.")
                        # print(f"Git pull output:\n{process.stdout}") # Optional: show detailed output
                else:
                    print(f"Error updating repository: {process.stderr}")
            else:
                print(f"Error: Path '{path}' exists but is not a Git repository.")
                print("Please remove or rename this directory, then re-run the script.")
                # Optionally, offer to remove it:
                # choice = input(f"Do you want to remove '{path}' and clone the repository? [y/N]: ")
                # if choice.lower() == 'y':
                #     shutil.rmtree(path)
                #     print(f"Directory '{path}' removed. Re-run the script to clone.")
                # else:
                #     print("Aborted. Please handle the directory manually.")
        else:
            print(f"Repository not found at '{path}'. Attempting to clone...")
            # Ensure parent directory exists before cloning
            parent_dir = os.path.dirname(path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
                print(f"Created directory: {parent_dir}")

            process = subprocess.run(['git', 'clone', repo_url, path], capture_output=True, text=True, check=False)
            if process.returncode == 0:
                print(f"Repository cloned successfully to '{path}'.")
            else:
                print(f"Error cloning repository: {process.stderr}")

    except FileNotFoundError:
        # This typically means 'git' command is not found
        print("Error: 'git' command not found. Please ensure Git is installed and in your system's PATH.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    print("--- FiveThirtyEight Data Downloader ---")
    print("This script clones or updates the data repository from FiveThirtyEight (github.com/fivethirtyeight/data).\n")

    repo_url = "https://github.com/fivethirtyeight/data.git"

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_base_dir = os.path.dirname(script_dir)
        local_clone_path = os.path.join(project_base_dir, 'data', 'fivethirtyeight_data')
    except NameError: # Fallback for environments where __file__ might not be defined
        print("Warning: __file__ not defined, using current working directory for data path relative calculations.")
        local_clone_path = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'fivethirtyeight_data')

    print(f"Target local path for the repository: {local_clone_path}\n")

    clone_or_update_repo(repo_url, local_clone_path)

    print("\n--- Process Finished ---")
    print(f"The data should be available in '{local_clone_path}' if the process was successful.")
    print("If you encountered 'git' command not found error, please install Git.")

    # For the purpose of this task, creating the script is the goal.
    # Actual cloning/pulling depends on 'git' being installed and network access.
    # Example of how to run: python sports_prediction_ai/src/fivethirtyeight_downloader.py
    # This would require Git to be installed.
