import pytest
import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock, call # Added call

# Import the function to be tested
from sports_prediction_ai.src.fivethirtyeight_downloader import clone_or_update_repo

TEST_REPO_URL = "https://github.com/fivethirtyeight/data.git"
TEST_CLONE_DIR_NAME = "fivethirtyeight_data" # Based on how local_clone_path is constructed in the script

@pytest.fixture
def mock_subprocess_run(mocker):
    """Fixture to mock subprocess.run."""
    return mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.subprocess.run")

@pytest.fixture
def mock_os_ops(mocker):
    """Fixture to mock os related operations."""
    mock_exists = mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.os.path.exists")
    mock_isdir = mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.os.path.isdir")
    mock_makedirs = mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.os.makedirs")
    # Mock os.path.dirname as it's used to get parent_dir
    mock_dirname = mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.os.path.dirname")
    return mock_exists, mock_isdir, mock_makedirs, mock_dirname

def test_clone_new_repo_success(tmp_path, mock_subprocess_run, mock_os_ops, capsys):
    """Test successful cloning of a new repository."""
    mock_exists, _, mock_makedirs, mock_dirname = mock_os_ops

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    parent_dir_path = tmp_path # What dirname would return if clone_target_path is tmp_path/name

    mock_dirname.return_value = str(parent_dir_path)
    # First os.path.exists for the repo path, second for the parent directory
    mock_exists.side_effect = [False, False]

    mock_process = MagicMock()
    mock_process.returncode = 0
    mock_process.stdout = "Cloned successfully."
    mock_subprocess_run.return_value = mock_process

    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    mock_exists.assert_any_call(str(clone_target_path)) # Called to check if repo exists
    mock_dirname.assert_called_once_with(str(clone_target_path)) # Called to get parent dir
    mock_exists.assert_any_call(str(parent_dir_path)) # Called to check if parent dir exists
    mock_makedirs.assert_called_once_with(str(parent_dir_path), exist_ok=True)
    mock_subprocess_run.assert_called_once_with(
        ['git', 'clone', TEST_REPO_URL, str(clone_target_path)],
        capture_output=True, text=True, check=False
    )
    captured = capsys.readouterr()
    assert f"Repository cloned successfully to '{str(clone_target_path)}'." in captured.out

def test_clone_new_repo_parent_dir_exists(tmp_path, mock_subprocess_run, mock_os_ops, capsys):
    """Test cloning when the parent directory for the clone path already exists."""
    mock_exists, _, mock_makedirs, mock_dirname = mock_os_ops

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    parent_dir_path = tmp_path

    mock_dirname.return_value = str(parent_dir_path)
    # Repo path does not exist, parent directory does exist
    mock_exists.side_effect = [False, True]

    mock_process = MagicMock(returncode=0, stdout="Cloned.", stderr="")
    mock_subprocess_run.return_value = mock_process

    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    mock_makedirs.assert_not_called() # Parent directory already exists
    mock_subprocess_run.assert_called_once_with(
        ['git', 'clone', TEST_REPO_URL, str(clone_target_path)],
        capture_output=True, text=True, check=False
    )
    captured = capsys.readouterr()
    assert f"Repository cloned successfully to '{str(clone_target_path)}'." in captured.out


def test_update_existing_repo_success(tmp_path, mock_subprocess_run, mock_os_ops, capsys):
    """Test successful update of an existing repository."""
    mock_exists, mock_isdir, _, _ = mock_os_ops
    mock_exists.return_value = True # Repo path exists
    mock_isdir.return_value = True  # It is a git directory

    mock_process = MagicMock(returncode=0, stdout="Successfully pulled.", stderr="")
    mock_subprocess_run.return_value = mock_process

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    mock_subprocess_run.assert_called_once_with(
        ['git', 'pull'], cwd=str(clone_target_path),
        capture_output=True, text=True, check=False
    )
    captured = capsys.readouterr()
    assert "Repository updated successfully." in captured.out

def test_update_existing_repo_already_up_to_date(tmp_path, mock_subprocess_run, mock_os_ops, capsys):
    """Test updating an existing repository that is already up-to-date."""
    mock_exists, mock_isdir, _, _ = mock_os_ops
    mock_exists.return_value = True
    mock_isdir.return_value = True

    mock_process = MagicMock(returncode=0, stdout="Already up to date.", stderr="")
    mock_subprocess_run.return_value = mock_process

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    captured = capsys.readouterr()
    assert "Repository is already up to date." in captured.out


@pytest.mark.parametrize("command_type", ["clone", "pull"])
def test_git_command_fails(tmp_path, mock_subprocess_run, mock_os_ops, capsys, command_type):
    """Test failure of git clone or git pull commands."""
    mock_exists, mock_isdir, _, _ = mock_os_ops
    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME

    if command_type == "clone":
        mock_exists.return_value = False # Trigger clone path
    else: # pull
        mock_exists.return_value = True
        mock_isdir.return_value = True   # Trigger pull path

    mock_process = MagicMock(returncode=1, stdout="", stderr="Simulated Git error")
    mock_subprocess_run.return_value = mock_process

    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    captured = capsys.readouterr()
    assert "Simulated Git error" in captured.out
    if command_type == "clone":
        assert f"Error cloning repository: Simulated Git error" in captured.out
    else:
        assert f"Error updating repository: Simulated Git error" in captured.out

def test_path_exists_not_git_repo(tmp_path, mock_subprocess_run, mock_os_ops, capsys):
    """Test scenario where the target path exists but is not a Git repository."""
    mock_exists, mock_isdir, _, _ = mock_os_ops
    mock_exists.return_value = True # Path exists
    mock_isdir.return_value = False # But it's not a .git directory

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    captured = capsys.readouterr()
    assert f"Error: Path '{str(clone_target_path)}' exists but is not a Git repository." in captured.out
    mock_subprocess_run.assert_not_called() # Git command should not be attempted

@pytest.mark.parametrize("exception_raised, expected_message_part", [
    (FileNotFoundError("git command not found"), "Error: 'git' command not found."),
    (subprocess.TimeoutExpired("cmd", 120), "An unexpected error occurred: Command 'cmd' timed out after 120 seconds"),
    (Exception("Some other unexpected error"), "An unexpected error occurred: Some other unexpected error")
])
def test_clone_or_update_exceptions(tmp_path, mock_subprocess_run, mock_os_ops, capsys, exception_raised, expected_message_part):
    """Test handling of various exceptions during subprocess.run or other operations."""
    mock_exists, _, _, _ = mock_os_ops
    # Configure mocks to attempt a clone, where subprocess.run will raise the exception
    mock_exists.return_value = False

    mock_subprocess_run.side_effect = exception_raised

    clone_target_path = tmp_path / TEST_CLONE_DIR_NAME
    clone_or_update_repo(TEST_REPO_URL, str(clone_target_path))

    captured = capsys.readouterr()
    assert expected_message_part in captured.out

# To run: pytest sports_prediction_ai/tests/unit/test_fivethirtyeight_downloader.py
