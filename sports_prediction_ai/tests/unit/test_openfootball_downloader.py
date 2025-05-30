import pytest
import os
import subprocess
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open, call, MagicMock # Added MagicMock

# Import functions to be tested
from sports_prediction_ai.src.openfootball_downloader import (
    clone_or_update_repo,
    parse_yaml_file,
    find_and_parse_yaml_files
)

# --- Tests for clone_or_update_repo ---
TEST_REPO_URL = "https://example.com/repo.git"
TEST_REPO_NAME = "repo" # from TEST_REPO_URL

@pytest.fixture
def mock_subprocess_run(mocker):
    """Fixture to mock subprocess.run."""
    return mocker.patch("sports_prediction_ai.src.openfootball_downloader.subprocess.run")

@pytest.fixture
def mock_os_path_ops(mocker):
    """Fixture to mock os.path and os.makedirs operations."""
    mock_exists = mocker.patch("sports_prediction_ai.src.openfootball_downloader.os.path.exists")
    mock_isdir = mocker.patch("sports_prediction_ai.src.openfootball_downloader.os.path.isdir")
    mock_makedirs = mocker.patch("sports_prediction_ai.src.openfootball_downloader.os.makedirs")
    mock_basename = mocker.patch("sports_prediction_ai.src.openfootball_downloader.os.path.basename", return_value=TEST_REPO_NAME)
    mock_dirname = mocker.patch("sports_prediction_ai.src.openfootball_downloader.os.path.dirname", return_value="parent/dir")
    return mock_exists, mock_isdir, mock_makedirs, mock_basename, mock_dirname


def test_clone_new_repo_success(tmp_path, mock_subprocess_run, mock_os_path_ops):
    mock_exists, _, mock_makedirs, _, mock_dirname_mock = mock_os_path_ops
    mock_exists.side_effect = [False, False] # First for path, second for parent_dir

    mock_process_result = MagicMock()
    mock_process_result.returncode = 0
    mock_process_result.stdout = "Cloned successfully."
    mock_process_result.stderr = ""
    mock_subprocess_run.return_value = mock_process_result

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is True
    mock_exists.assert_any_call(str(clone_path)) # Checks if repo path exists
    mock_dirname_mock.assert_called_with(str(clone_path)) # To get parent_dir
    mock_exists.assert_any_call("parent/dir") # Checks if parent_dir path exists
    mock_makedirs.assert_called_once_with("parent/dir", exist_ok=True)
    mock_subprocess_run.assert_called_once_with(
        ['git', 'clone', TEST_REPO_URL, str(clone_path)],
        capture_output=True, text=True, check=False, timeout=180
    )

def test_clone_new_repo_parent_dir_exists(tmp_path, mock_subprocess_run, mock_os_path_ops):
    mock_exists, _, mock_makedirs, _, _ = mock_os_path_ops
    mock_exists.side_effect = [False, True] # path doesn't exist, parent_dir does

    mock_process_result = MagicMock(returncode=0, stdout="Cloned.", stderr="")
    mock_subprocess_run.return_value = mock_process_result

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is True
    mock_makedirs.assert_not_called() # Parent dir exists, so makedirs for it shouldn't be called
    mock_subprocess_run.assert_called_once_with(
        ['git', 'clone', TEST_REPO_URL, str(clone_path)],
        capture_output=True, text=True, check=False, timeout=180
    )


def test_update_existing_repo_success(tmp_path, mock_subprocess_run, mock_os_path_ops):
    mock_exists, mock_isdir, _, _, _ = mock_os_path_ops
    mock_exists.return_value = True
    mock_isdir.return_value = True # It's a git repo

    mock_process_result = MagicMock(returncode=0, stdout="Updated.", stderr="")
    mock_subprocess_run.return_value = mock_process_result

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is True
    mock_subprocess_run.assert_called_once_with(
        ['git', 'pull'], cwd=str(clone_path),
        capture_output=True, text=True, check=False, timeout=120
    )

def test_update_existing_repo_already_up_to_date(tmp_path, mock_subprocess_run, mock_os_path_ops, capsys):
    mock_exists, mock_isdir, _, _, _ = mock_os_path_ops
    mock_exists.return_value = True
    mock_isdir.return_value = True

    mock_process_result = MagicMock(returncode=0, stdout="Already up to date.", stderr="")
    mock_subprocess_run.return_value = mock_process_result

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is True
    captured = capsys.readouterr()
    assert f"Repository '{TEST_REPO_NAME}' is already up to date." in captured.out


@pytest.mark.parametrize("command_type,git_command_args", [
    ("clone", ['git', 'clone', TEST_REPO_URL, "some_path"]),
    ("pull", ['git', 'pull'])
])
def test_clone_or_update_git_command_fails(tmp_path, mock_subprocess_run, mock_os_path_ops, capsys, command_type, git_command_args):
    mock_exists, mock_isdir, _, _, _ = mock_os_path_ops
    clone_path = tmp_path / TEST_REPO_NAME

    if command_type == "clone":
        mock_exists.return_value = False # To trigger clone path
        git_command_args[3] = str(clone_path) # Update path in args
    else: # pull
        mock_exists.return_value = True
        mock_isdir.return_value = True # To trigger pull path

    mock_process_result = MagicMock(returncode=1, stdout="", stderr="Git command failed error")
    mock_subprocess_run.return_value = mock_process_result

    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is False
    captured = capsys.readouterr()
    assert "Git command failed error" in captured.out
    if command_type == "clone":
        assert f"Error cloning repository '{TEST_REPO_URL}'" in captured.out
    else:
        assert f"Error updating repository '{TEST_REPO_NAME}'" in captured.out


def test_clone_or_update_path_exists_not_git_repo(tmp_path, mock_subprocess_run, mock_os_path_ops, capsys):
    mock_exists, mock_isdir, _, _, _ = mock_os_path_ops
    mock_exists.return_value = True
    mock_isdir.return_value = False # Path exists, but .git dir doesn't

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is False
    captured = capsys.readouterr()
    assert f"Path '{str(clone_path)}' exists but is not a Git repository." in captured.out
    mock_subprocess_run.assert_not_called()


@pytest.mark.parametrize("exception_raised, expected_message_part", [
    (FileNotFoundError("git not found"), "Error: 'git' command not found."),
    (subprocess.TimeoutExpired("cmd", 120), f"Error: Git operation timed out for '{TEST_REPO_URL}'."),
    (Exception("Some other error"), f"An unexpected error occurred during git operation for '{TEST_REPO_URL}'")
])
def test_clone_or_update_exceptions(tmp_path, mock_subprocess_run, mock_os_path_ops, capsys, exception_raised, expected_message_part):
    mock_exists, _, _, _, _ = mock_os_path_ops
    mock_exists.return_value = False # Trigger clone attempt

    mock_subprocess_run.side_effect = exception_raised

    clone_path = tmp_path / TEST_REPO_NAME
    result = clone_or_update_repo(TEST_REPO_URL, str(clone_path))

    assert result is False
    captured = capsys.readouterr()
    assert expected_message_part in captured.out


# --- Tests for parse_yaml_file ---

def test_parse_yaml_success(mocker):
    mock_yaml_content = "name: Test League\nclubs:\n  - Team A"
    expected_data = {"name": "Test League", "clubs": ["Team A"]}

    mocker.patch("builtins.open", mock_open(read_data=mock_yaml_content))
    mock_safe_load = mocker.patch("yaml.safe_load", return_value=expected_data)

    result = parse_yaml_file("dummy/path.yml")

    assert result == expected_data
    mock_safe_load.assert_called_once()

def test_parse_yaml_file_not_found(mocker, capsys):
    mocker.patch("builtins.open", side_effect=FileNotFoundError("File not here"))

    result = parse_yaml_file("nonexistent/path.yml")

    assert result is None
    captured = capsys.readouterr()
    assert "Error: YAML file not found at 'nonexistent/path.yml'." in captured.out

def test_parse_yaml_invalid_yaml(mocker, capsys):
    mocker.patch("builtins.open", mock_open(read_data="name: Test League\n  - Invalid Indent"))
    mocker.patch("yaml.safe_load", side_effect=yaml.YAMLError("Bad YAML"))

    result = parse_yaml_file("invalid/path.yml")

    assert result is None
    captured = capsys.readouterr()
    assert "Error parsing YAML file 'invalid/path.yml': Bad YAML" in captured.out

def test_parse_yaml_generic_exception_on_open(mocker, capsys):
    mocker.patch("builtins.open", side_effect=Exception("Some generic open error"))

    result = parse_yaml_file("error/path.yml")

    assert result is None
    captured = capsys.readouterr()
    assert "An unexpected error occurred while parsing YAML file 'error/path.yml': Some generic open error" in captured.out


# --- Tests for find_and_parse_yaml_files ---

@pytest.fixture
def mock_parse_yaml(mocker):
    # Mock the parse_yaml_file function from the same module where find_and_parse_yaml_files is defined
    return mocker.patch("sports_prediction_ai.src.openfootball_downloader.parse_yaml_file")

def test_find_and_parse_yaml_files_success(tmp_path, mock_parse_yaml, capsys):
    # Create dummy directory structure and files
    dir1 = tmp_path / "dir1"
    dir1.mkdir()
    (dir1 / "file1.yaml").write_text("content1")
    (dir1 / "file2.yml").write_text("content2")
    (dir1 / "not_yaml.txt").write_text("text")

    subdir = dir1 / "subdir"
    subdir.mkdir()
    (subdir / "file3.yaml").write_text("content3")

    mock_parse_yaml.return_value = {"parsed": "data"} # Simulate successful parsing

    find_and_parse_yaml_files(str(tmp_path))

    expected_calls = [
        call(str(dir1 / "file1.yaml")),
        call(str(dir1 / "file2.yml")),
        call(str(subdir / "file3.yaml"))
    ]
    # Check calls, allow any order because os.walk order can vary slightly by OS on identical fs
    mock_parse_yaml.assert_has_calls(expected_calls, any_order=True)
    assert mock_parse_yaml.call_count == 3

    captured = capsys.readouterr()
    assert "Found YAML file" in captured.out # General check
    assert "Successfully parsed" in captured.out
    assert "Finished scanning" in captured.out
    assert "Found 3 YAML files, successfully parsed 3." in captured.out


def test_find_and_parse_no_yaml_files(tmp_path, mock_parse_yaml, capsys):
    (tmp_path / "only_text.txt").write_text("hello")

    find_and_parse_yaml_files(str(tmp_path))

    mock_parse_yaml.assert_not_called()
    captured = capsys.readouterr()
    assert "No YAML files found in this directory." in captured.out

def test_find_and_parse_root_dir_not_exist(tmp_path, mock_parse_yaml, capsys):
    non_existent_dir = tmp_path / "nonexistent"

    find_and_parse_yaml_files(str(non_existent_dir)) # os.walk handles this gracefully

    mock_parse_yaml.assert_not_called()
    captured = capsys.readouterr()
    assert "No YAML files found in this directory." in captured.out # os.walk won't yield anything.
    assert f"Scanning for YAML files in: {str(non_existent_dir)}" in captured.out


def test_find_and_parse_yaml_parsing_fails_for_some(tmp_path, mock_parse_yaml, capsys):
    (tmp_path / "good.yaml").write_text("good: true")
    (tmp_path / "bad.yaml").write_text("bad: yaml: here")

    # Simulate one success and one failure
    mock_parse_yaml.side_effect = [{"good_data": True}, None]

    find_and_parse_yaml_files(str(tmp_path))

    assert mock_parse_yaml.call_count == 2
    captured = capsys.readouterr()
    assert "Successfully parsed" in captured.out # For good.yaml
    assert "Failed to parse" in captured.out    # For bad.yaml
    assert "Found 2 YAML files, successfully parsed 1." in captured.out

# To run: pytest sports_prediction_ai/tests/unit/test_openfootball_downloader.py
