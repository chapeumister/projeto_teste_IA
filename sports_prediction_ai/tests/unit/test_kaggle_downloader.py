import pytest
import os
from pathlib import Path
from unittest.mock import patch, call, MagicMock # Import call for checking multiple calls to makedirs and MagicMock
import os
import tempfile
import json
import shutil
# import atexit # No longer needed as global setup is removed

# --- Global Setup for Dummy Kaggle Config ---
# This code is removed as conftest.py's session-scoped autouse fixture `setup_dummy_kaggle_config` handles it.
# TEMP_KAGGLE_CONFIG_DIR = tempfile.mkdtemp(prefix="pytest-kaggle-cfg-")
# os.environ["KAGGLE_CONFIG_DIR"] = TEMP_KAGGLE_CONFIG_DIR
# dummy_kaggle_json_path = os.path.join(TEMP_KAGGLE_CONFIG_DIR, "kaggle.json")
# with open(dummy_kaggle_json_path, 'w') as f:
#     json.dump({"username": "testuser", "key": "testkey"}, f)
# print(f"INFO: Dummy kaggle config created at {TEMP_KAGGLE_CONFIG_DIR} for module {__name__}")

# def cleanup_dummy_kaggle_config():
#     if os.path.exists(TEMP_KAGGLE_CONFIG_DIR):
#         shutil.rmtree(TEMP_KAGGLE_CONFIG_DIR, ignore_errors=True)
#         print(f"INFO: Cleaned up dummy kaggle config dir {TEMP_KAGGLE_CONFIG_DIR}")
# atexit.register(cleanup_dummy_kaggle_config)
# --- End Global Setup ---

import pytest
from pathlib import Path
from unittest.mock import patch, call, MagicMock

# Now, import from kaggle should be safe
from kaggle.rest import ApiException
from sports_prediction_ai.src.kaggle_downloader import download_kaggle_dataset

# Define a standard dataset slug for testing
TEST_DATASET_SLUG = "testuser/testdataset"
TEST_DATASET_NAME = "testdataset" # from TEST_DATASET_SLUG.split('/')[-1]

# The conftest.py `setup_dummy_kaggle_config` fixture (session-scoped, autouse) handles Kaggle config.

def test_download_dataset_success(tmp_path, mocker):
    """Test successful download and unzip of a dataset."""
    base_download_path = tmp_path / "kaggle_data_root"
    # The function will create a subdirectory named after the dataset
    expected_dataset_specific_path = base_download_path / TEST_DATASET_NAME

    # Mock external dependencies
    mock_os_makedirs = mocker.patch("sports_prediction_ai.src.kaggle_downloader.os.makedirs")
    mock_kaggle_api_download = mocker.patch("sports_prediction_ai.src.kaggle_downloader.kaggle.api.dataset_download_files")

    # Call the function
    result = download_kaggle_dataset(TEST_DATASET_SLUG, str(base_download_path))

    # Assertions
    assert result is True, "Function should return True on success"

    # Check that os.makedirs was called correctly
    # It's called twice: once for base_path, once for dataset_specific_path
    expected_calls_makedirs = [
        call(str(base_download_path), exist_ok=True),
        call(str(expected_dataset_specific_path), exist_ok=True)
    ]
    mock_os_makedirs.assert_has_calls(expected_calls_makedirs, any_order=False) # Order matters here

    # Check that Kaggle API was called correctly
    mock_kaggle_api_download.assert_called_once_with(
        TEST_DATASET_SLUG,
        path=str(expected_dataset_specific_path),
        unzip=True,
        quiet=False
    )

@pytest.mark.parametrize("status_code, error_snippet", [
    (401, "401"), # Unauthorized
    (403, "403"), # Forbidden
    (404, "404"), # Not Found
    (500, "500")  # Generic server error
])
def test_download_dataset_api_errors(tmp_path, mocker, capsys, status_code, error_snippet):
    """Test handling of various Kaggle API errors."""
    base_download_path = tmp_path / "kaggle_data_root"

    mock_os_makedirs = mocker.patch("sports_prediction_ai.src.kaggle_downloader.os.makedirs")
    # Mock kaggle.api.dataset_download_files to raise ApiException
    mock_kaggle_api_download = mocker.patch(
        "sports_prediction_ai.src.kaggle_downloader.kaggle.api.dataset_download_files",
        # Construct ApiException with a string message that includes the status for SUT's error parsing.
        # The actual ApiException might not take status/reason keywords.
        side_effect=ApiException(f"Status {status_code} - Test API Error")
    )

    result = download_kaggle_dataset(TEST_DATASET_SLUG, str(base_download_path))

    assert result is False, f"Function should return False on ApiException with status {status_code}"

    # Check if appropriate error messages are printed
    captured = capsys.readouterr()
    assert f"Kaggle API Error downloading '{TEST_DATASET_SLUG}'" in captured.out
    assert error_snippet in captured.out # Check if the status code string is in the output

    if status_code == 401:
        assert "ensure your Kaggle API credentials" in captured.out
    elif status_code == 404:
        assert f"Dataset '{TEST_DATASET_SLUG}' not found" in captured.out
    elif status_code == 403:
        assert f"Access to dataset '{TEST_DATASET_SLUG}' is forbidden" in captured.out


def test_download_dataset_filenotfound_error(tmp_path, mocker, capsys):
    """Test handling of FileNotFoundError (e.g., kaggle command not found)."""
    base_download_path = tmp_path / "kaggle_data_root"

    mock_os_makedirs = mocker.patch("sports_prediction_ai.src.kaggle_downloader.os.makedirs")
    mocker.patch(
        "sports_prediction_ai.src.kaggle_downloader.kaggle.api.dataset_download_files",
        side_effect=FileNotFoundError("Mocked FileNotFoundError")
    )

    result = download_kaggle_dataset(TEST_DATASET_SLUG, str(base_download_path))

    assert result is False, "Function should return False on FileNotFoundError"
    captured = capsys.readouterr()
    assert "Error: `kaggle` command not found" in captured.out


def test_download_dataset_unexpected_exception(tmp_path, mocker, capsys):
    """Test handling of other unexpected exceptions."""
    base_download_path = tmp_path / "kaggle_data_root"

    mock_os_makedirs = mocker.patch("sports_prediction_ai.src.kaggle_downloader.os.makedirs")
    mocker.patch(
        "sports_prediction_ai.src.kaggle_downloader.kaggle.api.dataset_download_files",
        side_effect=Exception("Mocked unexpected error")
    )

    result = download_kaggle_dataset(TEST_DATASET_SLUG, str(base_download_path))

    assert result is False, "Function should return False on a generic Exception"
    captured = capsys.readouterr()
    assert f"An unexpected error occurred while downloading '{TEST_DATASET_SLUG}'" in captured.out
    assert "Mocked unexpected error" in captured.out

# Note: Directory creation logic is handled by `os.makedirs(..., exist_ok=True)`
# and its calls are verified in `test_download_dataset_success`.
# No separate tests for `os.path.exists` are needed as the script doesn't use it
# before `os.makedirs(exist_ok=True)`.
# The `exist_ok=True` parameter means `os.makedirs` won't raise an error if the directory already exists.
# The test `test_download_dataset_success` ensures `os.makedirs` is called for both
# the base path and the dataset-specific path.
# If `os.makedirs` were to fail for other reasons (e.g. permissions), it would raise an OSError,
# which would be caught by the generic `Exception` handler in the source code.
# We could add a specific test for OSError from os.makedirs if desired.

def test_download_dataset_os_makedirs_fails(tmp_path, mocker, capsys):
    """Test handling of OSError when os.makedirs fails."""
    base_download_path = tmp_path / "kaggle_data_root"

    # Mock os.makedirs to raise an OSError
    mocker.patch(
        "sports_prediction_ai.src.kaggle_downloader.os.makedirs",
        side_effect=OSError("Mocked OSError: Permission denied")
    )

    # We don't need to mock kaggle.api.dataset_download_files here, as os.makedirs is called first.

    result = download_kaggle_dataset(TEST_DATASET_SLUG, str(base_download_path))

    assert result is False, "Function should return False if os.makedirs raises an OSError"
    captured = capsys.readouterr()
    # The generic exception handler in download_kaggle_dataset should catch this
    assert f"An unexpected error occurred while downloading '{TEST_DATASET_SLUG}'" in captured.out
    assert "Mocked OSError: Permission denied" in captured.out

# To run these tests:
# Ensure pytest, pytest-mock, and kaggle package are installed.
# Navigate to the root of the sports_prediction_ai project.
# Run: pytest sports_prediction_ai/tests/unit/test_kaggle_downloader.py
# (Or simply `pytest` if tests are discoverable)
