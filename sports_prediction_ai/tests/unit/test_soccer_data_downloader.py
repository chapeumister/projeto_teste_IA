import pytest
import os
import requests
from pathlib import Path
from unittest.mock import MagicMock, mock_open, call # Import call

# Adjust the import path based on your project structure
from sports_prediction_ai.src.soccer_data_downloader import download_csv_from_url

# Constants for testing
TEST_URL = "http://example.com/data.csv"
TEST_FILENAME = "data.csv"
SAMPLE_CSV_CONTENT = b"col1,col2\nval1,val2\nline2,data2"

@pytest.fixture
def mock_successful_response(mocker):
    """Fixture to mock a successful requests.get response."""
    mock_resp = mocker.Mock(spec=requests.Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = mocker.Mock() # Does nothing for status 200
    # Simulate iter_content
    mock_resp.iter_content = mocker.Mock(return_value=[SAMPLE_CSV_CONTENT])
    return mock_resp

def test_download_csv_success(tmp_path, mocker, mock_successful_response):
    """Test successful CSV download and file writing."""
    download_dir = tmp_path / "soccer_downloads"
    # The function itself creates the directory with exist_ok=True

    mocker.patch("requests.get", return_value=mock_successful_response)
    mock_os_makedirs = mocker.patch("sports_prediction_ai.src.soccer_data_downloader.os.makedirs")
    # Mock builtins.open to verify write operations
    m_open = mock_open()
    mocker.patch("builtins.open", m_open)

    result = download_csv_from_url(TEST_URL, str(download_dir), TEST_FILENAME)

    assert result is True, "Function should return True on successful download"

    # Verify os.makedirs was called for the download directory
    mock_os_makedirs.assert_called_once_with(str(download_dir), exist_ok=True)

    # Verify requests.get was called correctly
    requests.get.assert_called_once_with(TEST_URL, stream=True, timeout=30)

    # Verify raise_for_status was called on the response object
    mock_successful_response.raise_for_status.assert_called_once()

    # Verify file was opened in write-binary mode and content was written
    expected_filepath = download_dir / TEST_FILENAME
    m_open.assert_called_once_with(str(expected_filepath), "wb")

    # Check that write was called with the content chunks
    handle = m_open() # Get the mock file handle
    handle.write.assert_called_once_with(SAMPLE_CSV_CONTENT)


@pytest.mark.parametrize("exception_type, error_message_snippet", [
    (requests.exceptions.HTTPError("Mocked HTTP Error"), "HTTP Error downloading"),
    (requests.exceptions.ConnectionError("Mocked Connection Error"), "Connection Error downloading"),
    (requests.exceptions.Timeout("Mocked Timeout Error"), "Timeout Error downloading"),
    (requests.exceptions.RequestException("Mocked Generic Request Error"), "An error occurred downloading")
])
def test_download_csv_network_errors(tmp_path, mocker, capsys, exception_type, error_message_snippet):
    """Test handling of various requests exceptions."""
    download_dir = tmp_path / "soccer_downloads"

    mocker.patch("sports_prediction_ai.src.soccer_data_downloader.os.makedirs")
    mocker.patch("requests.get", side_effect=exception_type)

    result = download_csv_from_url(TEST_URL, str(download_dir), TEST_FILENAME)

    assert result is False, f"Function should return False on {type(exception_type).__name__}"
    captured = capsys.readouterr()
    assert error_message_snippet in captured.out
    assert TEST_URL in captured.out # Ensure the URL is part of the error message

def test_download_csv_bad_http_response_simulated_by_raise_for_status(tmp_path, mocker, capsys):
    """Test handling of bad HTTP response where response.raise_for_status() raises HTTPError."""
    download_dir = tmp_path / "soccer_downloads"

    mock_resp_bad_status = mocker.Mock(spec=requests.Response)
    mock_resp_bad_status.status_code = 404 # Example bad status
    # Configure raise_for_status to actually raise an error, as it would for 4xx/5xx
    mock_resp_bad_status.raise_for_status = mocker.Mock(side_effect=requests.exceptions.HTTPError("404 Client Error"))

    mocker.patch("sports_prediction_ai.src.soccer_data_downloader.os.makedirs")
    mocker.patch("requests.get", return_value=mock_resp_bad_status)

    result = download_csv_from_url(TEST_URL, str(download_dir), TEST_FILENAME)

    assert result is False, "Function should return False when raise_for_status triggers an HTTPError"
    captured = capsys.readouterr()
    assert "HTTP Error downloading" in captured.out
    assert TEST_URL in captured.out
    assert "404 Client Error" in captured.out # Check if the specific HTTPError reason is logged

def test_download_csv_file_system_io_error_on_open(tmp_path, mocker, capsys, mock_successful_response):
    """Test handling of IOError when opening the file for writing."""
    download_dir = tmp_path / "soccer_downloads"
    expected_filepath = download_dir / TEST_FILENAME

    mocker.patch("requests.get", return_value=mock_successful_response)
    mocker.patch("sports_prediction_ai.src.soccer_data_downloader.os.makedirs")
    # Mock builtins.open to raise IOError
    mocker.patch("builtins.open", side_effect=IOError("Mocked IOError: Permission denied"))

    result = download_csv_from_url(TEST_URL, str(download_dir), TEST_FILENAME)

    assert result is False, "Function should return False on IOError during file open"
    captured = capsys.readouterr()
    assert f"IO Error related to file {str(expected_filepath)}" in captured.out # Changed "Error writing file" to "IO Error related to file"
    assert "Mocked IOError: Permission denied" in captured.out


def test_download_csv_os_makedirs_fails(tmp_path, mocker, capsys):
    """Test handling of OSError when os.makedirs fails, expecting SUT to catch and return False."""
    download_dir = tmp_path / "soccer_downloads"
    mock_os_error_message = "Mocked OSError from makedirs - permission denied"

    # Mock os.makedirs to raise an OSError
    mocker.patch("sports_prediction_ai.src.soccer_data_downloader.os.makedirs", side_effect=OSError(mock_os_error_message))

    # Mock requests.get just in case, though it shouldn't be called if makedirs fails.
    mock_requests_get = mocker.patch("requests.get")

    result = download_csv_from_url(TEST_URL, str(download_dir), TEST_FILENAME)

    assert result is False, "Function should return False when os.makedirs raises an OSError"

    captured = capsys.readouterr()
    # Check for the error message printed by the SUT's `except IOError` block (when filepath is None)
    assert "OS Error related to directory path" in captured.out
    assert str(download_dir) in captured.out # The path argument to os.makedirs
    assert mock_os_error_message in captured.out # The specific OSError message
    # TEST_FILENAME is not in this specific error message string from SUT anymore

    mock_requests_get.assert_not_called() # Network call should not happen if directory creation fails.

# To run: pytest sports_prediction_ai/tests/unit/test_soccer_data_downloader.py
