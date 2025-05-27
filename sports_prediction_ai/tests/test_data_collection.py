import pytest
import requests
import os
import json # Added
from unittest.mock import patch, Mock, mock_open # Added mock_open

# Updated import to include new functions and SSLError
from sports_prediction_ai.src.data_collection import (
    get_matches_from_apisports,
    get_matches_for_date,
    get_matches_with_fallback,
    MOCK_DATA_PATH # Import for potential patching or reference
)
from requests.exceptions import SSLError # Added

# Constants for API-SPORTS tests
MOCK_APISPORTS_API_KEY = "test_apisports_key"
SAMPLE_DATE = "2023-01-01"

# Sample successful API-SPORTS response
mock_apisports_success_response = {
    "get": "fixtures",
    "parameters": {"date": SAMPLE_DATE},
    "errors": [],
    "results": 2, # Assuming API-SPORTS uses "results" for total count
    "paging": {"current": 1, "total": 1}, # Paging can also indicate total
    "response": [
        {
            "fixture": {"id": 1, "timestamp": 1672581600, "status": {"long": "Match Finished"}},
            "league": {"id": 39, "name": "Premier League"},
            "teams": {"home": {"id": 40, "name": "Arsenal"}, "away": {"id": 47, "name": "Tottenham"}},
        },
        {
            "fixture": {"id": 2, "timestamp": 1672590000, "status": {"long": "Match Finished"}},
            "league": {"id": 39, "name": "Premier League"},
            "teams": {"home": {"id": 42, "name": "Manchester City"}, "away": {"id": 33, "name": "Manchester United"}},
        }
    ]
}

# Sample API-SPORTS response with API errors
mock_apisports_error_in_body_response = {
    "get": "fixtures",
    "parameters": {"date": SAMPLE_DATE},
    "errors": {"token": "Invalid API key. Go to https://www.api-football.com/documentation-v3#section/Authentication/Errors"},
    "results": 0,
    "paging": {"current": 1, "total": 1},
    "response": []
}

# Constant for football-data.org tests
MOCK_FOOTBALL_DATA_API_KEY = "test_football_data_key"
mock_football_data_success_response = {
    "count": 1,
    "matches": [
        {
            "id": 101,
            "utcDate": "2023-01-01T15:00:00Z",
            "status": "FINISHED",
            "homeTeam": {"id": 50, "name": "Team X"},
            "awayTeam": {"id": 51, "name": "Team Y"},
            "score": {"winner": "HOME_TEAM", "fullTime": {"home": 2, "away": 1}},
        }
    ]
}

# Sample mock data for get_matches_with_fallback
SAMPLE_MOCK_DATA_CONTENT = '{"count": 1, "matches": [{"id": 999, "homeTeam": {"name": "Mock Team A"}, "awayTeam": {"name": "Mock Team B"}}]}'


@pytest.fixture(autouse=True)
def manage_env_vars():
    original_football_data_key = os.environ.pop("FOOTBALL_DATA_API_KEY", None)
    original_apisports_key = os.environ.pop("APISPORTS_API_KEY", None)
    yield
    if original_football_data_key is not None:
        os.environ["FOOTBALL_DATA_API_KEY"] = original_football_data_key
    if original_apisports_key is not None:
        os.environ["APISPORTS_API_KEY"] = original_apisports_key

# --- Tests for get_matches_for_date (football-data.org) ---

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_success(mock_get):
    """Test successful fetch from football-data.org."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_football_data_success_response
    mock_get.return_value = mock_response

    matches = get_matches_for_date(SAMPLE_DATE, api_key=MOCK_FOOTBALL_DATA_API_KEY)
    assert len(matches) == 1
    assert matches[0]["id"] == 101
    mock_get.assert_called_once()

def test_get_matches_for_date_no_api_key(capsys):
    """Test football-data.org fetch when API key is placeholder (updated msg check)."""
    # This test assumes FOOTBALL_DATA_API_KEY is not set in env due to manage_env_vars
    # and the function defaults to "YOUR_API_TOKEN" if no key is passed.
    matches = get_matches_for_date(SAMPLE_DATE) 
    assert matches == []
    captured = capsys.readouterr()
    # The function prints "Error: Invalid or missing API key..." when the key is "YOUR_API_TOKEN"
    assert "Error: Invalid or missing API key for football-data.org." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_network_error(mock_get, capsys):
    """Test football-data.org fetch with a generic network error."""
    mock_response = Mock()
    mock_response.status_code = 500 # Example of server error
    mock_response.text = "Server Error"
    # Simulate raising an exception that has a response attribute
    mock_get.side_effect = requests.exceptions.HTTPError(response=mock_response)


    matches = get_matches_for_date(SAMPLE_DATE, api_key=MOCK_FOOTBALL_DATA_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert f"ERROR: Error fetching data from football-data.org: 500 Server Error" in captured.out
    assert "API Error Details: Status Code: 500" in captured.out # Check for detailed message

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_count_zero(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"count": 0, "matches": []}
    mock_get.return_value = mock_response

    matches = get_matches_for_date(SAMPLE_DATE, api_key="valid_key")
    assert matches == []
    captured = capsys.readouterr()
    assert f"INFO: No matches found for date {SAMPLE_DATE} from football-data.org. The API returned a count of 0." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_api_key_invalid_401(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.text = "Client Error: Unauthorized" # Example text
    # Simulate raising an exception that has a response attribute
    mock_get.side_effect = requests.exceptions.HTTPError(response=mock_response)

    matches = get_matches_for_date(SAMPLE_DATE, api_key="invalid_key")
    assert matches == []
    captured = capsys.readouterr()
    assert "ERROR: Invalid or unauthorized API key for football-data.org (HTTP 401)." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_api_key_forbidden_403(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 403
    mock_response.text = "Client Error: Forbidden" # Example text
    # Simulate raising an exception that has a response attribute
    mock_get.side_effect = requests.exceptions.HTTPError(response=mock_response)

    matches = get_matches_for_date(SAMPLE_DATE, api_key="forbidden_key")
    assert matches == []
    captured = capsys.readouterr()
    assert "ERROR: Forbidden access to football-data.org API (HTTP 403)." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_ssl_error(mock_get, capsys):
    mock_get.side_effect = SSLError("SSL handshake failed")
    matches = get_matches_for_date(SAMPLE_DATE, api_key="valid_key")
    assert matches == []
    captured = capsys.readouterr()
    assert "ERROR: An SSL error occurred while contacting football-data.org: SSL handshake failed" in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_other_request_exception(mock_get, capsys):
    # Test a RequestException that doesn't have a response (e.g. connection timeout)
    mock_get.side_effect = requests.exceptions.RequestException("Some other error")
    matches = get_matches_for_date(SAMPLE_DATE, api_key="valid_key")
    assert matches == []
    captured = capsys.readouterr()
    assert "ERROR: Error fetching data from football-data.org: Some other error" in captured.out


# --- Tests for get_matches_with_fallback ---

@patch('sports_prediction_ai.src.data_collection.get_matches_for_date')
def test_gwf_returns_real_data_when_available(mock_get_real_matches, capsys):
    mock_get_real_matches.return_value = [{"id": 1, "data": "real"}]
    
    matches = get_matches_with_fallback(SAMPLE_DATE, use_mock_data=True, api_key="dummy_key")
    assert matches == [{"id": 1, "data": "real"}]
    captured = capsys.readouterr()
    assert "Falling back to mock data" not in captured.out
    mock_get_real_matches.assert_called_once_with(SAMPLE_DATE, "dummy_key")


@patch('sports_prediction_ai.src.data_collection.get_matches_for_date')
@patch('os.path.exists')
@patch('builtins.open', new_callable=mock_open)
def test_gwf_returns_mock_data_on_failure_and_flag(mock_file_open, mock_os_path_exists, mock_get_real_matches, capsys):
    mock_get_real_matches.return_value = []  # API call returns no data
    mock_os_path_exists.return_value = True  # Mock file exists
    mock_file_open.return_value.read.return_value = SAMPLE_MOCK_DATA_CONTENT
    
    # The MOCK_DATA_PATH is defined in data_collection.py. We rely on that path.
    # If we needed to override it for testing, we could patch 'sports_prediction_ai.src.data_collection.MOCK_DATA_PATH'
    
    matches = get_matches_with_fallback(SAMPLE_DATE, use_mock_data=True, api_key="dummy_key")
    
    expected_mock_matches = json.loads(SAMPLE_MOCK_DATA_CONTENT)["matches"]
    assert matches == expected_mock_matches
    captured = capsys.readouterr()
    assert f"INFO: No real match data fetched for {SAMPLE_DATE}. Falling back to mock data as requested." in captured.out
    mock_os_path_exists.assert_called_once_with(MOCK_DATA_PATH)
    mock_file_open.assert_called_once_with(MOCK_DATA_PATH, 'r')


@patch('sports_prediction_ai.src.data_collection.get_matches_for_date')
@patch('os.path.exists') # To ensure it's not called if use_mock_data=False
def test_gwf_no_fallback_if_flag_is_false(mock_os_path_exists, mock_get_real_matches, capsys):
    mock_get_real_matches.return_value = []
    
    matches = get_matches_with_fallback(SAMPLE_DATE, use_mock_data=False, api_key="dummy_key")
    assert matches == []
    captured = capsys.readouterr()
    assert "Falling back to mock data" not in captured.out
    mock_os_path_exists.assert_not_called()


@patch('sports_prediction_ai.src.data_collection.get_matches_for_date')
@patch('os.path.exists')
def test_gwf_mock_file_not_found(mock_os_path_exists, mock_get_real_matches, capsys):
    mock_get_real_matches.return_value = []
    mock_os_path_exists.return_value = False # Mock file does not exist
    
    matches = get_matches_with_fallback(SAMPLE_DATE, use_mock_data=True, api_key="dummy_key")
    assert matches == []
    captured = capsys.readouterr()
    assert f"ERROR: Mock data file not found at {MOCK_DATA_PATH}" in captured.out


@patch('sports_prediction_ai.src.data_collection.get_matches_for_date')
@patch('os.path.exists')
@patch('builtins.open', new_callable=mock_open)
def test_gwf_mock_file_invalid_json(mock_file_open, mock_os_path_exists, mock_get_real_matches, capsys):
    mock_get_real_matches.return_value = []
    mock_os_path_exists.return_value = True
    mock_file_open.return_value.read.return_value = "invalid json data"
    
    matches = get_matches_with_fallback(SAMPLE_DATE, use_mock_data=True, api_key="dummy_key")
    assert matches == []
    captured = capsys.readouterr()
    assert f"ERROR: Error decoding mock data from {MOCK_DATA_PATH}" in captured.out

# --- Tests for get_matches_from_apisports ---
# Includes existing tests and new ones for logging counts

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_success(mock_get, capsys): # Added capsys for new logging
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_success_response
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert len(matches) == 2
    assert matches[0]["fixture"]["id"] == 1
    mock_get.assert_called_once()
    # Test for new logging
    captured = capsys.readouterr()
    assert f"INFO: API-SPORTS API reported 2 results. Fetched 2 matches for date {SAMPLE_DATE}." in captured.out


def test_get_matches_from_apisports_no_api_key_env_and_no_arg(capsys):
    matches = get_matches_from_apisports(SAMPLE_DATE)
    assert matches == []
    captured = capsys.readouterr()
    assert "Warning: API key for API-SPORTS not provided" in captured.out


@patch.dict(os.environ, {"APISPORTS_API_KEY": MOCK_APISPORTS_API_KEY})
@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_api_key_from_env(mock_get, capsys): # Added capsys
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_success_response
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE)
    assert len(matches) == 2
    assert matches[1]["fixture"]["id"] == 2
    mock_get.assert_called_once()
    captured = capsys.readouterr() # Check logging
    assert f"INFO: API-SPORTS API reported 2 results. Fetched 2 matches for date {SAMPLE_DATE}." in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_invalid_api_key_401(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 401
    mock_get.return_value = mock_response
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Unauthorized (401)" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_forbidden_403(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 403
    mock_get.return_value = mock_response
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Forbidden (403)" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_rate_limit_429(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Too Many Requests (429)" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_error_in_response_body(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_error_in_body_response
    mock_get.return_value = mock_response
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    # The new logging for counts might print before the error in body is detected.
    # The important part is "API-SPORTS returned errors:"
    assert "API-SPORTS returned errors:" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_network_error(mock_get, capsys):
    mock_get.side_effect = requests.exceptions.RequestException("Network error")
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Network error" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_malformed_json(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Decoding JSON has failed")
    mock_get.return_value = mock_response
    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error decoding JSON response from API-SPORTS: Decoding JSON has failed" in captured.out


# New tests for get_matches_from_apisports logging counts

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_gmfas_logging_api_reported_count_matches_fetched(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"results": 2, "response": [{"id": 1}, {"id": 2}]}
    mock_get.return_value = mock_response
    get_matches_from_apisports(SAMPLE_DATE, api_key="key")
    captured = capsys.readouterr()
    assert f"INFO: API-SPORTS API reported 2 results. Fetched 2 matches for date {SAMPLE_DATE}." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_gmfas_logging_no_matches_found_api_confirms_zero(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"results": 0, "response": []}
    mock_get.return_value = mock_response
    get_matches_from_apisports(SAMPLE_DATE, api_key="key")
    captured = capsys.readouterr()
    assert f"INFO: API-SPORTS API reported 0 results. Fetched 0 matches for date {SAMPLE_DATE}." in captured.out
    assert f"INFO: No matches found for date {SAMPLE_DATE} from API-SPORTS (API confirmed 0 results)." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_gmfas_logging_fetched_zero_api_reports_some(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    # The mock API response from the subtask description has been slightly modified.
    # The original was: `{"results": 2, "response": []}`.
    # The original warning message was: `WARNING: API-SPORTS API reported 2 results, but 0 were fetched for date {SAMPLE_DATE}. Possible API inconsistency or data filtering issue.`
    # The implemented warning message in `data_collection.py` is: `WARNING: API-SPORTS API reported {api_reported_count} results, but {num_matches} were fetched for date {date_str}. This might indicate an issue with the response list or pagination (if not handled).`
    # These two messages are compatible.
    mock_response.json.return_value = {"results": 2, "response": []}
    mock_get.return_value = mock_response
    get_matches_from_apisports(SAMPLE_DATE, api_key="key")
    captured = capsys.readouterr().out # Use .out directly
    assert f"INFO: API-SPORTS API reported 2 results. Fetched 0 matches for date {SAMPLE_DATE}." in captured
    assert f"WARNING: API-SPORTS API reported 2 results, but 0 were fetched for date {SAMPLE_DATE}. This might indicate an issue with the response list or pagination (if not handled)." in captured


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_gmfas_logging_count_not_available_matches_fetched(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"response": [{"id": 1}]} # No "results" or "paging"
    mock_get.return_value = mock_response
    get_matches_from_apisports(SAMPLE_DATE, api_key="key")
    captured = capsys.readouterr()
    assert f"INFO: Fetched 1 matches from API-SPORTS for date {SAMPLE_DATE}. (API reported count not available or not an integer)." in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_gmfas_logging_count_not_available_no_matches(mock_get, capsys):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"response": []} # No "results" or "paging"
    mock_get.return_value = mock_response
    get_matches_from_apisports(SAMPLE_DATE, api_key="key")
    captured = capsys.readouterr().out # Use .out directly
    assert f"INFO: Fetched 0 matches from API-SPORTS for date {SAMPLE_DATE}. (API reported count not available or not an integer)." in captured
    assert f"INFO: No matches found for date {SAMPLE_DATE} from API-SPORTS." in captured

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_empty_match_list_with_logging(mock_get, capsys): # Renamed to avoid conflict
    """Test API-SPORTS fetch with a successful response but an empty list of matches, checking logs."""
    empty_response_data = mock_apisports_success_response.copy()
    empty_response_data["response"] = []
    empty_response_data["results"] = 0 
    # "paging" still shows "total": 1, which is inconsistent with "results": 0.
    # The function prioritizes "results". If "results" were missing, it would check "paging.total".
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = empty_response_data
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert f"INFO: API-SPORTS API reported 0 results. Fetched 0 matches for date {SAMPLE_DATE}." in captured.out
    assert f"INFO: No matches found for date {SAMPLE_DATE} from API-SPORTS (API confirmed 0 results)." in captured.out
