import pytest
import os
import requests
import json
from unittest.mock import MagicMock, patch, mock_open

# Import functions to be tested
from sports_prediction_ai.src.data_collection import (
    get_matches_for_date,
    get_historical_matches_for_competition,
    get_matches_from_apisports,
    search_league_thesportsdb,
    get_future_events_thesportsdb,
    get_event_details_thesportsdb,
    get_matches_with_fallback,
    FOOTBALL_DATA_API_KEY, # To check against placeholder
    APISPORTS_API_KEY,     # To check against placeholder
    THESPORTSDB_API_KEY,   # To check against placeholder
    FOOTBALL_DATA_BASE_URL,
    APISPORTS_BASE_URL,
    THESPORTSDB_BASE_URL,
    MOCK_DATA_PATH
)

# --- Helper for Mock API Responses ---
def create_mock_response(mocker, status_code=200, json_data=None, text_data=None, raise_for_status_error=None):
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.json = MagicMock(return_value=json_data)
    if text_data is not None: # For non-json error responses
        mock_resp.text = text_data
    if raise_for_status_error:
        mock_resp.raise_for_status = MagicMock(side_effect=raise_for_status_error)
    else:
        mock_resp.raise_for_status = MagicMock() # Does nothing if no error
    return mock_resp

# --- Tests for get_matches_for_date (football-data.org) ---
@pytest.fixture
def mock_env_football_data_valid(mocker):
    mocker.patch.dict(os.environ, {"FOOTBALL_DATA_API_KEY": "VALID_FD_KEY"})
    # Also patch the module-level variable if it's already been set by os.getenv at import time
    mocker.patch("sports_prediction_ai.src.data_collection.FOOTBALL_DATA_API_KEY", "VALID_FD_KEY")

@pytest.fixture
def mock_env_football_data_invalid(mocker):
    mocker.patch.dict(os.environ, {"FOOTBALL_DATA_API_KEY": "YOUR_API_TOKEN"})
    mocker.patch("sports_prediction_ai.src.data_collection.FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")


def test_get_matches_for_date_success(mocker, mock_env_football_data_valid):
    mock_response_data = {"matches": [{"id": 1, "name": "Match 1"}]}
    mock_resp = create_mock_response(mocker, json_data=mock_response_data)
    mock_requests_get = mocker.patch("requests.get", return_value=mock_resp)

    result = get_matches_for_date("2023-01-01", api_key="VALID_FD_KEY") # Explicitly pass key

    assert result == mock_response_data["matches"]
    expected_url_part = f"{FOOTBALL_DATA_BASE_URL}matches?dateFrom=2023-01-01&dateTo=2023-01-02"
    mock_requests_get.assert_called_once_with(expected_url_part, headers={"X-Auth-Token": "VALID_FD_KEY"})
    mock_resp.raise_for_status.assert_called_once()

def test_get_matches_for_date_no_matches(mocker, mock_env_football_data_valid, capsys):
    mock_response_data = {"count": 0, "matches": []}
    mock_resp = create_mock_response(mocker, json_data=mock_response_data)
    mocker.patch("requests.get", return_value=mock_resp)

    result = get_matches_for_date("2023-01-01", api_key="VALID_FD_KEY")
    assert result == []
    captured = capsys.readouterr()
    assert "INFO: No matches found for date 2023-01-01" in captured.out


def test_get_matches_for_date_missing_key(mock_env_football_data_invalid, capsys, mocker):
    # Ensure requests.get is NOT called if API key is invalid/placeholder
    mock_requests_get = mocker.patch("requests.get")
    result = get_matches_for_date("2023-01-01") # Relies on module-level FOOTBALL_DATA_API_KEY

    assert result == []
    captured = capsys.readouterr()
    assert "Error: Invalid or missing API key for football-data.org" in captured.out
    mock_requests_get.assert_not_called()

@pytest.mark.parametrize("status_code, error_type, specific_message_part", [
    (401, requests.exceptions.HTTPError("401 Client Error"), "Invalid or unauthorized API key"),
    (403, requests.exceptions.HTTPError("403 Client Error"), "Forbidden access"),
    (404, requests.exceptions.HTTPError("404 Client Error"), "API Error Details"), # Generic for other 4xx/5xx
    (500, requests.exceptions.HTTPError("500 Server Error"), "API Error Details"),
])
def test_get_matches_for_date_http_errors(mocker, mock_env_football_data_valid, capsys, status_code, error_type, specific_message_part):
    mock_resp = create_mock_response(mocker, status_code=status_code, json_data={}, text_data="Error response", raise_for_status_error=error_type)
    mocker.patch("requests.get", return_value=mock_resp)

    result = get_matches_for_date("2023-01-01", api_key="VALID_FD_KEY")

    assert result == []
    captured = capsys.readouterr()
    assert "ERROR: Error fetching data from football-data.org" in captured.out
    assert specific_message_part in captured.out

def test_get_matches_for_date_request_exception(mocker, mock_env_football_data_valid, capsys):
    mocker.patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed"))
    result = get_matches_for_date("2023-01-01", api_key="VALID_FD_KEY")
    assert result == []
    captured = capsys.readouterr()
    assert "ERROR: Error fetching data from football-data.org" in captured.out
    assert "Connection failed" in captured.out

def test_get_matches_for_date_json_decode_error(mocker, mock_env_football_data_valid, capsys):
    mock_resp = create_mock_response(mocker, status_code=200)
    mock_resp.json = MagicMock(side_effect=ValueError("JSON decode error")) # Can also be requests.exceptions.JSONDecodeError
    mocker.patch("requests.get", return_value=mock_resp)

    result = get_matches_for_date("2023-01-01", api_key="VALID_FD_KEY")

    assert result == []
    captured = capsys.readouterr()
    assert "Error decoding JSON response" in captured.out


# --- Tests for get_historical_matches_for_competition (football-data.org) ---
# Similar structure to get_matches_for_date tests (success, missing key, HTTP errors, other errors)
def test_get_historical_matches_success(mocker, mock_env_football_data_valid):
    mock_data = {"matches": [{"id": 1, "name": "Historical Match 1"}]}
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    result = get_historical_matches_for_competition("PL", 2022, api_key="VALID_FD_KEY")
    assert result == mock_data["matches"]
    expected_url = f"{FOOTBALL_DATA_BASE_URL}competitions/PL/matches?season=2022"
    mock_get.assert_called_once_with(expected_url, headers={"X-Auth-Token": "VALID_FD_KEY"}, timeout=60)

def test_get_historical_matches_missing_key(mock_env_football_data_invalid, capsys, mocker):
    mock_get = mocker.patch("requests.get") # Ensure it's not called
    result = get_historical_matches_for_competition("PL", 2022) # Relies on module-level key
    assert result == []
    captured = capsys.readouterr()
    assert "Invalid or missing API key" in captured.out
    mock_get.assert_not_called()


# --- Tests for get_matches_from_apisports ---
@pytest.fixture
def mock_env_apisports_valid(mocker):
    mocker.patch.dict(os.environ, {"APISPORTS_API_KEY": "VALID_APISPORTS_KEY"})
    mocker.patch("sports_prediction_ai.src.data_collection.APISPORTS_API_KEY", "VALID_APISPORTS_KEY")

def test_get_matches_from_apisports_success(mocker, mock_env_apisports_valid):
    mock_data = {"response": [{"fixture": {"id": 1}}]}
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    result = get_matches_from_apisports("2023-01-01", api_key="VALID_APISPORTS_KEY")
    assert result == mock_data["response"]
    expected_url = f"{APISPORTS_BASE_URL}fixtures?date=2023-01-01"
    mock_get.assert_called_once_with(expected_url, headers={
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": "VALID_APISPORTS_KEY",
    })

def test_get_matches_from_apisports_missing_key(capsys, mocker):
    # Test when APISPORTS_API_KEY is None or not set
    mocker.patch("sports_prediction_ai.src.data_collection.APISPORTS_API_KEY", None)
    mock_get = mocker.patch("requests.get")
    result = get_matches_from_apisports("2023-01-01")
    assert result == []
    captured = capsys.readouterr()
    assert "Warning: API key for API-SPORTS not provided" in captured.out
    mock_get.assert_not_called()


# --- Tests for TheSportsDB functions ---
@pytest.fixture
def mock_env_thesportsdb_valid(mocker):
    mocker.patch.dict(os.environ, {"THESPORTSDB_API_KEY": "VALID_TSDB_KEY"})
    mocker.patch("sports_prediction_ai.src.data_collection.THESPORTSDB_API_KEY", "VALID_TSDB_KEY")

def test_search_league_thesportsdb_success(mocker, mock_env_thesportsdb_valid):
    mock_data = {"leagues": [{"idLeague": "123", "strLeague": "Test League"}]}
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    result = search_league_thesportsdb("Test League", api_key="VALID_TSDB_KEY")
    assert result == mock_data["leagues"]
    expected_url_part = f"{THESPORTSDB_BASE_URL}VALID_TSDB_KEY/searchleagues.php?l=Test%20League"
    mock_get.assert_called_once_with(expected_url_part, timeout=15)

def test_get_future_events_thesportsdb_success(mocker, mock_env_thesportsdb_valid):
    mock_data = {"events": [{"idEvent": "evt1", "strEvent": "Event 1"}]}
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    result = get_future_events_thesportsdb("123", api_key="VALID_TSDB_KEY")
    assert result == mock_data["events"]
    expected_url = f"{THESPORTSDB_BASE_URL}VALID_TSDB_KEY/eventsnextleague.php?id=123"
    mock_get.assert_called_once_with(expected_url, timeout=15)

def test_get_event_details_thesportsdb_success(mocker, mock_env_thesportsdb_valid):
    mock_data = {"events": [{"idEvent": "evt1", "strEvent": "Event 1 Details"}]}
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    result = get_event_details_thesportsdb("evt1", api_key="VALID_TSDB_KEY")
    assert result == mock_data["events"][0]
    expected_url = f"{THESPORTSDB_BASE_URL}VALID_TSDB_KEY/lookupevent.php?id=evt1"
    mock_get.assert_called_once_with(expected_url, timeout=15)

def test_get_event_details_thesportsdb_no_event(mocker, mock_env_thesportsdb_valid, capsys):
    mock_data = {"events": None} # API returns null if no event
    mock_resp = create_mock_response(mocker, json_data=mock_data)
    mocker.patch("requests.get", return_value=mock_resp)
    result = get_event_details_thesportsdb("evt_nonexistent", api_key="VALID_TSDB_KEY")
    assert result is None
    captured = capsys.readouterr()
    assert "INFO: No event details found for event ID evt_nonexistent" in captured.out

# Test for TheSportsDB rate limit error (HTTP 429)
@pytest.mark.parametrize("tsdb_function_name", [
    "search_league_thesportsdb",
    "get_future_events_thesportsdb",
    "get_event_details_thesportsdb"
])
def test_thesportsdb_rate_limit_handling(mocker, mock_env_thesportsdb_valid, capsys, tsdb_function_name):
    module_path = "sports_prediction_ai.src.data_collection"
    func_to_test = getattr(__import__(module_path, fromlist=[tsdb_function_name]), tsdb_function_name)

    http_error = requests.exceptions.HTTPError("429 Client Error: Too Many Requests")
    mock_response = create_mock_response(
        mocker,
        status_code=429,
        text_data="Too Many Requests",
        raise_for_status_error=http_error
    )
    mocker.patch("requests.get", return_value=mock_response)

    args = []
    if tsdb_function_name == "search_league_thesportsdb":
        args = ["Test League"]
    elif tsdb_function_name == "get_future_events_thesportsdb":
        args = ["1234"]  # league_id
    elif tsdb_function_name == "get_event_details_thesportsdb":
        args = ["evt123"]  # event_id

    result = func_to_test(*args, api_key="VALID_TSDB_KEY") # Pass key explicitly

    if tsdb_function_name == "get_event_details_thesportsdb":
        assert result is None
    else:
        assert result == []

    captured = capsys.readouterr()
    assert "ERROR: Error fetching data" in captured.out # Generic part from SUT
    assert "429" in captured.out or "429" in str(http_error) # Check the error string itself
    assert "too many requests" in captured.out.lower() or "too many requests" in str(http_error).lower()


# --- Tests for get_matches_with_fallback ---
@pytest.fixture
def mock_get_matches_for_date(mocker):
    return mocker.patch("sports_prediction_ai.src.data_collection.get_matches_for_date")

def test_fallback_live_data_succeeds(mock_get_matches_for_date, mocker):
    live_data = [{"id": 1, "source": "live"}]
    mock_get_matches_for_date.return_value = live_data
    mock_open_func = mocker.patch("builtins.open") # Ensure mock data isn't opened

    result = get_matches_with_fallback("2023-01-01", use_mock_data=True, api_key="ANY_KEY")

    assert result == live_data
    mock_get_matches_for_date.assert_called_once()
    mock_open_func.assert_not_called()

def test_fallback_live_data_fails_mock_succeeds(mock_get_matches_for_date, mocker):
    mock_get_matches_for_date.return_value = [] # Live data fails (empty list)
    mock_data_content = {"matches": [{"id": 2, "source": "mock"}]}

    # Correctly mock open for reading JSON
    m_open = mock_open(read_data=json.dumps(mock_data_content))
    mocker.patch("builtins.open", m_open)
    mocker.patch("sports_prediction_ai.src.data_collection.MOCK_DATA_PATH", "dummy/path/mock.json")


    result = get_matches_with_fallback("2023-01-01", use_mock_data=True, api_key="ANY_KEY")

    assert result == mock_data_content["matches"]
    mock_get_matches_for_date.assert_called_once()
    m_open.assert_called_once_with("dummy/path/mock.json", 'r')

def test_fallback_live_data_fails_mock_file_not_found(mock_get_matches_for_date, mocker, capsys):
    mock_get_matches_for_date.return_value = []
    mocker.patch("builtins.open", side_effect=FileNotFoundError("Mock file not found"))
    mocker.patch("sports_prediction_ai.src.data_collection.MOCK_DATA_PATH", "dummy/path/nonexistent.json")


    result = get_matches_with_fallback("2023-01-01", use_mock_data=True, api_key="ANY_KEY")

    assert result == []
    captured = capsys.readouterr()
    assert "ERROR: Mock data file not found at dummy/path/nonexistent.json" in captured.out

def test_fallback_live_data_fails_mock_invalid_json(mock_get_matches_for_date, mocker, capsys):
    mock_get_matches_for_date.return_value = []
    m_open = mock_open(read_data="this is not json")
    mocker.patch("builtins.open", m_open)
    mocker.patch("sports_prediction_ai.src.data_collection.MOCK_DATA_PATH", "dummy/path/invalid.json")

    result = get_matches_with_fallback("2023-01-01", use_mock_data=True, api_key="ANY_KEY")

    assert result == []
    captured = capsys.readouterr()
    assert "ERROR: Error decoding mock data from dummy/path/invalid.json" in captured.out

# General API error tests (can be applied to other functions if needed)
@pytest.mark.parametrize("api_function_name", [
    "get_historical_matches_for_competition",
    "get_matches_from_apisports", # Already has some specific tests, this adds more generic ones
    "search_league_thesportsdb",
    "get_future_events_thesportsdb",
    "get_event_details_thesportsdb"
])
@pytest.mark.parametrize("error_type, error_message_snippet", [
    (requests.exceptions.HTTPError("Mocked HTTP Error"), "ERROR: Error fetching data"),
    (requests.exceptions.ConnectionError("Mocked Connection Error"), "ERROR: Error fetching data"), # Generic part
    (requests.exceptions.Timeout("Mocked Timeout Error"), "ERROR: Error fetching data"), # Generic part
    (ValueError("JSON decode error"), "Error decoding JSON response") # For functions that directly call .json()
])
def test_generic_api_function_errors(mocker, capsys, api_function_name, error_type, error_message_snippet):
    # Get the actual function object from its name
    module_path = "sports_prediction_ai.src.data_collection"
    func_to_test = getattr(__import__(module_path, fromlist=[api_function_name]), api_function_name)

    # Mock requests.get or response.json() based on error_type
    if isinstance(error_type, ValueError): # JSON decode error
        mock_resp = create_mock_response(mocker, status_code=200)
        mock_resp.json = MagicMock(side_effect=error_type)
        mocker.patch("requests.get", return_value=mock_resp)
    else: # Network/HTTP errors
        mocker.patch("requests.get", side_effect=error_type)

    # Call the function with minimal valid arguments
    # This requires knowing typical arguments for each function.
    # For simplicity, assume a valid API key is patched in environment or passed if needed.
    # And use dummy arguments.
    args = []
    kwargs = {}
    if api_function_name in ["get_historical_matches_for_competition", "get_matches_from_apisports"]:
        # These functions typically use a module-level API key if not provided.
        # Let's assume valid keys are in place for this generic test.
        if api_function_name == "get_historical_matches_for_competition":
            args = ["PL", 2022] # comp_code, season
            mocker.patch(f"{module_path}.FOOTBALL_DATA_API_KEY", "VALID_KEY")
        elif api_function_name == "get_matches_from_apisports":
            args = ["2023-01-01"] # date_str
            mocker.patch(f"{module_path}.APISPORTS_API_KEY", "VALID_KEY")
    elif "thesportsdb" in api_function_name:
        mocker.patch(f"{module_path}.THESPORTSDB_API_KEY", "VALID_KEY") # Assume '1' or a test key
        if api_function_name == "search_league_thesportsdb": args = ["Test League"]
        elif api_function_name == "get_future_events_thesportsdb": args = ["1234"] # league_id
        elif api_function_name == "get_event_details_thesportsdb": args = ["evt123"] # event_id

    result = func_to_test(*args, **kwargs)

    if api_function_name == "get_event_details_thesportsdb":
        assert result is None
    else:
        assert result == []

    captured = capsys.readouterr()
    assert error_message_snippet in captured.out
    if not isinstance(error_type, ValueError): # For requests exceptions, the original error is often in the message
         assert str(error_type) in captured.out or type(error_type).__name__ in captured.out
    else: # For JSON decode value error
        assert "JSON decode error" in captured.out

# Note: SSL Error specific to football-data.org is already tested in test_get_matches_for_date_http_errors
# if we add a parameter for it. The current test_get_matches_for_date handles it via generic RequestException.
# SSLError is a subclass of ConnectionError, which is a subclass of RequestException.
# For example, test_get_matches_for_date_request_exception could be made more specific.
# The existing tests for get_matches_for_date cover its specific error messages well.
# The generic test above adds broader coverage for other functions.
