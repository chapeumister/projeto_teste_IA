import pytest
import requests
import os
from unittest.mock import patch, Mock
from sports_prediction_ai.src.data_collection import get_matches_from_apisports, get_matches_for_date

# Constants for API-SPORTS tests
MOCK_APISPORTS_API_KEY = "test_apisports_key"
SAMPLE_DATE = "2023-01-01"

# Sample successful API-SPORTS response
mock_apisports_success_response = {
    "get": "fixtures",
    "parameters": {"date": SAMPLE_DATE},
    "errors": [],
    "results": 2,
    "paging": {"current": 1, "total": 1},
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


@pytest.fixture(autouse=True)
def manage_env_vars():
    # Clean up environment variables before and after each test
    original_apisports_key = os.environ.pop("APISPORTS_API_KEY", None)
    original_football_data_key = os.environ.pop("FOOTBALL_DATA_API_KEY", None)
    yield
    if original_apisports_key is not None:
        os.environ["APISPORTS_API_KEY"] = original_apisports_key
    if original_football_data_key is not None:
        os.environ["FOOTBALL_DATA_API_KEY"] = original_football_data_key


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_success(mock_get):
    """Test successful fetch from API-SPORTS."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_success_response
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert len(matches) == 2
    assert matches[0]["fixture"]["id"] == 1
    mock_get.assert_called_once()


def test_get_matches_from_apisports_no_api_key_env_and_no_arg(capsys):
    """Test API-SPORTS fetch when API key is not in env and not passed as arg."""
    # Ensure APISPORTS_API_KEY is not set by manage_env_vars fixture
    matches = get_matches_from_apisports(SAMPLE_DATE)
    assert matches == []
    captured = capsys.readouterr()
    assert "Warning: API key for API-SPORTS not provided" in captured.out


@patch.dict(os.environ, {"APISPORTS_API_KEY": MOCK_APISPORTS_API_KEY})
@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_api_key_from_env(mock_get):
    """Test API-SPORTS fetch when API key is sourced from environment variable."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_success_response
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE) # No api_key argument
    assert len(matches) == 2
    assert matches[1]["fixture"]["id"] == 2
    mock_get.assert_called_once() # Verifies requests.get was called


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_invalid_api_key_401(mock_get, capsys):
    """Test API-SPORTS fetch with a 401 Unauthorized error."""
    mock_response = Mock()
    mock_response.status_code = 401
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Unauthorized (401)" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_forbidden_403(mock_get, capsys):
    """Test API-SPORTS fetch with a 403 Forbidden error."""
    mock_response = Mock()
    mock_response.status_code = 403
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Forbidden (403)" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_rate_limit_429(mock_get, capsys):
    """Test API-SPORTS fetch with a 429 Too Many Requests error."""
    mock_response = Mock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Too Many Requests (429)" in captured.out
    

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_error_in_response_body(mock_get, capsys):
    """Test API-SPORTS when API returns 200 OK but with an error message in the JSON body."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_apisports_error_in_body_response
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "API-SPORTS returned errors:" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_empty_match_list(mock_get):
    """Test API-SPORTS fetch with a successful response but an empty list of matches."""
    empty_response_data = mock_apisports_success_response.copy()
    empty_response_data["response"] = []
    empty_response_data["results"] = 0
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = empty_response_data
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_network_error(mock_get, capsys):
    """Test API-SPORTS fetch with a requests.exceptions.RequestException."""
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from API-SPORTS: Network error" in captured.out


@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_from_apisports_malformed_json(mock_get, capsys):
    """Test API-SPORTS fetch with a response that is not valid JSON."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Decoding JSON has failed") # Simulate JSONDecodeError
    mock_get.return_value = mock_response

    matches = get_matches_from_apisports(SAMPLE_DATE, api_key=MOCK_APISPORTS_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error decoding JSON response from API-SPORTS: Decoding JSON has failed" in captured.out

# --- Tests for get_matches_for_date (football-data.org) ---
# This function was pre-existing, but good to have a couple of tests.
# We'll only add basic ones as it's not the primary focus of *this* subtask.

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

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_success(mock_get):
    """Test successful fetch from football-data.org."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_football_data_success_response
    mock_get.return_value = mock_response

    # Test with API key provided as argument
    matches = get_matches_for_date(SAMPLE_DATE, api_key=MOCK_FOOTBALL_DATA_API_KEY)
    assert len(matches) == 1
    assert matches[0]["id"] == 101
    mock_get.assert_called_once()


def test_get_matches_for_date_no_api_key(capsys):
    """Test football-data.org fetch when API key is placeholder."""
    # Default placeholder FOOTBALL_DATA_API_KEY = "YOUR_API_TOKEN" is used by the function
    # if no key is passed and env var is not set.
    matches = get_matches_for_date(SAMPLE_DATE) # No key passed, no env var set by manage_env_vars
    assert matches == []
    captured = capsys.readouterr()
    assert "Warning: Using a placeholder API key for football-data.org" in captured.out

@patch('sports_prediction_ai.src.data_collection.requests.get')
def test_get_matches_for_date_network_error(mock_get, capsys):
    """Test football-data.org fetch with a network error."""
    mock_get.side_effect = requests.exceptions.RequestException("Connection failed")

    matches = get_matches_for_date(SAMPLE_DATE, api_key=MOCK_FOOTBALL_DATA_API_KEY)
    assert matches == []
    captured = capsys.readouterr()
    assert "Error fetching data from football-data.org: Connection failed" in captured.out
