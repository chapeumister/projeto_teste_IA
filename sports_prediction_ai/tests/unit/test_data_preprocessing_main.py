import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import sqlite3 # Used by the module, so good to have for type hints if needed
from unittest.mock import MagicMock, patch
from datetime import datetime

# Import the function to be tested
from sports_prediction_ai.src.data_preprocessing import get_ml_ready_dataframe_from_db

# --- Fixtures ---

@pytest.fixture
def mock_db_conn(mocker):
    """Provides a mocked SQLite connection and cursor."""
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_conn.cursor.return_value = mock_cursor
    # If pd.read_sql_query is used, it might not use cursor directly in the way older code might.
    # We will mock pd.read_sql_query directly in tests.
    mocker.patch("pandas.read_sql_query", new_callable=MagicMock) # Mock at pandas level
    return mock_conn


@pytest.fixture
def mock_engineer_form_features(mocker):
    """Mocks the engineer_form_features function."""
    mock_func = mocker.patch("sports_prediction_ai.src.data_preprocessing.engineer_form_features")

    def dummy_engineer_form(processed_df, historical_df, num_games=5):
        # Simulate adding form columns
        df_copy = processed_df.copy() # Work on a copy
        form_cols = [
            'home_form_overall_W', 'home_form_overall_D', 'home_form_overall_L', 'home_form_overall_games_played',
            'away_form_overall_W', 'away_form_overall_D', 'away_form_overall_L', 'away_form_overall_games_played',
            'home_form_home_W', 'home_form_home_D', 'home_form_home_L', 'home_form_home_games_played',
            'away_form_away_W', 'away_form_away_D', 'away_form_away_L', 'away_form_away_games_played',
            'home_form_away_W', 'home_form_away_D', 'home_form_away_L', 'home_form_away_games_played',
            'away_form_home_W', 'away_form_home_D', 'away_form_home_L', 'away_form_home_games_played',
        ]
        for col in form_cols:
            if col.endswith("_W"): df_copy[col] = 1 # Dummy win
            elif col.endswith("_D"): df_copy[col] = 0 # Dummy draw
            elif col.endswith("_L"): df_copy[col] = 0 # Dummy loss
            elif col.endswith("_games_played"): df_copy[col] = 1 # Dummy games played
        return df_copy

    mock_func.side_effect = dummy_engineer_form
    return mock_func

# --- Sample Data for Mocking DB Responses ---
# This represents data *after* pd.read_sql_query has processed it
SAMPLE_MATCHES_DATA = pd.DataFrame({
    'match_id': [1, 2],
    'datetime': [datetime(2023, 1, 1, 15, 0, 0), datetime(2023, 1, 2, 18, 0, 0)],
    'home_team_id': [10, 12],
    'home_team_name': ['Team A', 'Team C'],
    'away_team_id': [11, 10],
    'away_team_name': ['Team B', 'Team A'], # Team A plays as away in second match
    'league_id': [100, 100],
    'league_name': ['Premier League', 'Premier League'],
    'home_score': [2, 0],
    'away_score': [1, 0],
    'status': ['FINISHED', 'FINISHED'],
    'source_match_id': ['src1', 'src2'],
    'source': ['TestSource', 'TestSource']
})

SAMPLE_HISTORICAL_MATCHES_DATA = pd.DataFrame({
    'match_id': [101, 102],
    # Column names here should match the aliases in the historical_query in SUT
    'utcDate': [datetime(2022, 12, 1, 15, 0, 0), datetime(2022, 12, 5, 18, 0, 0)],
    'home_team_id': [10, 11],
    'away_team_id': [11, 12],
    'home_team_score': [1, 2], # Aliased from home_score
    'away_team_score': [0, 2]  # Aliased from away_score
})


# --- Test Cases ---

def test_get_ml_data_basic_flow(mock_db_conn, mock_engineer_form_features, mocker):
    # Configure the mock for pd.read_sql_query
    # It will be called twice: once for main matches, once for historical
    mock_read_sql = mocker.patch("pandas.read_sql_query")
    mock_read_sql.side_effect = [
        SAMPLE_MATCHES_DATA.copy(),       # First call returns main matches
        SAMPLE_HISTORICAL_MATCHES_DATA.copy() # Second call returns historical matches
    ]

    result_df = get_ml_ready_dataframe_from_db(mock_db_conn)

    assert not result_df.empty
    assert mock_read_sql.call_count == 2

    # Check that engineer_form_features was called
    mock_engineer_form_features.assert_called_once()
    # We can also check the arguments it was called with if needed
    call_args = mock_engineer_form_features.call_args[0]
    assert len(call_args) == 2 # processed_matches_df, historical_matches_df (num_games has default)
    pd.testing.assert_frame_equal(call_args[0].reset_index(drop=True), SAMPLE_MATCHES_DATA.rename(columns={'datetime': 'utcDate'}).reset_index(drop=True))
    # The historical data passed to engineer_form_features should have 'utcDate' as datetime
    expected_hist_for_engineer = SAMPLE_HISTORICAL_MATCHES_DATA.copy()
    expected_hist_for_engineer['utcDate'] = pd.to_datetime(expected_hist_for_engineer['utcDate'])
    pd.testing.assert_frame_equal(call_args[1].reset_index(drop=True), expected_hist_for_engineer.reset_index(drop=True))


    assert 'match_outcome' in result_df.columns
    assert 'home_form_overall_W' in result_df.columns # Check a dummy form column

    # Check match_outcome calculation for the first match (Team A 2 vs Team B 1 -> Home Win = 1)
    assert result_df.loc[result_df['match_id'] == 1, 'match_outcome'].iloc[0] == 1
    # Check match_outcome for the second match (Team C 0 vs Team A 0 -> Draw = 0, but sample data is 2-0, so Home win = 1)
    # Oh, sample data is Team C 2 vs Team A 0. So home win (1)
    assert result_df.loc[result_df['match_id'] == 2, 'match_outcome'].iloc[0] == 1

    # Check utcDate column type (renamed from datetime)
    assert pd.api.types.is_datetime64_any_dtype(result_df['utcDate'])


def test_get_ml_data_with_league_filter(mock_db_conn, mock_engineer_form_features, mocker):
    mock_read_sql = mocker.patch("pandas.read_sql_query")
    mock_read_sql.side_effect = [SAMPLE_MATCHES_DATA.copy(), SAMPLE_HISTORICAL_MATCHES_DATA.copy()]

    league_filter = ['Premier League']
    get_ml_ready_dataframe_from_db(mock_db_conn, league_names=league_filter)

    # Check the main query for league filter
    first_call_args = mock_read_sql.call_args_list[0]
    query_string = first_call_args.args[0] # query is the first positional arg
    query_params = first_call_args.args[2] # params is the third positional arg

    assert "l.name IN (?)" in query_string # Check for placeholder
    assert query_params == league_filter


def test_get_ml_data_with_date_filters(mock_db_conn, mock_engineer_form_features, mocker):
    mock_read_sql = mocker.patch("pandas.read_sql_query")
    mock_read_sql.side_effect = [SAMPLE_MATCHES_DATA.copy(), SAMPLE_HISTORICAL_MATCHES_DATA.copy()]

    date_from_filter = "2023-01-01"
    date_to_filter = "2023-01-31"
    get_ml_ready_dataframe_from_db(mock_db_conn, date_from=date_from_filter, date_to=date_to_filter)

    first_call_args = mock_read_sql.call_args_list[0]
    query_string = first_call_args.args[0]
    query_params = first_call_args.args[2]

    assert "DATE(m.datetime) >= DATE(?)" in query_string
    assert "DATE(m.datetime) <= DATE(?)" in query_string
    assert date_from_filter in query_params
    assert date_to_filter in query_params


def test_get_ml_data_no_matches_found(mock_db_conn, mock_engineer_form_features, mocker):
    mock_read_sql = mocker.patch("pandas.read_sql_query")
    # Simulate no matches found for the main query
    mock_read_sql.return_value = pd.DataFrame(columns=SAMPLE_MATCHES_DATA.columns)
                                           # Or just an empty DataFrame if schema is complex

    result_df = get_ml_ready_dataframe_from_db(mock_db_conn)

    assert result_df.empty
    mock_read_sql.assert_called_once() # Only called for main matches, then exits
    mock_engineer_form_features.assert_not_called()


def test_get_ml_data_no_historical_matches(mock_db_conn, mock_engineer_form_features, mocker):
    mock_read_sql = mocker.patch("pandas.read_sql_query")
    mock_read_sql.side_effect = [
        SAMPLE_MATCHES_DATA.copy(), # Main matches found
        pd.DataFrame(columns=SAMPLE_HISTORICAL_MATCHES_DATA.columns) # No historical matches
    ]

    result_df = get_ml_ready_dataframe_from_db(mock_db_conn)

    assert not result_df.empty
    assert mock_read_sql.call_count == 2
    # mock_engineer_form_features.assert_called_once() # This was incorrect, SUT handles this.

    # Check that form columns are present and defaulted to 0, due to SUT's internal handling
    # The mocked engineer_form_features fixture adds columns with 1s, so this test
    # relies on the SUT's logic for when historical_matches_df_from_db is empty.
    # The SUT adds default 0 columns in this case *before* calling engineer_form_features.
    # So, the mocked engineer_form_features will overwrite these with its dummy 1s.
    # This test needs to check the state *before* the mock engineer_form_features would run,
    # OR adjust the mock to reflect the "no historical data" scenario.

    # Let's adjust the mock_engineer_form_features for this specific test case,
    # or verify the SUT's default column creation.
    # The SUT's code:
    # if historical_matches_df_from_db.empty:
    #     ... add empty form columns with 0s ...
    # else:
    #     matches_df = engineer_form_features(matches_df, historical_matches_df_from_db, num_games=5)
    # So, if historical is empty, our mock_engineer_form_features fixture (which is active for all tests in this module)
    # should NOT have been called by the SUT.

    # The SUT itself adds default 0-columns if historical_matches_df_from_db is empty.
    # Then it does NOT call engineer_form_features.
    mock_engineer_form_features.assert_not_called() # Verify the mocked one wasn't called.

    assert 'home_form_overall_W' in result_df.columns
    assert result_df['home_form_overall_W'].iloc[0] == 0 # Check for default 0 created by SUT
    assert result_df['home_form_overall_games_played'].iloc[0] == 0


def test_get_ml_data_db_query_error(mock_db_conn, mocker, capsys):
    # Mock pd.read_sql_query to raise a database error for the first call
    mock_read_sql = mocker.patch("pandas.read_sql_query", side_effect=pd.io.sql.DatabaseError("Simulated DB query error"))

    result_df = get_ml_ready_dataframe_from_db(mock_db_conn)

    assert result_df.empty
    mock_read_sql.assert_called_once() # Should fail on the first query
    captured = capsys.readouterr()
    assert "Database query error: Simulated DB query error" in captured.out

# To run: pytest sports_prediction_ai/tests/unit/test_data_preprocessing_main.py
