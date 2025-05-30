import pytest
import os
import pandas as pd
import sqlite3
import json
import yaml # For OpenFootball dummy data
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Import the main functions/entry points from the scripts
from sports_prediction_ai.src import database_setup # To setup schema if needed, though conftest does this
from sports_prediction_ai.src.database_importer import run_all_importers
from sports_prediction_ai.src import database_importer # Added for direct calls to its functions
from sports_prediction_ai.src.data_preprocessing import get_ml_ready_dataframe_from_db

# --- Helper to create dummy CSV ---
def _create_dummy_csv(filepath: Path, headers: list, rows: list[list]):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=headers).to_csv(filepath, index=False)
    print(f"Created dummy CSV: {filepath}")

# --- Helper to create dummy JSON ---
def _create_dummy_json(filepath: Path, data: dict | list):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Created dummy JSON: {filepath}")

# --- Helper to create dummy YAML ---
def _create_dummy_yaml(filepath: Path, data: dict):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        yaml.dump(data, f, indent=4)
    print(f"Created dummy YAML: {filepath}")


@pytest.fixture
def mock_downloaders(mocker, tmp_path):
    """Mocks all downloader functions to create dummy files in tmp_path."""
    mocks = {}

    # Kaggle
    def mock_download_kaggle(dataset_slug, base_path_str):
        base_path = Path(base_path_str) # Ensure it's a Path object
        print(f"Mocked Kaggle download for {dataset_slug} to {base_path}")
        dataset_name = dataset_slug.split('/')[-1]
        file_path = base_path / dataset_name / "results.csv" # Path structure from kaggle_downloader
        _create_dummy_csv(file_path,
                          ['date', 'home_team', 'away_team', 'home_score', 'away_score', 'tournament', 'city', 'country', 'neutral'],
                          [['2023-03-01', 'KaggleTeamA', 'KaggleTeamB', '1', '0', 'Kaggle Cup', 'KCity', 'KCountry', 'FALSE']])
        return True # download_kaggle_dataset returns True on success
    mocks['kaggle'] = mocker.patch("sports_prediction_ai.src.kaggle_downloader.download_kaggle_dataset", side_effect=mock_download_kaggle)

    # Soccer-Data.co.uk
    def mock_download_soccer_csv(url, path_str, filename):
        path = Path(path_str)
        print(f"Mocked SoccerDataUK download for {url} to {path / filename}")
        _create_dummy_csv(path / filename,
                          ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'B365H', 'B365D', 'B365A', 'Div'],
                          [['10/03/2023', 'SoccerTeamX', 'SoccerTeamY', '2', '2', '1.8', '3.5', '4.0', 'E0']])
        return True # download_csv_from_url returns True on success
    mocks['soccer'] = mocker.patch("sports_prediction_ai.src.soccer_data_downloader.download_csv_from_url", side_effect=mock_download_soccer_csv)

    # FiveThirtyEight
    def mock_clone_fivethirtyeight(repo_url, path_str):
        path = Path(path_str)
        print(f"Mocked FiveThirtyEight clone for {repo_url} to {path}")
        file_path = path / "soccer-spi" / "spi_matches.csv"
        _create_dummy_csv(file_path,
                           ['date','league','team1','team2','score1','score2','xg1','xg2','nsxg1','nsxg2','adj_score1','adj_score2'],
                           [['2023-03-15','FTE League','FTETeamP','FTETeamQ','3','0','2.5','0.5',None,None,None,None]])
        # clone_or_update_repo in fivethirtyeight_downloader doesn't return a bool, it just prints.
    mocks['fte'] = mocker.patch("sports_prediction_ai.src.fivethirtyeight_downloader.clone_or_update_repo", side_effect=mock_clone_fivethirtyeight)

    # OpenFootball
    def mock_clone_openfootball(repo_url, path_str):
        path = Path(path_str)
        print(f"Mocked OpenFootball clone for {repo_url} to {path}")
        # Simulate creating a specific YAML file that the importer expects
        repo_name = repo_url.split('/')[-1].replace('.git', '')
        if repo_name == "eng-england":
            yaml_file_path = path / "2022-23" / "1-premierleague.yml"
            _create_dummy_yaml(yaml_file_path, {"name": "English Premier League", "season": "2022-23", "clubs": ["OFTeamA", "OFTeamB"]})
        return True # clone_or_update_repo in openfootball_downloader returns bool
    # We also need to ensure find_and_parse_yaml_files doesn't try to re-parse in the E2E if we only test league/team import
    # The importer `import_openfootball_league_teams` takes parsed data, so we don't need to mock find_and_parse for it.
    # The `run_all_importers` function handles opening and parsing the YAML itself.
    mocks['openfootball_clone'] = mocker.patch("sports_prediction_ai.src.openfootball_downloader.clone_or_update_repo", side_effect=mock_clone_openfootball)

    return mocks


@pytest.fixture
def mock_data_collection_apis(mocker, tmp_path):
    """Mocks data_collection.py functions that make external API calls."""
    mocks = {}

    # Football-Data.org historical
    def mock_get_historical(competition_code, season, api_key=None):
        print(f"Mocked get_historical_matches for {competition_code} season {season}")
        return [{"id": 9001, "utcDate": "2023-03-10T19:00:00Z", "status": "FINISHED",
                 "competition": {"name": "FD.org League", "code": competition_code},
                 "homeTeam": {"id": 701, "name": "FDTeamHome"},
                 "awayTeam": {"id": 702, "name": "FDTeamAway"},
                 "score": {"fullTime": {"home": 1, "away": 1}}}]
    mocks['fd_hist'] = mocker.patch("sports_prediction_ai.src.data_collection.get_historical_matches_for_competition", side_effect=mock_get_historical)

    # Mock save_data_to_json to use tmp_path
    # This is essential because run_all_importers in database_importer.py reads these files.
    def mock_save_json(data, base_path_str, competition_code, season):
        # base_path_str in E2E test will be like `tmp_path / "football_data_org_historical"`
        # The importer expects base_path/COMP_CODE/SEASON/matches.json
        file_path = Path(base_path_str) / competition_code / str(season) / "matches.json"
        _create_dummy_json(file_path, data)
        print(f"Mocked save_data_to_json to {file_path}")
        return True
    mocks['fd_save_json'] = mocker.patch("sports_prediction_ai.src.data_collection.save_data_to_json", side_effect=mock_save_json)

    # TheSportsDB
    def mock_search_league_tsdb(league_name_query, api_key=None):
        print(f"Mocked search_league_thesportsdb for {league_name_query}")
        if "Premier League" in league_name_query:
            return [{"idLeague": "4328", "strLeague": "English Premier League", "strSport": "Soccer", "strCountryAlternate": "England"}]
        return []
    # These functions are imported and used by the E2E test directly, so we patch them in their original module.
    mocks['tsdb_search_league'] = mocker.patch("sports_prediction_ai.src.data_collection.search_league_thesportsdb", side_effect=mock_search_league_tsdb)

    def mock_get_future_events_tsdb(league_id, api_key=None):
        print(f"Mocked get_future_events_thesportsdb for league {league_id}")
        if league_id == "4328":
            return [{"idEvent": "tsdb_evt1", "strEvent": "TSDBTeam1 vs TSDBTeam2", "strHomeTeam": "TSDBTeam1", "strAwayTeam": "TSDBTeam2", "idLeague": "4328", "strLeague": "English Premier League", "strSport": "Soccer", "dateEvent": "2023-03-25", "strTime": "15:00:00", "intHomeScore": None, "intAwayScore": None, "strStatus": "Scheduled"}]
        return []
    mocks['tsdb_future_events'] = mocker.patch("sports_prediction_ai.src.data_collection.get_future_events_thesportsdb", side_effect=mock_get_future_events_tsdb)

    def mock_get_event_details_tsdb(event_id, api_key=None):
        print(f"Mocked get_event_details_thesportsdb for event {event_id}")
        if event_id == "tsdb_evt1":
            return {"idEvent": "tsdb_evt1", "strEvent": "TSDBTeam1 vs TSDBTeam2", "strHomeTeam": "TSDBTeam1", "strAwayTeam": "TSDBTeam2", "idLeague": "4328", "strLeague": "English Premier League", "strSport": "Soccer", "dateEvent": "2023-03-25", "strTime": "15:00:00", "intHomeScore": "1", "intAwayScore": "0", "strStatus": "Match Finished"}
        return None
    mocks['tsdb_event_details'] = mocker.patch("sports_prediction_ai.src.data_collection.get_event_details_thesportsdb", side_effect=mock_get_event_details_tsdb)

    return mocks


def test_full_pipeline_workflow(in_memory_db, mocker, tmp_path, mock_downloaders, mock_data_collection_apis):
    """
    End-to-end test for the data pipeline.
    Uses mocked downloaders and API callers, an in-memory DB, and temporary file paths.
    """
    db_conn = in_memory_db # Get the connection from the fixture

    # --- Phase 1: Simulate Data Collection/Download ---
    # The mock_downloaders fixture already "creates" dummy files when the actual downloader functions are called.
    # The E2E test doesn't call the downloader scripts' main blocks, but rather the `run_all_importers`
    # function relies on these files existing. So we need to simulate the *outcome* of downloaders.

    # Define base paths within tmp_path for each data source, as expected by run_all_importers
    data_paths_config = {
        "kaggle_datasets": tmp_path / "kaggle_datasets",
        "soccer_data_co_uk": tmp_path / "soccer_data_co_uk",
        "fivethirtyeight_data": tmp_path / "fivethirtyeight_data",
        "football_data_org_historical": tmp_path / "football_data_org_historical",
        "openfootball_data": tmp_path / "openfootball_data",
        # TheSportsDB data is handled by mocking data_collection functions that return data directly,
        # so its data isn't written to files by the downloader mocks in the same way.
    }

    # Create dummy files using the same logic as the mocks would (or ensure mocks are configured to use tmp_path correctly)
    # Kaggle
    mock_downloaders['kaggle']("martj42/international-football-results-from-1872-to-2017", str(data_paths_config["kaggle_datasets"]))
    # SoccerDataUK
    mock_downloaders['soccer']("dummy_url_e0", str(data_paths_config["soccer_data_co_uk"]), "E0_2324.csv")
    # FiveThirtyEight
    mock_downloaders['fte']("dummy_fte_url", str(data_paths_config["fivethirtyeight_data"]))
    # OpenFootball (creates a specific file: eng-england/2022-23/1-premierleague.yml)
    mock_downloaders['openfootball_clone']("https://github.com/openfootball/eng-england.git", str(data_paths_config["openfootball_data"] / "eng-england"))

    # Football-Data.org (Historical) - this is more about mocking the data_collection functions
    # The `mock_data_collection_apis` fixture handles `get_historical_matches_for_competition`
    # and `save_data_to_json`. We need to ensure `save_data_to_json` uses `tmp_path`.
    # The `run_all_importers` calls `import_football_data_org_historical_json` which reads these.
    # The mock_save_json in mock_data_collection_apis is already configured to save under tmp_path.
    # We need to "trigger" the save for the importer to pick it up.
    # This is slightly artificial for E2E, as data_collection.py's main would do this.
    # Here, we ensure the file exists for the importer by calling the mocked save.
    sample_fd_org_hist_data = mock_data_collection_apis['fd_hist']("PL", 2022) # Get data from mock
    mock_data_collection_apis['fd_save_json'](sample_fd_org_hist_data, str(data_paths_config["football_data_org_historical"]), "PL", 2022)


    # --- Phase 2: Run Database Importers ---
    # The `run_all_importers` function in database_importer.py now uses base_data_path
    import_summary = run_all_importers(db_conn, tmp_path) # tmp_path is the base for all data

    # TheSportsDB import is handled differently by run_all_importers (expects E2E to call importers directly)
    # So, we call TheSportsDB importers here directly after mocking data_collection.
    print("\nRunning TheSportsDB importers directly for E2E test...")
    # 1. Search (mocked to return data)
    epl_leagues_tsdb = mock_data_collection_apis['tsdb_search_league']("English Premier League")
    tsdb_leagues_imported_count = 0
    if epl_leagues_tsdb:
        tsdb_leagues_imported_count = database_importer.import_thesportsdb_leagues(db_conn, epl_leagues_tsdb)

    # 2. Get Future Events (mocked) & Import
    tsdb_matches_added_count = 0
    tsdb_matches_updated_count = 0
    epl_id_tsdb = "4328" # Assuming this is found or known
    future_events_tsdb = mock_data_collection_apis['tsdb_future_events'](epl_id_tsdb)
    if future_events_tsdb:
        added, updated = database_importer.import_thesportsdb_events(db_conn, future_events_tsdb, default_league_name_if_missing="English Premier League")
        tsdb_matches_added_count += added
        tsdb_matches_updated_count += updated

    # 3. Get Event Details (mocked) & Re-Import/Update
    if future_events_tsdb and future_events_tsdb[0].get('idEvent'):
        event_detail = mock_data_collection_apis['tsdb_event_details'](future_events_tsdb[0]['idEvent'])
        if event_detail:
            added, updated = database_importer.import_thesportsdb_events(db_conn, [event_detail], default_league_name_if_missing="English Premier League")
            # Avoid double counting if it was already added, just count potential update
            if updated > 0 and added == 0 : tsdb_matches_updated_count += updated
            elif added > 0 : tsdb_matches_added_count += added # If it was somehow not added before

    db_conn.commit() # Commit any pending TSDb transactions

    # Assertions for Importers
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM matches")
    total_matches_in_db = cursor.fetchone()[0]
    print(f"Total matches in DB after all imports: {total_matches_in_db}")
    assert total_matches_in_db > 0 # Check that some matches were inserted from file-based importers
    assert tsdb_matches_added_count > 0 # Check that TSDb matches were added


    # --- Phase 3: Run Data Preprocessing ---
    print("\nRunning data preprocessing (get_ml_ready_dataframe_from_db)...")
    # Filter by a league we know we added dummy data for
    ml_df = get_ml_ready_dataframe_from_db(db_conn, league_names=['Kaggle Cup', 'FTE League', 'FD.org League', 'English Premier League'])

    # Assertions for Preprocessing
    assert not ml_df.empty, "ML-ready DataFrame should not be empty"
    assert 'match_outcome' in ml_df.columns, "Target variable 'match_outcome' should be present"
    assert 'home_form_overall_W' in ml_df.columns, "Form features should be present"

    # Check if some data looks plausible (very basic check)
    # Example: If KaggleTeamA won, its outcome should be 1
    kaggle_match_outcome = ml_df[ml_df['home_team_name'] == 'KaggleTeamA']['match_outcome']
    if not kaggle_match_outcome.empty:
        assert kaggle_match_outcome.iloc[0] == 1.0 # Home win

    # Example: Check if form features have non-default values for some rows
    # (Our dummy mock for engineer_form_features puts 1s, so this should pass if called)
    if not ml_df.empty:
         assert ml_df['home_form_overall_games_played'].sum() > 0 # Some games should have form calculated

    print("\n--- E2E Pipeline Test Workflow Completed ---")
    print(f"Final ML DataFrame shape: {ml_df.shape}")
    ml_df.info()
    print(ml_df.head())

# To run: pytest sports_prediction_ai/tests/e2e/test_full_pipeline.py
