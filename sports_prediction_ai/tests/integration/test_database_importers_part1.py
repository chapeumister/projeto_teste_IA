import pytest
import os
import pandas as pd
import csv # For writing CSV files easily
from pathlib import Path

# Functions to be tested
from sports_prediction_ai.src.database_importer import (
    import_kaggle_international_results,
    import_soccer_data_csv,
    get_or_create_league, # For direct checks if needed, though importers use it
    get_or_create_team    # For direct checks if needed
)
# The conftest.py should provide the in_memory_db fixture

# --- Helper Function to Create Temporary CSV ---
def create_temp_csv(tmp_path: Path, filename: str, headers: list, data_rows: list[list]):
    """Creates a temporary CSV file and returns its path."""
    file_path = tmp_path / filename
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data_rows)
    return str(file_path)

# --- Tests for import_kaggle_international_results ---

KAGGLE_CSV_HEADERS = ['date', 'home_team', 'away_team', 'home_score', 'away_score', 'tournament', 'city', 'country', 'neutral']
KAGGLE_SAMPLE_DATA_ROWS = [
    ['2023-01-01', 'Team Alpha', 'Team Beta', '2', '1', 'World Cup Qual.', 'AlphaCity', 'Alphia', 'FALSE'],
    ['2023-01-05', 'Team Gamma', 'Team Delta', '0', '0', 'Friendly', 'GammaVille', 'Gammaria', 'TRUE'],
    ['2023-01-01', 'Team Alpha', 'Team Beta', '3', '3', 'World Cup Qual.', 'BetaCity', 'Betania', 'TRUE'] # Deliberate partial duplicate for idempotency test
]

def test_kaggle_basic_import(in_memory_db, tmp_path):
    """Test basic import of Kaggle international results."""
    csv_filepath = create_temp_csv(tmp_path, "kaggle_results.csv", KAGGLE_CSV_HEADERS, KAGGLE_SAMPLE_DATA_ROWS)

    conn = in_memory_db # Use the in-memory DB connection

    matches_added, _ = import_kaggle_international_results(conn, csv_filepath)

    assert matches_added == 3 # Expect 3 matches to be added

    cursor = conn.cursor()

    # Verify leagues
    cursor.execute("SELECT COUNT(*) FROM leagues")
    assert cursor.fetchone()[0] == 2 # "World Cup Qual." and "Friendly"
    cursor.execute("SELECT name, sport, country, source FROM leagues WHERE name = ?", ("World Cup Qual.",))
    league1 = cursor.fetchone()
    assert league1 == ("World Cup Qual.", "Football", "Alphia", "Kaggle/martj42_intl_results") # Country from first match of this tournament

    # Verify teams (Alpha, Beta, Gamma, Delta)
    cursor.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 4
    cursor.execute("SELECT name, country, source FROM teams WHERE name = ?", ("Team Alpha",))
    team_alpha = cursor.fetchone()
    assert team_alpha == ("Team Alpha", "Team Alpha", "Kaggle/martj42_intl_results") # Country is team name for this importer

    # Verify matches
    cursor.execute("SELECT COUNT(*) FROM matches")
    assert cursor.fetchone()[0] == 3

    cursor.execute("""
        SELECT ht.name, at.name, m.home_score, m.away_score, l.name, m.source, m.status
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        JOIN leagues l ON m.league_id = l.league_id
        WHERE ht.name = 'Team Alpha' AND at.name = 'Team Beta' AND m.home_score = 2
    """)
    match1_details = cursor.fetchone()
    assert match1_details is not None
    assert match1_details[0] == "Team Alpha"
    assert match1_details[1] == "Team Beta"
    assert match1_details[2] == 2
    assert match1_details[3] == 1
    assert match1_details[4] == "World Cup Qual."
    assert match1_details[5] == "Kaggle/martj42_intl_results"
    assert match1_details[6] == "FINISHED"

def test_kaggle_idempotency(in_memory_db, tmp_path):
    """Test that importing the same Kaggle data twice does not duplicate entries."""
    csv_filepath = create_temp_csv(tmp_path, "kaggle_results_idem.csv", KAGGLE_CSV_HEADERS, KAGGLE_SAMPLE_DATA_ROWS)
    conn = in_memory_db

    # First import
    import_kaggle_international_results(conn, csv_filepath)

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM matches")
    matches_count_after_first = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM leagues")
    leagues_count_after_first = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM teams")
    teams_count_after_first = cursor.fetchone()[0]

    assert matches_count_after_first == 3
    assert leagues_count_after_first == 2
    assert teams_count_after_first == 4

    # Second import
    matches_added_second, _ = import_kaggle_international_results(conn, csv_filepath)
    assert matches_added_second == 0 # No new matches should be added

    cursor.execute("SELECT COUNT(*) FROM matches")
    assert cursor.fetchone()[0] == matches_count_after_first
    cursor.execute("SELECT COUNT(*) FROM leagues")
    assert cursor.fetchone()[0] == leagues_count_after_first
    cursor.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == teams_count_after_first


# --- Tests for import_soccer_data_csv ---

SOCCERDATA_CSV_HEADERS_WITH_ODDS = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'B365H', 'B365D', 'B365A', 'Div']
SOCCERDATA_SAMPLE_ROWS_WITH_ODDS = [
    ['10/08/2023', 'Team X', 'Team Y', '2', '1', '1.5', '3.0', '2.5', 'E0'],
    ['11/08/2023', 'Team P', 'Team Q', '0', '0', '2.0', '3.2', '2.0', 'E0']
]
SOCCERDATA_CSV_HEADERS_NO_ODDS = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'Div']
SOCCERDATA_SAMPLE_ROWS_NO_ODDS = [
    ['12/08/2023', 'Team R', 'Team S', '3', '3', 'E0']
]

TEST_LEAGUE_NAME = "Test Premier League"
TEST_COUNTRY = "Testland"
TEST_SEASON = 2023

def test_soccer_data_basic_import_with_odds(in_memory_db, tmp_path):
    csv_filepath = create_temp_csv(tmp_path, "soccer_data_odds.csv", SOCCERDATA_CSV_HEADERS_WITH_ODDS, SOCCERDATA_SAMPLE_ROWS_WITH_ODDS)
    conn = in_memory_db

    matches_added, odds_added = import_soccer_data_csv(conn, csv_filepath, TEST_LEAGUE_NAME, TEST_COUNTRY, TEST_SEASON)

    assert matches_added == 2
    assert odds_added == 2 # One set of odds for each match

    cursor = conn.cursor()
    # Verify league
    cursor.execute("SELECT league_id, name, country, source FROM leagues WHERE name = ?", (TEST_LEAGUE_NAME,))
    league_db = cursor.fetchone()
    assert league_db is not None
    assert league_db[1] == TEST_LEAGUE_NAME
    assert league_db[2] == TEST_COUNTRY
    assert league_db[3] == "SoccerDataUK"
    league_id_db = league_db[0]

    # Verify teams (X, Y, P, Q)
    cursor.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 4

    # Verify matches
    cursor.execute("SELECT COUNT(*) FROM matches")
    assert cursor.fetchone()[0] == 2

    # Verify odds for the first match (Team X vs Team Y)
    cursor.execute("""
        SELECT m.match_id, ht.name, at.name
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE ht.name = 'Team X'
    """)
    match_x_y = cursor.fetchone()
    assert match_x_y is not None
    match_id_x_y = match_x_y[0]

    cursor.execute("SELECT bookmaker, home_win_odds, draw_odds, away_win_odds, source FROM odds WHERE match_id = ?", (match_id_x_y,))
    odds_details = cursor.fetchone()
    assert odds_details is not None
    assert odds_details[0] == "Bet365" # From B365H etc.
    assert odds_details[1] == 1.5
    assert odds_details[2] == 3.0
    assert odds_details[3] == 2.5
    assert odds_details[4] == "SoccerDataUK"


def test_soccer_data_idempotency(in_memory_db, tmp_path):
    csv_filepath = create_temp_csv(tmp_path, "soccer_data_idem.csv", SOCCERDATA_CSV_HEADERS_WITH_ODDS, SOCCERDATA_SAMPLE_ROWS_WITH_ODDS)
    conn = in_memory_db

    # First import
    import_soccer_data_csv(conn, csv_filepath, TEST_LEAGUE_NAME, TEST_COUNTRY, TEST_SEASON)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM matches")
    matches_count1 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM odds")
    odds_count1 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM leagues")
    leagues_count1 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM teams")
    teams_count1 = cursor.fetchone()[0]

    assert matches_count1 == 2
    assert odds_count1 == 2
    assert leagues_count1 == 1
    assert teams_count1 == 4


    # Second import
    matches_added, odds_added = import_soccer_data_csv(conn, csv_filepath, TEST_LEAGUE_NAME, TEST_COUNTRY, TEST_SEASON)
    assert matches_added == 0 # No new matches
    assert odds_added == 0    # No new odds, as they should already exist

    cursor.execute("SELECT COUNT(*) FROM matches"); assert cursor.fetchone()[0] == matches_count1
    cursor.execute("SELECT COUNT(*) FROM odds"); assert cursor.fetchone()[0] == odds_count1
    cursor.execute("SELECT COUNT(*) FROM leagues"); assert cursor.fetchone()[0] == leagues_count1
    cursor.execute("SELECT COUNT(*) FROM teams"); assert cursor.fetchone()[0] == teams_count1


def test_soccer_data_missing_odds(in_memory_db, tmp_path):
    csv_filepath_no_odds = create_temp_csv(tmp_path, "soccer_data_no_odds.csv", SOCCERDATA_CSV_HEADERS_NO_ODDS, SOCCERDATA_SAMPLE_ROWS_NO_ODDS)
    conn = in_memory_db

    matches_added, odds_added = import_soccer_data_csv(conn, csv_filepath_no_odds, "No Odds League", "Testland", 2024)

    assert matches_added == 1
    assert odds_added == 0 # Crucial: no odds should be added

    cursor = conn.cursor()
    # Verify league and teams are still created
    cursor.execute("SELECT COUNT(*) FROM leagues WHERE name = ?", ("No Odds League",))
    assert cursor.fetchone()[0] == 1
    cursor.execute("SELECT COUNT(*) FROM teams WHERE name IN (?,?)", ("Team R", "Team S"))
    assert cursor.fetchone()[0] == 2

    # Verify match is created
    cursor.execute("""
        SELECT m.match_id FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        WHERE ht.name = 'Team R'
    """)
    match_r_s = cursor.fetchone()
    assert match_r_s is not None
    match_id_r_s = match_r_s[0]

    # Verify no odds for this match
    cursor.execute("SELECT COUNT(*) FROM odds WHERE match_id = ?", (match_id_r_s,))
    assert cursor.fetchone()[0] == 0

# To run: pytest sports_prediction_ai/tests/integration/test_database_importers_part1.py
