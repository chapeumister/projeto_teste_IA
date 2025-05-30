import pytest
import os
import pandas as pd
import csv
import json # For JSON helper
from pathlib import Path
from datetime import datetime

# Functions to be tested
from sports_prediction_ai.src.database_importer import (
    import_fivethirtyeight_spi_matches,
    import_football_data_org_historical_json,
    import_openfootball_league_teams,
    import_thesportsdb_leagues,
    import_thesportsdb_events
)
# The conftest.py should provide the in_memory_db fixture

# --- Helper Functions to Create Temporary Data Files ---
def create_temp_csv(tmp_path: Path, filename: str, headers: list, data_rows: list[list]):
    file_path = tmp_path / filename
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data_rows)
    return str(file_path)

def create_temp_json(tmp_path: Path, filename: str, data_content: dict | list):
    file_path = tmp_path / filename
    with open(file_path, 'w') as jsonfile:
        json.dump(data_content, jsonfile, indent=4)
    return str(file_path)

# --- Tests for import_fivethirtyeight_spi_matches ---
FTE_CSV_HEADERS = ['date','league','team1','team2','score1','score2','xg1','xg2','nsxg1','nsxg2','adj_score1','adj_score2']
FTE_SAMPLE_DATA_ROWS = [
    ['2023-01-15','Test Premier League','TeamP','TeamQ','3','1','2.1','0.8','1.9','0.7','2.8','1.1'],
    ['2023-01-16','Test Premier League','TeamP','TeamR','1','1',None,None,None,None,None,None], # Match with no stats
    ['2023-01-17','Another League','TeamS','TeamT',None,None,'1.5','1.2',None,None,None,None] # Scheduled match
]

def test_fte_basic_import_with_stats(in_memory_db, tmp_path):
    csv_filepath = create_temp_csv(tmp_path, "fte_spi.csv", FTE_CSV_HEADERS, FTE_SAMPLE_DATA_ROWS)
    conn = in_memory_db

    matches_added, _, stats_added = import_fivethirtyeight_spi_matches(conn, csv_filepath)

    assert matches_added == 3
    assert stats_added == 3 # 2 for xg, nsxg, adj_score for first match; 1 for xg for third match

    cursor = conn.cursor()
    # Verify leagues
    cursor.execute("SELECT COUNT(DISTINCT name) FROM leagues")
    assert cursor.fetchone()[0] == 2 # Test Premier League, Another League
    # Verify teams
    cursor.execute("SELECT COUNT(DISTINCT name) FROM teams")
    assert cursor.fetchone()[0] == 5 # P, Q, R, S, T

    # Verify match 1 (TeamP vs TeamQ)
    cursor.execute("""
        SELECT m.match_id, ht.name, at.name, m.home_score, m.away_score, m.status, l.name
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        JOIN leagues l ON m.league_id = l.league_id
        WHERE ht.name = 'TeamP' AND at.name = 'TeamQ'
    """)
    match1 = cursor.fetchone()
    assert match1 is not None
    assert match1[1] == 'TeamP' and match1[3] == 3 and match1[5] == 'FINISHED'
    match1_id = match1[0]

    # Verify stats for match 1
    cursor.execute("SELECT stat_type, value_home, value_away FROM stats WHERE match_id = ?", (match1_id,))
    stats_match1 = cursor.fetchall()
    assert len(stats_match1) == 3
    expected_stats_match1 = {
        'expected_goals': ('2.1', '0.8'),
        'non_shot_expected_goals': ('1.9', '0.7'),
        'adjusted_score': ('2.8', '1.1')
    }
    for stat_row in stats_match1:
        assert stat_row[0] in expected_stats_match1
        assert (stat_row[1], stat_row[2]) == expected_stats_match1[stat_row[0]]

    # Verify match 3 (TeamS vs TeamT - scheduled)
    cursor.execute("SELECT status, home_score, away_score FROM matches WHERE source_match_id LIKE '%TeamS_vs_TeamT'")
    match3 = cursor.fetchone()
    assert match3[0] == 'SCHEDULED' and match3[1] is None and match3[2] is None


def test_fte_idempotency(in_memory_db, tmp_path):
    csv_filepath = create_temp_csv(tmp_path, "fte_spi_idem.csv", FTE_CSV_HEADERS, FTE_SAMPLE_DATA_ROWS)
    conn = in_memory_db

    import_fivethirtyeight_spi_matches(conn, csv_filepath) # First import
    matches_count1 = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    stats_count1 = conn.execute("SELECT COUNT(*) FROM stats").fetchone()[0]

    matches_added, _, stats_added = import_fivethirtyeight_spi_matches(conn, csv_filepath) # Second import
    assert matches_added == 0
    assert stats_added == 0 # Stats should also not be re-added if match exists and stats are same

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == matches_count1
    assert conn.execute("SELECT COUNT(*) FROM stats").fetchone()[0] == stats_count1


# --- Tests for import_football_data_org_historical_json ---
FD_ORG_SAMPLE_JSON_DATA = [
    {"id": 123, "utcDate": "2023-02-20T20:00:00Z", "status": "FINISHED", "competition": {"name": "Test Bundesliga", "code": "TBL"}, "homeTeam": {"id": 789, "name": "TeamR"}, "awayTeam": {"id": 790, "name": "TeamS"}, "score": {"fullTime": {"home": 2, "away": 2}}},
    {"id": 124, "utcDate": "2023-02-21T15:00:00Z", "status": "SCHEDULED", "competition": {"name": "Test Bundesliga", "code": "TBL"}, "homeTeam": {"id": 789, "name": "TeamR"}, "awayTeam": {"id": 791, "name": "TeamU"}, "score": {"fullTime": {"home": None, "away": None}}}
]

def test_fd_org_json_basic_import(in_memory_db, tmp_path):
    json_filepath = create_temp_json(tmp_path, "fd_org_hist.json", FD_ORG_SAMPLE_JSON_DATA)
    conn = in_memory_db

    matches_added, _ = import_football_data_org_historical_json(conn, json_filepath, competition_code_override="TBL_override", season_year_override=2022)

    assert matches_added == 2
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM matches")
    assert cursor.fetchone()[0] == 2
    cursor.execute("SELECT name, source FROM leagues WHERE name = 'Test Bundesliga'")
    league = cursor.fetchone()
    assert league == ("Test Bundesliga", "FootballDataOrg")
    cursor.execute("SELECT COUNT(DISTINCT name) FROM teams")
    assert cursor.fetchone()[0] == 3 # TeamR, TeamS, TeamU

    # Check details of first match
    cursor.execute("SELECT source_match_id, status, home_score, away_score FROM matches WHERE source_match_id = '123'")
    match1 = cursor.fetchone()
    assert match1 == ('123', 'FINISHED', 2, 2)


def test_fd_org_json_idempotency(in_memory_db, tmp_path):
    json_filepath = create_temp_json(tmp_path, "fd_org_hist_idem.json", FD_ORG_SAMPLE_JSON_DATA)
    conn = in_memory_db

    import_football_data_org_historical_json(conn, json_filepath) # First import
    matches_count1 = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    leagues_count1 = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    teams_count1 = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]

    matches_added, _ = import_football_data_org_historical_json(conn, json_filepath) # Second import
    assert matches_added == 0
    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == matches_count1
    assert conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0] == leagues_count1
    assert conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0] == teams_count1


# --- Tests for import_openfootball_league_teams ---
OF_SAMPLE_YAML_DATA = {"name": "Test Championship", "season": "2022/23", "clubs": ["TeamU", "TeamV", {"name": "TeamW", "code": "TW"}]}

def test_openfootball_basic_import(in_memory_db):
    conn = in_memory_db
    leagues_processed, teams_processed = import_openfootball_league_teams(conn, OF_SAMPLE_YAML_DATA, "test-country")

    assert leagues_processed == 1
    assert teams_processed == 3

    cursor = conn.cursor()
    cursor.execute("SELECT name, source, country FROM leagues WHERE name = 'Test Championship 2022/23'")
    league = cursor.fetchone()
    assert league is not None
    assert league[1] == "OpenFootball/test-country"
    # Country inference is basic, may be None or based on repo name like "eng-england"
    # For "test-country", it might be None unless specific logic is added.
    # The current SUT code for `import_openfootball_league_teams` has basic inference.
    # "test-country" does not match any, so country_of will be None.
    assert league[2] is None

    cursor.execute("SELECT COUNT(*) FROM teams WHERE source = 'OpenFootball/test-country'")
    assert cursor.fetchone()[0] == 3
    cursor.execute("SELECT name FROM teams WHERE name = 'TeamW'")
    assert cursor.fetchone() is not None

def test_openfootball_idempotency(in_memory_db):
    conn = in_memory_db
    import_openfootball_league_teams(conn, OF_SAMPLE_YAML_DATA, "test-country") # First
    l_count1 = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    t_count1 = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]

    leagues_processed, teams_processed = import_openfootball_league_teams(conn, OF_SAMPLE_YAML_DATA, "test-country") # Second
    assert leagues_processed == 1 # Reports processed if found or created
    assert teams_processed == 3   # Reports processed if found or created

    assert conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0] == l_count1
    assert conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0] == t_count1


# --- Tests for TheSportsDB Importers ---
TSDB_SAMPLE_LEAGUES_DATA = [{"idLeague": "999", "strLeague": "Test API League", "strSport": "Soccer", "strCountryAlternate": "Testlandia"}]
TSDB_SAMPLE_EVENTS_DATA = [
    {"idEvent": "evt1", "strEvent": "TeamW vs TeamX", "strHomeTeam": "TeamW", "strAwayTeam": "TeamX", "idLeague": "999", "strLeague": "Test API League", "strSport": "Soccer", "dateEvent": "2023-10-01", "strTime": "14:00:00", "intHomeScore": None, "intAwayScore": None, "strStatus": "Scheduled"},
    {"idEvent": "evt2", "strEvent": "TeamY vs TeamZ", "strHomeTeam": "TeamY", "strAwayTeam": "TeamZ", "idLeague": "999", "strLeague": "Test API League", "strSport": "Soccer", "dateEvent": "2023-10-02", "strTime": "18:00:00", "intHomeScore": '1', "intAwayScore": '0', "strStatus": "Match Finished"}
]

def test_thesportsdb_import_leagues(in_memory_db):
    conn = in_memory_db
    processed_count = import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA)
    assert processed_count == 1
    league = conn.execute("SELECT name, sport, country, source FROM leagues WHERE idLeagueApi = '999'", {"idLeagueApi": "999"}).fetchone() # Assuming idLeagueApi is not in schema, search by name
    league = conn.execute("SELECT name, sport, country, source FROM leagues WHERE name = 'Test API League'").fetchone()
    assert league == ("Test API League", "Football", "Testlandia", "TheSportsDB")

def test_thesportsdb_import_new_events(in_memory_db):
    conn = in_memory_db
    # Import league first so events can link to it
    import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA)

    added, updated = import_thesportsdb_events(conn, TSDB_SAMPLE_EVENTS_DATA)
    assert added == 2
    assert updated == 0

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM matches WHERE source = 'TheSportsDB'")
    assert cursor.fetchone()[0] == 2
    cursor.execute("SELECT status, home_score, away_score FROM matches WHERE source_match_id = 'evt1'")
    evt1 = cursor.fetchone()
    assert evt1 == ("SCHEDULED", None, None)
    cursor.execute("SELECT status, home_score, away_score FROM matches WHERE source_match_id = 'evt2'")
    evt2 = cursor.fetchone()
    assert evt2 == ("FINISHED", 1, 0)

def test_thesportsdb_update_existing_event(in_memory_db):
    conn = in_memory_db
    import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA)
    # Initial import (evt1 is scheduled)
    import_thesportsdb_events(conn, [TSDB_SAMPLE_EVENTS_DATA[0]])

    updated_event_data = [{
        "idEvent": "evt1", "strEvent": "TeamW vs TeamX", "strHomeTeam": "TeamW",
        "strAwayTeam": "TeamX", "idLeague": "999", "strLeague": "Test API League",
        "strSport": "Soccer", "dateEvent": "2023-10-01", "strTime": "14:00:00",
        "intHomeScore": '3', "intAwayScore": '3', "strStatus": "Match Finished"
    }]
    added, updated = import_thesportsdb_events(conn, updated_event_data)
    assert added == 0
    assert updated == 1

    cursor = conn.cursor()
    cursor.execute("SELECT status, home_score, away_score FROM matches WHERE source_match_id = 'evt1'")
    evt1_updated = cursor.fetchone()
    assert evt1_updated == ("FINISHED", 3, 3)
    cursor.execute("SELECT COUNT(*) FROM matches WHERE source = 'TheSportsDB'") # Should still be 1 match
    assert cursor.fetchone()[0] == 1


def test_thesportsdb_idempotency_leagues_and_events(in_memory_db):
    conn = in_memory_db
    # Leagues
    import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA)
    l_count1 = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    processed_l = import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA)
    assert processed_l == 1 # Reports processed if found
    assert conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0] == l_count1

    # Events
    # Ensure league is imported first to get a stable league_id for events
    import_thesportsdb_leagues(conn, TSDB_SAMPLE_LEAGUES_DATA) # Ensures league exists
    leagues_count_before_event_import = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    teams_count_before_event_import = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0] # Teams are created by event importer

    import_thesportsdb_events(conn, TSDB_SAMPLE_EVENTS_DATA) # First event import
    matches_count1 = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    teams_count_after_first_event_import = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]


    added, updated = import_thesportsdb_events(conn, TSDB_SAMPLE_EVENTS_DATA) # Second event import
    assert added == 0
    assert updated == 0 # No changes in data, so no updates

    assert conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == matches_count1
    assert conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0] == leagues_count_before_event_import # League count shouldn't change due to event import
    assert conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0] == teams_count_after_first_event_import # Team count established by first event import

# To run: pytest sports_prediction_ai/tests/integration/test_database_importers_part2.py
