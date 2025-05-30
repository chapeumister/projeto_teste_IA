import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
from datetime import datetime, timedelta

# Import functions to be tested
from sports_prediction_ai.src.data_preprocessing import (
    get_team_form_features,
    engineer_form_features
)

# --- Fixtures for Test Data ---

@pytest.fixture
def sample_historical_matches():
    """Provides a sample historical matches DataFrame."""
    data = {
        'utcDate': pd.to_datetime([
            "2023-01-01", "2023-01-05", "2023-01-10", "2023-01-15", "2023-01-20", # Team 1's games
            "2023-01-02", "2023-01-06", "2023-01-11", "2023-01-16", "2023-01-21", # Team 2's games
            "2023-01-25", # A game after the typical match_date used in tests
            "2023-01-03", # Team 3 game (limited history)
            "2023-01-04"  # Game with missing score for Team 1
        ]),
        'home_team_id':    [1,  2,  1,  2,  1,  2,  1,  2,  1,  2,  1,  3, 1],
        'away_team_id':    [2,  1,  2,  1,  2,  1,  3,  1,  3,  1,  2,  1, 4], # Team 4 for missing score game
        'home_team_score': [2,  1,  0,  3,  2,  1,  1,  2,  0,  0,  5,  1, None], # Team 1: W, L, D, W, W (vs Team 2)
        'away_team_score': [1,  2,  0,  1,  1,  2,  1,  0,  1,  0,  0,  1, 1]  # Team 2: L, W, D, L, L (vs Team 1)
                                                                            # Team 1 vs 3 (H): D, Team 1 vs 3 (A): D
                                                                            # Team 3 vs 1 (H): D
                                                                            # Team 1 vs 4 (H): score missing
    }
    return pd.DataFrame(data)

@pytest.fixture
def default_overall_form():
    return {'form_overall_W': 0, 'form_overall_D': 0, 'form_overall_L': 0, 'form_overall_games_played': 0}

@pytest.fixture
def default_spec_venue_form():
    return {'form_spec_venue_W': 0, 'form_spec_venue_D': 0, 'form_spec_venue_L': 0, 'form_spec_venue_games_played': 0}


# --- Tests for get_team_form_features ---

def test_basic_form_calculation(sample_historical_matches):
    # Team 1's last 5 games before 2023-01-22 (excluding game on 2023-01-25 and missing score game)
    # T1 Home vs T2 (W: 2-1) on 2023-01-01
    # T1 Away vs T2 (L: 1-2) on 2023-01-05
    # T1 Home vs T2 (D: 0-0) on 2023-01-10
    # T1 Away vs T2 (W: 3-1) on 2023-01-15 -> This is actually T2(H) vs T1(A), so T1 Win
    # T1 Home vs T2 (W: 2-1) on 2023-01-20
    # Corrected for Team 1:
    # 2023-01-20: T1 (H) vs T2 (A) -> 2-1 (W)
    # 2023-01-15: T2 (H) vs T1 (A) -> 3-1 (L for T1, as T1 is away) -> This was misread. T2 home, T1 away. T2 won 3-1, so T1 Lost.
    # Let's re-evaluate Team 1's record from sample_historical_matches before '2023-01-22'
    # 2023-01-01: T1(H) vs T2(A) -> 2-1 (W)
    # 2023-01-05: T2(H) vs T1(A) -> 1-2 (W for T1)
    # 2023-01-10: T1(H) vs T2(A) -> 0-0 (D)
    # 2023-01-15: T2(H) vs T1(A) -> 3-1 (L for T1)
    # 2023-01-20: T1(H) vs T2(A) -> 2-1 (W)
    # Overall for Team 1 (last 5): 3 Wins, 1 Draw, 1 Loss
    # Game with missing score (2023-01-04) for T1 is skipped.
    # Game vs T3 for T1: 2023-01-06 (H) T1-T3 1-1 (D), 2023-01-11 (A) T3-T1 0-1 (W for T1)
    # Let's re-evaluate Team 1's form before "2023-01-22", num_games=5, considering all opponents.
    # Most recent:
    # 1. 2023-01-21: T2(H) vs T1(A) -> 1-2 (W for T1)
    # 2. 2023-01-20: T1(H) vs T2(A) -> 2-1 (W for T1)
    # 3. 2023-01-16: T1(H) vs T3(A) -> 0-1 (L for T1) This is actually T1(H) 0, T3(A) 1. T1 Lost.
    # 4. 2023-01-15: T2(H) vs T1(A) -> 3-1 (L for T1)
    # 5. 2023-01-11: T2(H) vs T1(A) -> 2-0 (L for T1) This is T2(H) 2, T1(A) 0. T1 Lost.
    # Re-checking sample data to be clear:
    # For team_id=1, before '2023-01-22', sorted by date desc:
    # ('2023-01-21', away, T2(H) 1 vs T1(A) 2 -> W)
    # ('2023-01-20', home, T1(H) 2 vs T2(A) 1 -> W)
    # ('2023-01-16', home, T1(H) 0 vs T3(A) 1 -> L)
    # ('2023-01-15', away, T2(H) 3 vs T1(A) 1 -> L)
    # ('2023-01-11', away, T2(H) 2 vs T1(A) 0 -> L)
    # ('2023-01-10', home, T1(H) 0 vs T2(A) 0 -> D)
    # ('2023-01-06', home, T1(H) 1 vs T3(A) 1 -> D)
    # ('2023-01-05', away, T2(H) 1 vs T1(A) 2 -> W)
    # ('2023-01-03', away, T3(H) 1 vs T1(A) 1 -> D)
    # ('2023-01-01', home, T1(H) 2 vs T2(A) 1 -> W)
    # Game at 2023-01-04 with None score is ignored.
    # For team_id=1, before '2023-01-22', sorted by date desc:
    # ('2023-01-21', away, T2(H) 0 vs T1(A) 0 -> D)
    # ('2023-01-20', home, T1(H) 2 vs T2(A) 1 -> W)
    # ('2023-01-16', home, T1(H) 0 vs T3(A) 1 -> L)
    # ('2023-01-15', away, T2(H) 3 vs T1(A) 1 -> L)
    # ('2023-01-11', away, T2(H) 2 vs T1(A) 0 -> L)
    # Expected: 1W, 1D, 3L, 5GP
    form = get_team_form_features(1, "2023-01-22", sample_historical_matches, num_games=5)
    assert form == {'form_overall_W': 1, 'form_overall_D': 1, 'form_overall_L': 3, 'form_overall_games_played': 5}

def test_fewer_games_than_num_games(sample_historical_matches):
    # Team 3 has only one game before 2023-01-22:
    # 2023-01-03 (idx 11): T3(H) 1 vs T1(A) 1 -> D
    form = get_team_form_features(3, "2023-01-22", sample_historical_matches, num_games=5)
    assert form == {'form_overall_W': 0, 'form_overall_D': 1, 'form_overall_L': 0, 'form_overall_games_played': 1}

def test_no_games_played(sample_historical_matches, default_overall_form):
    form = get_team_form_features(99, "2023-01-22", sample_historical_matches, num_games=5) # Team 99 has no games
    assert form == default_overall_form

def test_date_filtering(sample_historical_matches):
    # Team 1, before 2023-01-15. Games considered:
    # ('2023-01-11', away, T2(H) 2 vs T1(A) 0 -> L)
    # ('2023-01-10', home, T1(H) 0 vs T2(A) 0 -> D)
    # ('2023-01-06', home, T1(H) 1 vs T3(A) 1 -> D)
    # ('2023-01-05', away, T2(H) 1 vs T1(A) 2 -> W)
    # ('2023-01-03', away, T3(H) 1 vs T1(A) 1 -> D)
    # ('2023-01-01', home, T1(H) 2 vs T2(A) 1 -> W)
    # Last 5: L, D, D, W, D. So 1W, 3D, 1L.
    form = get_team_form_features(1, "2023-01-15", sample_historical_matches, num_games=5)
    assert form == {'form_overall_W': 1, 'form_overall_D': 3, 'form_overall_L': 1, 'form_overall_games_played': 5}

def test_specific_venue_home(sample_historical_matches):
    # Team 1, home games, before "2023-01-22", num_games=5
    # ('2023-01-20', home, T1(H) 2 vs T2(A) 1 -> W)
    # ('2023-01-16', home, T1(H) 0 vs T3(A) 1 -> L)
    # ('2023-01-10', home, T1(H) 0 vs T2(A) 0 -> D)
    # ('2023-01-06', home, T1(H) 1 vs T3(A) 1 -> D)
    # ('2023-01-01', home, T1(H) 2 vs T2(A) 1 -> W)
    # Last 5 home games: W, L, D, D, W. So 2W, 2D, 1L.
    form = get_team_form_features(1, "2023-01-22", sample_historical_matches, num_games=5, specific_venue='home')
    assert form == {'form_spec_venue_W': 2, 'form_spec_venue_D': 2, 'form_spec_venue_L': 1, 'form_spec_venue_games_played': 5}

def test_specific_venue_away(sample_historical_matches):
    # Team 1, away games, before "2023-01-22", num_games=5
    # ('2023-01-21', away, T2(H) 1 vs T1(A) 2 -> W)
    # ('2023-01-15', away, T2(H) 3 vs T1(A) 1 -> L)
    # ('2023-01-11', away, T2(H) 2 vs T1(A) 0 -> L)
    # ('2023-01-05', away, T2(H) 1 vs T1(A) 2 -> W)
    # ('2023-01-03', away, T3(H) 1 vs T1(A) 1 -> D)
    # Last 5 away games: W, L, L, W, D. So 2W, 1D, 2L.
    form = get_team_form_features(1, "2023-01-22", sample_historical_matches, num_games=5, specific_venue='away')
    assert form == {'form_spec_venue_W': 2, 'form_spec_venue_D': 1, 'form_spec_venue_L': 2, 'form_spec_venue_games_played': 5}

def test_invalid_inputs_get_team_form(default_overall_form, default_spec_venue_form, sample_historical_matches):
    assert get_team_form_features(1, "2023-01-15", pd.DataFrame(), num_games=5) == default_overall_form
    assert get_team_form_features(None, "2023-01-15", sample_historical_matches, num_games=5) == default_overall_form
    assert get_team_form_features(1, "invalid-date", sample_historical_matches, num_games=5) == default_overall_form

    no_utc_date_df = sample_historical_matches.drop(columns=['utcDate'])
    assert get_team_form_features(1, "2023-01-15", no_utc_date_df, num_games=5) == default_overall_form

    # Test with specific_venue for default return structure
    assert get_team_form_features(1, "2023-01-15", pd.DataFrame(), num_games=5, specific_venue='home') == default_spec_venue_form

def test_games_with_missing_scores(sample_historical_matches):
    # Team 1 has one game with a missing score (2023-01-04, T1 vs T4). This game should be ignored.
    # Recalculate form for Team 1 before 2023-01-22, num_games=10 (to ensure all valid games are included)
    # Valid games for T1: W, W, L, L, L, D, D, W, D, W (10 games)
    # The game on 2023-01-04 T1(H) vs T4(A) where home_team_score is None should be skipped.
    # Total valid games for team 1 before 2023-01-22 is 10.
    # W: 5, D: 3, L: 2
    form = get_team_form_features(1, "2023-01-22", sample_historical_matches, num_games=10) # Increase num_games
    assert form['form_overall_games_played'] == 10 # 11 total games, 1 with missing score.
    assert form == {'form_overall_W': 5, 'form_overall_D': 3, 'form_overall_L': 2, 'form_overall_games_played': 10}


# --- Tests for engineer_form_features ---

@pytest.fixture
def sample_processed_matches():
    """Provides a sample DataFrame of matches to engineer features for."""
    data = {
        'utcDate': pd.to_datetime(["2023-01-22", "2023-01-23"]),
        'home_team_id': [1, 2],
        'away_team_id': [2, 1]
    }
    return pd.DataFrame(data)

def test_engineer_form_features_basic(sample_processed_matches, sample_historical_matches):
    result_df = engineer_form_features(sample_processed_matches.copy(), sample_historical_matches, num_games=5)

    assert 'home_form_overall_W' in result_df.columns
    assert 'away_form_away_D' in result_df.columns # Corrected: specific venue 'away' for away team
    assert len(result_df) == 2

    # Check values for the first match (Team 1 vs Team 2 on 2023-01-22)
    # Team 1 overall form (as calculated in test_basic_form_calculation): 2W, 0D, 3L, 5GP
    # Team 1 home form (as calculated in test_specific_venue_home): 2W, 2D, 1L, 5GP
    # Team 1 away form (as calculated in test_specific_venue_away): 2W, 1D, 2L, 5GP

    # Team 2 overall form before 2023-01-22, last 5:
    # 2023-01-21: T2(H) vs T1(A) -> 1-2 (L)
    # 2023-01-20: T1(H) vs T2(A) -> 2-1 (L)
    # 2023-01-15: T2(H) vs T1(A) -> 3-1 (W)
    # 2023-01-10: T1(H) vs T2(A) -> 0-0 (D)
    # 2023-01-05: T2(H) vs T1(A) -> 1-2 (L)
    # Team 2 Overall: 1W, 1D, 3L, 5GP

    # Team 2 home form before 2023-01-22, last 5:
    # 2023-01-21: T2(H) vs T1(A) -> 1-2 (L)
    # 2023-01-15: T2(H) vs T1(A) -> 3-1 (W)
    # 2023-01-05: T2(H) vs T1(A) -> 1-2 (L)
    # 2023-01-02: T2(H) vs T1(A) -> 1-2 (L) - This is T2(H) vs T1(A) from historical_data. T2(H) 1, T1(A) 2. T2 Lost.
    # Re-eval Team 2 Home Form: L, W, L. Total 3 games. 1W, 0D, 2L.
    # Historical data for T2 Home: (2023-01-21, L), (2023-01-15, W), (2023-01-05, L), (2023-01-02, L). 1W, 3L.

    match1_home_form_overall_W = result_df.loc[0, 'home_form_overall_W']
    assert match1_home_form_overall_W == 2 # Team 1 overall Wins

    match1_away_form_overall_W = result_df.loc[0, 'away_form_overall_W']
    assert match1_away_form_overall_W == 1 # Team 2 overall Wins

    match1_home_form_home_W = result_df.loc[0, 'home_form_home_W']
    assert match1_home_form_home_W == 2 # Team 1 home Wins

    match1_away_form_away_D = result_df.loc[0, 'away_form_away_D'] # Team 2 away Draws
    # Team 2 away games before 2023-01-22:
    # 2023-01-20: T1(H) vs T2(A) -> 2-1 (L)
    # 2023-01-10: T1(H) vs T2(A) -> 0-0 (D)
    # 2023-01-01: T1(H) vs T2(A) -> 2-1 (L)
    # Team 2 Away Form: 0W, 1D, 2L, 3GP
    assert match1_away_form_away_D == 1


def test_engineer_form_empty_processed_df(sample_historical_matches):
    empty_df = pd.DataFrame(columns=['home_team_id', 'away_team_id', 'utcDate'])
    result_df = engineer_form_features(empty_df, sample_historical_matches)
    assert result_df.empty

def test_engineer_form_missing_required_cols(sample_historical_matches):
    # Missing 'home_team_id'
    processed_df = pd.DataFrame({'away_team_id': [1], 'utcDate': [pd.to_datetime("2023-01-15")]})
    result_df = engineer_form_features(processed_df.copy(), sample_historical_matches)
    assert 'home_form_overall_W' in result_df.columns # Should add default columns
    assert result_df['home_form_overall_W'].iloc[0] == 0

def test_engineer_form_match_date_is_nat(sample_historical_matches):
    processed_df = pd.DataFrame({
        'home_team_id': [1],
        'away_team_id': [2],
        'utcDate': [pd.NaT]
    })
    result_df = engineer_form_features(processed_df.copy(), sample_historical_matches)
    assert result_df['home_form_overall_games_played'].iloc[0] == 0
    assert result_df['away_form_home_L'].iloc[0] == 0

def test_engineer_form_with_empty_historical_df(sample_processed_matches):
    empty_hist_df = pd.DataFrame(columns=['home_team_id', 'away_team_id', 'home_team_score', 'away_team_score', 'utcDate'])
    result_df = engineer_form_features(sample_processed_matches.copy(), empty_hist_df)

    # Expect all form features to be 0
    assert result_df['home_form_overall_W'].sum() == 0
    assert result_df['home_form_overall_games_played'].sum() == 0
    assert result_df['away_form_away_L'].sum() == 0 # Corrected: e.g. away team's away form
    assert result_df['away_form_home_games_played'].sum() == 0

# Expected column names for form features generated by engineer_form_features
EXPECTED_FORM_COLUMNS = [
    'home_form_overall_W', 'home_form_overall_D', 'home_form_overall_L', 'home_form_overall_games_played',
    'home_form_home_W', 'home_form_home_D', 'home_form_home_L', 'home_form_home_games_played',
    'home_form_away_W', 'home_form_away_D', 'home_form_away_L', 'home_form_away_games_played',
    'away_form_overall_W', 'away_form_overall_D', 'away_form_overall_L', 'away_form_overall_games_played',
    'away_form_home_W', 'away_form_home_D', 'away_form_home_L', 'away_form_home_games_played',
    'away_form_away_W', 'away_form_away_D', 'away_form_away_L', 'away_form_away_games_played',
]

def test_engineer_form_features_column_names(sample_processed_matches, sample_historical_matches):
    result_df = engineer_form_features(sample_processed_matches.copy(), sample_historical_matches)
    for col in EXPECTED_FORM_COLUMNS:
        assert col in result_df.columns, f"Expected column '{col}' not found in DataFrame."

def test_engineer_form_features_handles_missing_team_ids(sample_historical_matches):
    processed_df = pd.DataFrame({
        'home_team_id': [1, None, 3], # Has a None home_team_id
        'away_team_id': [2, 4, None], # Has a None away_team_id
        'utcDate': pd.to_datetime(["2023-01-22", "2023-01-23", "2023-01-24"])
    })
    result_df = engineer_form_features(processed_df.copy(), sample_historical_matches)

    # For the row with home_team_id=None, home form features should be default (0)
    home_form_row1 = result_df.iloc[1][[col for col in result_df.columns if col.startswith('home_form')]]
    assert all(home_form_row1 == 0)

    # For the row with away_team_id=None, away form features should be default (0)
    away_form_row2 = result_df.iloc[2][[col for col in result_df.columns if col.startswith('away_form')]]
    assert all(away_form_row2 == 0)

    # Check that valid rows still get processed
    assert result_df.loc[0, 'home_form_overall_games_played'] > 0 # Team 1 should have games
    assert result_df.loc[0, 'away_form_overall_games_played'] > 0 # Team 2 should have games
    assert result_df.loc[2, 'home_form_overall_games_played'] > 0 # Team 3 should have some games
    assert result_df.loc[1, 'away_form_overall_games_played'] == 0 # Team 4 has no history in sample data
