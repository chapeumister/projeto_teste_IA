import pytest
import pandas as pd
from datetime import datetime, timedelta
from sports_prediction_ai.src.data_preprocessing import get_team_form_features, engineer_form_features

# --- Fixtures for historical_matches_df ---
@pytest.fixture
def sample_historical_matches():
    """Provides a sample historical_matches_df DataFrame."""
    data = {
        'match_id': range(1, 11),
        'utcDate': pd.to_datetime([
            "2023-01-01", "2023-01-05", "2023-01-10", "2023-01-15", "2023-01-20", # Team 10 (home/away)
            "2023-01-02", "2023-01-06", "2023-01-11", "2023-01-16", "2023-01-21"  # Team 11 (home/away)
        ]),
        'home_team_id': [10, 11, 10, 12, 10,  11, 10, 12, 11, 13],
        'away_team_id': [11, 10, 12, 10, 13,  10, 11, 13, 10, 10], # Team 10 is away in matches 2, 6, 9, 10
        'home_team_score': [2, 1, 3, 0, 1,  2, 2, 0, 3, 4],
        'away_team_score': [1, 2, 0, 1, 1,  0, 2, 1, 1, 1]
        # Team 10 results (before 2023-01-25, last 5):
        # 2023-01-21: Away vs Team 11 (1-3 L) -> game_id 9 (idx 8)
        # 2023-01-20: Home vs Team 13 (1-1 D) -> game_id 5 (idx 4)
        # 2023-01-15: Away vs Team 12 (1-0 W, as away_team_score > home_team_score is wrong, should be away_score < home_score for loss)
        # This is incorrect. Let's fix scores for team 10's perspective:
        # Match idx 1 (game_id 2): T11 (H) 1 vs T10 (A) 2 -> T10 Win
        # Match idx 3 (game_id 4): T12 (H) 0 vs T10 (A) 1 -> T10 Win
        # Match idx 5 (game_id 6): T11 (H) 2 vs T10 (A) 0 -> T10 Loss
        # Match idx 8 (game_id 9): T11 (H) 3 vs T10 (A) 1 -> T10 Loss
        # Match idx 9 (game_id 10): T13 (H) 4 vs T10 (A) 1 -> T10 Loss
        
        # For Team 10 (before 2023-01-25, last 5):
        # 2023-01-21 (idx 8, game_id 9): T11(H) 3 vs T10(A) 1 -> Loss
        # 2023-01-20 (idx 4, game_id 5): T10(H) 1 vs T13(A) 1 -> Draw
        # 2023-01-15 (idx 3, game_id 4): T12(H) 0 vs T10(A) 1 -> Win
        # 2023-01-10 (idx 2, game_id 3): T10(H) 3 vs T12(A) 0 -> Win
        # 2023-01-05 (idx 1, game_id 2): T11(H) 1 vs T10(A) 2 -> Win
        # Expected for Team 10 (match_date="2023-01-25", num_games=5): 3W, 1D, 1L, 5GP

        # For Team 11 (before 2023-01-25, last 5):
        # 2023-01-21 (idx 8, game_id 9): T11(H) 3 vs T10(A) 1 -> Win
        # 2023-01-16 (idx 7, game_id 8): T12(H) 0 vs T11(A) 1 -> Win (Incorrect interpretation in comment, T11 is away, away_score is 1, home is 0. So T11 wins)
        # This is based on current test data: home_team_score[7]=0, away_team_score[7]=1, away_team_id[7]=11
        # Let's fix scores for team 11 perspective:
        # Match idx 0 (game_id 1): T10(H) 2 vs T11(A) 1 -> T11 Loss
        # Match idx 6 (game_id 7): T10(H) 2 vs T11(A) 2 -> T11 Draw
        # Match idx 7 (game_id 8): T12(H) 0 vs T11(A) 1 -> T11 Win (away_team_score[7] is 1, away_team_id[7] is 11)
        # Expected for Team 11 (match_date="2023-01-25", num_games=5):
        # 2023-01-21 (idx 8): T11(H) 3 vs T10(A) 1 -> W
        # 2023-01-16 (idx 7): T12(H) 0 vs T11(A) 1 -> W
        # 2023-01-11 (idx 6): T10(H) 2 vs T11(A) 2 -> D
        # 2023-01-06 (idx 5): T11(H) 2 vs T10(A) 0 -> W
        # 2023-01-01 (idx 0): T10(H) 2 vs T11(A) 1 -> L
        # Expected for Team 11: 3W, 1D, 1L, 5GP
    }
    df = pd.DataFrame(data)
    # Correcting scores based on comments for Team 10
    df.loc[df.match_id == 2, 'away_team_score'] = 2 # T10 Win
    df.loc[df.match_id == 4, 'away_team_score'] = 1 # T10 Win
    df.loc[df.match_id == 6, 'home_team_score'] = 2 # T10 Loss (T11 scored 2 as home)
    df.loc[df.match_id == 6, 'away_team_score'] = 0 # T10 Loss
    df.loc[df.match_id == 9, 'home_team_score'] = 3 # T10 Loss (T11 scored 3 as home)
    df.loc[df.match_id == 9, 'away_team_score'] = 1 # T10 Loss
    df.loc[df.match_id == 10, 'home_team_score'] = 4 # T10 Loss (T13 scored 4 as home)
    df.loc[df.match_id == 10, 'away_team_score'] = 1 # T10 Loss
    
    # Correcting scores for Team 11
    # Match idx 7 (game_id 8): T12(H) 0 vs T11(A) 1 -> T11 Win (already correct in data)
    return df

@pytest.fixture
def processed_matches_sample():
    """Provides a sample processed_matches_df DataFrame."""
    data = {
        'match_id': [101, 102, 103],
        'utcDate': pd.to_datetime(["2023-01-25", "2023-01-26", "2023-01-27"]),
        'home_team_id': [10, 11, 15], # Team 15 has no history
        'away_team_id': [11, 10, 16], # Team 16 has no history
        'home_team_name': ['Team 10', 'Team 11', 'Team 15'],
        'away_team_name': ['Team 11', 'Team 10', 'Team 16']
    }
    return pd.DataFrame(data)

# --- Tests for get_team_form_features ---

def test_get_team_form_full_history(sample_historical_matches):
    team_id = 10
    match_date = "2023-01-25"
    form = get_team_form_features(team_id, match_date, sample_historical_matches, num_games=5)
    assert form['form_W'] == 3  # Based on corrected logic: W, W, D, L, W (reversed order of processing)
    assert form['form_D'] == 1
    assert form['form_L'] == 1
    assert form['form_games_played'] == 5

def test_get_team_form_partial_history(sample_historical_matches):
    team_id = 10 # Team 10 has 5 games in sample_historical_matches before 2023-01-25
    match_date = "2023-01-25"
    form = get_team_form_features(team_id, match_date, sample_historical_matches, num_games=7) # Ask for 7 games
    assert form['form_games_played'] == 5 # Should only find 5

    # Test with fewer games than available
    form_3_games = get_team_form_features(team_id, match_date, sample_historical_matches, num_games=3)
    # Expected for Team 10 (match_date="2023-01-25", num_games=3, most recent 3):
    # 2023-01-21 (idx 8): Loss
    # 2023-01-20 (idx 4): Draw
    # 2023-01-15 (idx 3): Win
    assert form_3_games['form_W'] == 1
    assert form_3_games['form_D'] == 1
    assert form_3_games['form_L'] == 1
    assert form_3_games['form_games_played'] == 3

def test_get_team_form_no_history(sample_historical_matches):
    team_id = 99 # Non-existent team
    match_date = "2023-01-25"
    form = get_team_form_features(team_id, match_date, sample_historical_matches, num_games=5)
    assert form == {'form_W': 0, 'form_D': 0, 'form_L': 0, 'form_games_played': 0}

def test_get_team_form_date_exclusion(sample_historical_matches):
    team_id = 10
    # Match date is before any of Team 10's games
    match_date = "2022-12-31" 
    form = get_team_form_features(team_id, match_date, sample_historical_matches, num_games=5)
    assert form['form_games_played'] == 0

    # Match date is such that only some games are included
    # Team 10's games: 01-01, 01-05, 01-10, 01-15, 01-20
    match_date_mid = "2023-01-12"
    form_mid = get_team_form_features(team_id, match_date_mid, sample_historical_matches, num_games=5)
    # Expected games before 2023-01-12 for T10:
    # 2023-01-10 (idx 2): T10(H) 3 vs T12(A) 0 -> Win
    # 2023-01-05 (idx 1): T11(H) 1 vs T10(A) 2 -> Win
    # 2023-01-01 (idx 0): T10(H) 2 vs T11(A) 1 -> Win
    assert form_mid['form_W'] == 3 
    assert form_mid['form_D'] == 0
    assert form_mid['form_L'] == 0
    assert form_mid['form_games_played'] == 3


def test_get_team_form_correct_score_interpretation(sample_historical_matches):
    # Team 11, match_date="2023-01-25", num_games=5
    # From fixture comments: 3W, 1D, 1L, 5GP
    form_team11 = get_team_form_features(11, "2023-01-25", sample_historical_matches, num_games=5)
    assert form_team11['form_W'] == 3
    assert form_team11['form_D'] == 1
    assert form_team11['form_L'] == 1
    assert form_team11['form_games_played'] == 5

def test_get_team_form_missing_scores(sample_historical_matches):
    historical_missing_scores = sample_historical_matches.copy()
    # Team 10's most recent game (2023-01-21) will have NaN score
    historical_missing_scores.loc[historical_missing_scores.match_id == 9, 'home_team_score'] = None 
    
    form = get_team_form_features(10, "2023-01-25", historical_missing_scores, num_games=5)
    # One game outcome cannot be determined, so games_played should be 4.
    # Original form for T10 (last 5): W, W, D, L, W. Most recent is L (idx 8 / id 9).
    # If id 9 is removed: W, D, W, W. (2023-01-20 D, 2023-01-15 W, 2023-01-10 W, 2023-01-05 W)
    assert form['form_games_played'] == 4 
    assert form['form_W'] == 3 # W, W, W, D
    assert form['form_D'] == 1
    assert form['form_L'] == 0


def test_get_team_form_invalid_match_date_str(sample_historical_matches, capsys):
    form = get_team_form_features(10, "invalid-date", sample_historical_matches, num_games=5)
    assert form == {'form_W': 0, 'form_D': 0, 'form_L': 0, 'form_games_played': 0}
    captured = capsys.readouterr()
    assert "Warning: Could not parse match_date_str: invalid-date" in captured.out

def test_get_team_form_historical_missing_utcdate_col(sample_historical_matches, capsys):
    historical_no_utcdate = sample_historical_matches.drop(columns=['utcDate'])
    form = get_team_form_features(10, "2023-01-25", historical_no_utcdate, num_games=5)
    assert form == {'form_W': 0, 'form_D': 0, 'form_L': 0, 'form_games_played': 0}
    captured = capsys.readouterr()
    assert "Warning: 'utcDate' column missing in historical_matches_df" in captured.out


# --- Tests for engineer_form_features ---

def test_engineer_form_features_basic(processed_matches_sample, sample_historical_matches):
    result_df = engineer_form_features(processed_matches_sample, sample_historical_matches, num_games=5)
    
    assert 'home_form_W' in result_df.columns
    assert 'away_form_L' in result_df.columns
    assert len(result_df) == len(processed_matches_sample)

    # Check form for Team 10 (home team in first match of processed_matches_sample, date 2023-01-25)
    # Expected for Team 10: 3W, 1D, 1L (from previous tests)
    assert result_df.loc[result_df['home_team_id'] == 10, 'home_form_W'].iloc[0] == 3
    assert result_df.loc[result_df['home_team_id'] == 10, 'home_form_D'].iloc[0] == 1
    assert result_df.loc[result_df['home_team_id'] == 10, 'home_form_L'].iloc[0] == 1
    assert result_df.loc[result_df['home_team_id'] == 10, 'home_form_games_played'].iloc[0] == 5

    # Check form for Team 11 (away team in first match, or home in second)
    # Expected for Team 11 (date 2023-01-25 or 2023-01-26): 3W, 1D, 1L
    assert result_df.loc[result_df['away_team_id'] == 11, 'away_form_W'].iloc[0] == 3
    assert result_df.loc[result_df['home_team_id'] == 11, 'home_form_W'].iloc[0] == 3
    
    # Check for teams with no history (Team 15, Team 16)
    assert result_df.loc[result_df['home_team_id'] == 15, 'home_form_games_played'].iloc[0] == 0
    assert result_df.loc[result_df['away_team_id'] == 16, 'away_form_games_played'].iloc[0] == 0


def test_engineer_form_features_empty_processed_df(sample_historical_matches):
    empty_processed_df = pd.DataFrame(columns=['home_team_id', 'away_team_id', 'utcDate'])
    result_df = engineer_form_features(empty_processed_df, sample_historical_matches)
    assert result_df.empty

def test_engineer_form_features_empty_historical_df(processed_matches_sample):
    empty_historical_df = pd.DataFrame(columns=['home_team_id', 'away_team_id', 'home_team_score', 'away_team_score', 'utcDate'])
    result_df = engineer_form_features(processed_matches_sample, empty_historical_df)
    
    assert 'home_form_W' in result_df.columns
    assert result_df['home_form_games_played'].sum() == 0 # All should be 0
    assert result_df['away_form_W'].sum() == 0

def test_engineer_form_features_missing_required_cols_processed(processed_matches_sample, sample_historical_matches, capsys):
    processed_missing_cols = processed_matches_sample.drop(columns=['home_team_id'])
    result_df = engineer_form_features(processed_missing_cols, sample_historical_matches)
    
    # Check if it adds empty form columns as per current implementation
    assert 'home_form_W' in result_df.columns
    assert result_df['home_form_W'].sum() == 0 # Should be all zeros
    captured = capsys.readouterr()
    assert "Warning: Required column 'home_team_id' not found" in captured.out

def test_engineer_form_features_nat_utcdate_processed(processed_matches_sample, sample_historical_matches, capsys):
    processed_nat_date = processed_matches_sample.copy()
    processed_nat_date.loc[0, 'utcDate'] = pd.NaT
    
    result_df = engineer_form_features(processed_nat_date, sample_historical_matches)
    # For the row with NaT date, form features should be default (0s)
    assert result_df.loc[0, 'home_form_games_played'] == 0
    assert result_df.loc[0, 'away_form_games_played'] == 0
    # Other rows should be processed normally
    if len(processed_nat_date) > 1:
         # e.g. Team 11 (home in 2nd match, date 2023-01-26)
        assert result_df.loc[1, 'home_form_W'] == 3 # Assuming Team 11 has 3W,1D,1L
    
    captured = capsys.readouterr()
    assert "Warning: Match date is NaT for a row. Skipping form calculation for this row." in captured.out

def test_engineer_form_features_historical_df_missing_utcdate(processed_matches_sample, sample_historical_matches, capsys):
    historical_no_utcdate = sample_historical_matches.drop(columns=['utcDate'])
    result_df = engineer_form_features(processed_matches_sample, historical_no_utcdate)
    
    # All form features should be default (0s) because get_team_form_features will return default
    assert result_df['home_form_games_played'].sum() == 0
    assert result_df['away_form_games_played'].sum() == 0
    # capsys check for the warning from get_team_form_features (it's called multiple times)
    # We can check if it appears at least once.
    captured = capsys.readouterr()
    assert "Warning: 'utcDate' column missing in historical_matches_df" in captured.out
