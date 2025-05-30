# src/data_preprocessing.py
import pandas as pd
import sqlite3
import os
from datetime import datetime

# --- Database Path ---
try:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'sports_data.db')
except NameError: # Fallback for environments where __file__ might not be defined
    print("Warning: __file__ not defined, using current working directory for DB_PATH relative calculations.")
    DB_PATH = os.path.join(os.getcwd(), 'sports_prediction_ai', 'data', 'sports_data.db')

def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        # conn.execute("PRAGMA foreign_keys = ON;") # Foreign keys already handled by database_importer
        print(f"Successfully connected to database at {DB_PATH}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        raise
    return conn

def preprocess_match_data(matches_raw_data: list):
    """
    Preprocesses raw match data obtained from an API.
    For now, it converts the list of matches into a Pandas DataFrame.
    This function will be expanded to include feature engineering and cleaning.

    Args:
        matches_raw_data (list): A list of match dictionaries from the API.

    Returns:
        pd.DataFrame: A DataFrame containing preprocessed match data.
                      Returns an empty DataFrame if input is empty or not a list.
    """
    if not isinstance(matches_raw_data, list) or not matches_raw_data:
        print("Input data is empty or not a list. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(matches_raw_data)

    # Basic data extraction and cleaning (examples)
    # These will be expanded significantly.
    if not df.empty:
        # Example: Extract home team, away team names and scores if available
        # The actual structure depends heavily on the API's response format.
        # This is a generic example assuming football-data.org structure.
        
        # Extract basic match and team identifiers
        df['match_id'] = df['id'] # Assuming 'id' is the match_id at the top level
        df['home_team_id'] = df['homeTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        df['home_team_name'] = df['homeTeam'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        df['away_team_id'] = df['awayTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        df['away_team_name'] = df['awayTeam'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        df['competition_id'] = df['competition'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

        # Extracting scores - checking if 'score' and 'fullTime' exist
        def extract_score(score_data, team_type):
            if isinstance(score_data, dict) and isinstance(score_data.get('fullTime'), dict):
                return score_data['fullTime'].get(team_type)
            return None

        df['home_team_score'] = df['score'].apply(lambda x: extract_score(x, 'home'))
        df['away_team_score'] = df['score'].apply(lambda x: extract_score(x, 'away'))

        # Example: Convert UTC date to datetime objects
        if 'utcDate' in df.columns:
            df['utcDate'] = pd.to_datetime(df['utcDate'], errors='coerce')

        # Drop columns that might be complex objects or not immediately useful
        # This is very preliminary; feature selection will be more sophisticated.
        # df = df.drop(columns=['homeTeam', 'awayTeam', 'score', 'referees'], errors='ignore')
        
        # Placeholder for more advanced feature engineering:
        # - Recent form of teams
        # - Head-to-head statistics
        # - League standings
        # - Player availability (if data allows)
        print(f"Initial preprocessing done. DataFrame shape: {df.shape}")
    
    return df

if __name__ == '__main__':
    # Example Usage (with dummy data similar to football-data.org)
    sample_matches_data = [
        {
            "id": 1001, # match_id
            "utcDate": "2023-01-01T15:00:00Z",
            "status": "FINISHED",
            "homeTeam": {"id": 10, "name": "Team A", "shortName": "TMA", "tla": "TA"},
            "awayTeam": {"id": 11, "name": "Team B", "shortName": "TMB", "tla": "TB"},
            "score": {"winner": "HOME_TEAM", "fullTime": {"home": 2, "away": 1}},
            "competition": {"id": 2021, "name": "Sample League", "code": "SL"}
        },
        {
            "id": 1002, # match_id
            "utcDate": "2023-01-01T17:30:00Z",
            "status": "FINISHED",
            "homeTeam": {"id": 12, "name": "Team C", "shortName": "TMC", "tla": "TC"},
            "awayTeam": {"id": 13, "name": "Team D", "shortName": "TMD", "tla": "TD"},
            "score": {"winner": "AWAY_TEAM", "fullTime": {"home": 0, "away": 3}},
            "competition": {"id": 2021, "name": "Sample League", "code": "SL"}
        },
        { # A match that might not have score details yet (e.g. scheduled) or missing competition id
            "id": 1003, # match_id
            "utcDate": "2023-01-02T19:00:00Z",
            "status": "SCHEDULED",
            "homeTeam": {"id": 10, "name": "Team A"}, # "id" is present
            "awayTeam": {"name": "Team D"}, # "id" is missing
            "score": {"winner": None, "fullTime": {"home": None, "away": None}},
            "competition": {"name": "Another League"} # "id" is missing
        },
        { # A match with missing homeTeam and competition structures
            "id": 1004, # match_id
            "utcDate": "2023-01-03T19:00:00Z",
            "status": "SCHEDULED",
            "homeTeam": None, # Missing homeTeam dict
            "awayTeam": {"id": 15, "name": "Team E"},
            "score": {"winner": None, "fullTime": {"home": None, "away": None}},
            "competition": None # Missing competition dict
        }
    ]

    processed_df = preprocess_match_data(sample_matches_data)
    if not processed_df.empty:
        print("\nProcessed DataFrame head:")
        expected_cols = ['match_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 
                         'competition_id', 'home_team_score', 'away_team_score', 'utcDate']
        # Ensure all expected columns exist before trying to print them
        cols_to_print = [col for col in expected_cols if col in processed_df.columns]
        print(processed_df[cols_to_print].head())
    else:
        print("\nNo data processed or returned.")

    # Test with empty data
    print("\nTesting with empty list:")
    empty_df = preprocess_match_data([])
    print(f"Empty DataFrame columns: {empty_df.columns}, shape: {empty_df.shape}")

    # Test with invalid data
    print("\nTesting with invalid data type:")
    invalid_df = preprocess_match_data("not a list")
    print(f"Invalid DataFrame columns: {invalid_df.columns}, shape: {invalid_df.shape}")


def get_team_form_features(team_id: int, match_date_str: str, historical_matches_df: pd.DataFrame, num_games: int = 5, specific_venue: str = None) -> dict:
    """
    Calculates the form of a given team based on its last N games before a specific match date,
    optionally filtering for home or away games.

    Args:
        team_id (int): The ID of the team.
        match_date_str (str): The date of the upcoming match (e.g., "YYYY-MM-DD").
                              Past games will be considered before this date.
        historical_matches_df (pd.DataFrame): DataFrame of past matches. Must include:
                                              'home_team_id', 'away_team_id',
                                              'home_team_score', 'away_team_score',
                                              'utcDate' (or a comparable date column).
        num_games (int): The number of recent games to consider for form calculation.
        specific_venue (str, optional): If 'home', calculate form for home games only.
                                        If 'away', calculate form for away games only.
                                        If None, calculate overall form. Defaults to None.

    Returns:
        dict: Form features. Keys depend on `specific_venue`:
              - If 'home' or 'away': {'form_spec_venue_W': W, 'form_spec_venue_D': D, ...}
              - If None: {'form_overall_W': W, 'form_overall_D': D, ...}
    """
    if specific_venue is None:
        key_prefix = "form_overall"
    elif specific_venue == 'home' or specific_venue == 'away':
        key_prefix = "form_spec_venue"
    else: # Should not happen with controlled inputs, but as a fallback
        key_prefix = "form_unknown_venue_type"

    default_form = {
        f'{key_prefix}_W': 0, f'{key_prefix}_D': 0, f'{key_prefix}_L': 0,
        f'{key_prefix}_games_played': 0
    }

    if historical_matches_df.empty or team_id is None or match_date_str is None:
        return default_form

    # Ensure match_date is in datetime format for comparison
    try:
        current_match_date = pd.to_datetime(match_date_str)
    except ValueError:
        print(f"Warning: Could not parse match_date_str: {match_date_str}. Skipping form calculation for team {team_id}.")
        return default_form
        
    # Ensure historical dates are also in datetime format
    if 'utcDate' not in historical_matches_df.columns:
        print("Warning: 'utcDate' column missing in historical_matches_df. Skipping form calculation.")
        return default_form
        
    # Make a copy to avoid SettingWithCopyWarning when converting 'utcDate'
    historical_df_copy = historical_matches_df.copy()
    historical_df_copy['utcDate'] = pd.to_datetime(historical_df_copy['utcDate'], errors='coerce')

    # Filter for matches involving the team, before the current match_date, and with valid dates
    base_filter = (
        ((historical_df_copy['home_team_id'] == team_id) | (historical_df_copy['away_team_id'] == team_id)) &
        (historical_df_copy['utcDate'] < current_match_date) &
        (historical_df_copy['utcDate'].notna())
    )

    if specific_venue == 'home':
        venue_filter = (historical_df_copy['home_team_id'] == team_id)
        team_matches = historical_df_copy[base_filter & venue_filter].copy()
    elif specific_venue == 'away':
        venue_filter = (historical_df_copy['away_team_id'] == team_id)
        team_matches = historical_df_copy[base_filter & venue_filter].copy()
    else: # Overall form
        team_matches = historical_df_copy[base_filter].copy()


    if team_matches.empty:
        return default_form

    # Sort by date to get the most recent games
    team_matches_sorted = team_matches.sort_values(by='utcDate', ascending=False)
    recent_games = team_matches_sorted.head(num_games)
    
    actual_games_played = len(recent_games)
    if actual_games_played == 0:
        return default_form

    wins = 0
    draws = 0
    losses = 0

    for _, row in recent_games.iterrows():
        # Scores must be numeric for comparison
        home_score = pd.to_numeric(row.get('home_team_score'), errors='coerce')
        away_score = pd.to_numeric(row.get('away_team_score'), errors='coerce')

        if pd.isna(home_score) or pd.isna(away_score):
            # If scores are not available or not numeric, cannot determine outcome for this match
            actual_games_played -=1 # Decrement because this game outcome is unknown
            continue 

        if row['home_team_id'] == team_id: # Team was home
            if home_score > away_score:
                wins += 1
            elif home_score == away_score:
                draws += 1
            else:
                losses += 1
        elif row['away_team_id'] == team_id: # Team was away
            if away_score > home_score:
                wins += 1
            elif away_score == home_score:
                draws += 1
            else:
                losses += 1
        # If team_id is not in home or away, it's an issue with the initial filter (should not happen)

    return {
        f'{key_prefix}_W': wins,
        f'{key_prefix}_D': draws,
        f'{key_prefix}_L': losses,
        f'{key_prefix}_games_played': actual_games_played
    }


def engineer_form_features(processed_matches_df: pd.DataFrame, historical_matches_df: pd.DataFrame, num_games: int = 5) -> pd.DataFrame:
    """
    Engineers overall, home-specific, and away-specific team form features for matches.

    Args:
        processed_matches_df (pd.DataFrame): DataFrame of current/upcoming matches. 
                                             Requires 'home_team_id', 'away_team_id', 'utcDate'.
        historical_matches_df (pd.DataFrame): DataFrame of past matches. 
                                              Required by get_team_form_features.
        num_games (int): Number of past games to consider for form calculation.

    Returns:
        pd.DataFrame: The processed_matches_df augmented with home and away team form features.
    """
    if processed_matches_df.empty:
        print("Warning: processed_matches_df is empty. Returning it as is.")
        return processed_matches_df

    # Check for required columns in processed_matches_df
    required_cols_processed = ['home_team_id', 'away_team_id', 'utcDate']
    for col in required_cols_processed:
        if col not in processed_matches_df.columns:
            print(f"Warning: Required column '{col}' not found in processed_matches_df. Cannot engineer form features.")
            # Add empty form columns for all expected types to maintain schema consistency
            prefixes = ['home_form_overall_', 'home_form_home_', 'home_form_away_',
                        'away_form_overall_', 'away_form_home_', 'away_form_away_']
            suffixes = ['W', 'D', 'L', 'games_played']
            for p in prefixes:
                for s in suffixes:
                    processed_matches_df[f'{p}{s}'] = 0
            return processed_matches_df

    all_features_list = []

    for _, row in processed_matches_df.iterrows():
        match_date = row['utcDate']
        match_features = {}

        if pd.isna(match_date):
            print(f"Warning: Match date is NaT for a row. Skipping form calculation for this row.")
            # Append default/empty features for this row
            team_prefixes = ['home', 'away']
            venue_types = {'overall': None, 'home': 'home', 'away': 'away'}
            for team_prefix in team_prefixes:
                for venue_key, venue_val in venue_types.items():
                    # Determine the correct key_prefix that get_team_form_features would use
                    internal_key_prefix = "form_overall" if venue_val is None else "form_spec_venue"
                    default_single_form = {
                        f'{internal_key_prefix}_W': 0, f'{internal_key_prefix}_D': 0, f'{internal_key_prefix}_L': 0,
                        f'{internal_key_prefix}_games_played': 0
                    }
                    for k, v in default_single_form.items():
                        # replace 'form_overall' or 'form_spec_venue' with actual desired column prefix part
                        col_name_suffix = k.replace('form_overall_', f'form_{venue_key}_').replace('form_spec_venue_', f'form_{venue_key}_')
                        match_features[f'{team_prefix}_{col_name_suffix}'] = v
            all_features_list.append(match_features)
            continue

        match_date_str = match_date.strftime('%Y-%m-%d') if isinstance(match_date, pd.Timestamp) else str(match_date)
        
        home_team_id = row.get('home_team_id')
        away_team_id = row.get('away_team_id')

        # Home team features
        home_overall_form = get_team_form_features(home_team_id, match_date_str, historical_matches_df, num_games, specific_venue=None)
        home_home_form = get_team_form_features(home_team_id, match_date_str, historical_matches_df, num_games, specific_venue='home')
        home_away_form = get_team_form_features(home_team_id, match_date_str, historical_matches_df, num_games, specific_venue='away')

        # Away team features
        away_overall_form = get_team_form_features(away_team_id, match_date_str, historical_matches_df, num_games, specific_venue=None)
        away_home_form = get_team_form_features(away_team_id, match_date_str, historical_matches_df, num_games, specific_venue='home') # team's performance when they were designated home
        away_away_form = get_team_form_features(away_team_id, match_date_str, historical_matches_df, num_games, specific_venue='away') # team's performance when they were designated away
        
        # Consolidate features for the current match
        # Renaming keys to be more descriptive in the final DataFrame
        for k, v in home_overall_form.items(): match_features[f"home_{k.replace('form_overall_', 'form_overall_')}"] = v
        for k, v in home_home_form.items(): match_features[f"home_{k.replace('form_spec_venue_', 'form_home_')}"] = v
        for k, v in home_away_form.items(): match_features[f"home_{k.replace('form_spec_venue_', 'form_away_')}"] = v
        
        for k, v in away_overall_form.items(): match_features[f"away_{k.replace('form_overall_', 'form_overall_')}"] = v
        for k, v in away_home_form.items(): match_features[f"away_{k.replace('form_spec_venue_', 'form_home_')}"] = v # away team's record when playing at their home
        for k, v in away_away_form.items(): match_features[f"away_{k.replace('form_spec_venue_', 'form_away_')}"] = v # away team's record when playing at other's home

        all_features_list.append(match_features)

    # Create DataFrame from the list of feature dictionaries
    form_features_df = pd.DataFrame(all_features_list)

    # Concatenate with the original DataFrame
    result_df = pd.concat([processed_matches_df.reset_index(drop=True), 
                           form_features_df.reset_index(drop=True)], axis=1)

    # Fill NaN values in form feature columns with 0
    # Identify newly added form columns
    form_cols = [col for col in result_df.columns if 'form_' in col and col not in processed_matches_df.columns]
    if form_cols:
        result_df[form_cols] = result_df[form_cols].fillna(0).astype(int)

    return result_df


if __name__ == '__main__':
    # ... (previous example usage of preprocess_match_data) ...
    sample_matches_data = [
        # ... (keep existing sample_matches_data) ...
        {
            "id": 1, "utcDate": "2023-01-01T15:00:00Z", "status": "FINISHED",
            "homeTeam": {"id": 10, "name": "Team A"}, "awayTeam": {"id": 11, "name": "Team B"},
            "score": {"fullTime": {"home": 2, "away": 1}},
        },
        {
            "id": 2, "utcDate": "2023-01-01T17:30:00Z", "status": "FINISHED",
            "homeTeam": {"id": 12, "name": "Team C"}, "awayTeam": {"id": 13, "name": "Team D"},
            "score": {"fullTime": {"home": 0, "away": 3}},
        },
        {
            "id": 3, "utcDate": "2023-01-02T19:00:00Z", "status": "SCHEDULED",
            "homeTeam": {"id": 10, "name": "Team A"}, "awayTeam": {"id": 13, "name": "Team D"},
            "score": {"fullTime": {"home": None, "away": None}},
        }
    ]
    processed_df = preprocess_match_data(sample_matches_data)
    if not processed_df.empty:
        print("\nProcessed DataFrame head (before form features):")
        # Display relevant columns from the initial processing
        display_cols_initial = ['match_id', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'utcDate']
        cols_to_print_initial = [col for col in display_cols_initial if col in processed_df.columns]
        print(processed_df[cols_to_print_initial].head())


        # --- Test get_team_form_features with specific_venue ---
        print("\n--- Testing get_team_form_features directly ---")
        # Create minimal historical data for direct testing
        sample_hist_data_for_direct_test = {
            'utcDate': pd.to_datetime(['2022-12-01', '2022-12-05', '2022-12-10', '2022-12-15', '2022-12-20']),
            'home_team_id':    [10, 20, 10, 30, 10],
            'away_team_id':    [20, 10, 30, 10, 20],
            'home_team_score': [1,  0,  2,  1,  3], # Team 10: Home Win, Away Loss, Home Win, Away Draw, Home Win
            'away_team_score': [0,  1,  1,  1,  0]
        }
        sample_historical_df_direct = pd.DataFrame(sample_hist_data_for_direct_test)
        test_team_id = 10
        test_match_date = "2023-01-01"

        print(f"\nForm for Team ID {test_team_id} before {test_match_date}:")
        overall_form = get_team_form_features(test_team_id, test_match_date, sample_historical_df_direct, num_games=5, specific_venue=None)
        print(f"  Overall Form: {overall_form}") # Expected: 3W, 1D, 1L, 5GP

        home_form = get_team_form_features(test_team_id, test_match_date, sample_historical_df_direct, num_games=5, specific_venue='home')
        print(f"  Home-Specific Form: {home_form}") # Expected: 3W, 0D, 0L, 3GP

        away_form = get_team_form_features(test_team_id, test_match_date, sample_historical_df_direct, num_games=5, specific_venue='away')
        print(f"  Away-Specific Form: {away_form}") # Expected: 0W, 1D, 1L, 2GP
        
        # --- Test engineer_form_features with detailed output ---
        print("\n--- Testing engineer_form_features ---")
        # Create dummy historical data for engineer_form_features calculation
        # Important: Column names must match what get_team_form_features expects
        # 'home_team_id', 'away_team_id', 'home_team_score', 'away_team_score', 'utcDate'
        historical_data = {
            'match_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112],
            'utcDate': pd.to_datetime([
                "2022-12-01", "2022-12-05", "2022-12-10", "2022-12-15", "2022-12-20", "2022-12-25", # Team 10
                "2022-12-02", "2022-12-06", "2022-12-11", "2022-12-16", "2022-12-21", "2022-12-26"  # Team 11, 12, 13
            ]),
            'home_team_id': [10, 11, 10, 12, 10, 13, 11, 10, 12, 11, 10, 13],
            'away_team_id': [11, 10, 12, 10, 13, 10, 12, 13, 11, 13, 11, 12],
            'home_team_score': [2, 1, 3, 0, 1, 2, 2, 2, 0, 3, 4, 1], # Team 10: W, L, W, W, D, W (recent first for form)
            'away_team_score': [1, 2, 0, 1, 1, 0, 2, 2, 1, 1, 1, 1], # Team 11: L, W, D, L, L, D
                                                                  # Team 12: D, W, L, D
                                                                  # Team 13: W, D, W, L
        }
        historical_df = pd.DataFrame(historical_data)
        
        # For preprocess_match_data to extract IDs, we need 'id' in homeTeam/awayTeam dicts
        # Let's simulate that processed_df got these IDs correctly.
        # In a real scenario, preprocess_match_data should populate these correctly.
        # The sample data for preprocess_match_data already has "id" field in homeTeam/awayTeam
        # So, we need to ensure preprocess_match_data creates 'home_team_id' and 'away_team_id'
        
        # First, let's define a more robust preprocess_match_data that extracts IDs
        # (The existing one does not create home_team_id/away_team_id) - this comment is now outdated as preprocess_match_data handles it.
        # For this test, let's manually add them to processed_df if they are missing
        # if 'home_team_id' not in processed_df.columns and 'homeTeam' in processed_df.columns:
        #     processed_df['home_team_id'] = processed_df['homeTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        # if 'away_team_id' not in processed_df.columns and 'awayTeam' in processed_df.columns:
        #     processed_df['away_team_id'] = processed_df['awayTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

        # Drop rows where team IDs might be missing after extraction, or if utcDate is NaT
        # Note: preprocess_match_data now creates these ID columns.
        # We rely on them being present for form engineering.
        processed_df_cleaned = processed_df.dropna(subset=['home_team_id', 'away_team_id', 'utcDate']).copy()


        print("\nCleaned Processed DataFrame for Form Engineering (IDs and Dates):")
        print(processed_df_cleaned[['home_team_id', 'away_team_id', 'utcDate']].head())
        
        print("\nHistorical DataFrame head:")
        print(historical_df.head())

        # Engineer form features
        matches_with_form = engineer_form_features(processed_df_cleaned, historical_df, num_games=5)
        
        print("\nMatches DataFrame with All Form Features (first 1 match):")
        # Select a subset of columns for concise display, including new form features
        cols_to_display = ['match_id', 'home_team_id', 'away_team_id', 'utcDate']
        # Add some representative form features to display
        form_feature_suffixes_overall = ['form_overall_W', 'form_overall_games_played']
        form_feature_suffixes_home = ['form_home_W', 'form_home_games_played']
        form_feature_suffixes_away = ['form_away_W', 'form_away_games_played']
        
        for team_prefix in ['home_', 'away_']:
            for suffix in form_feature_suffixes_overall: cols_to_display.append(f'{team_prefix}{suffix}')
            for suffix in form_feature_suffixes_home: cols_to_display.append(f'{team_prefix}{suffix}')
            for suffix in form_feature_suffixes_away: cols_to_display.append(f'{team_prefix}{suffix}')
            
        # Ensure columns exist in the dataframe before trying to print
        displayable_cols = [col for col in cols_to_display if col in matches_with_form.columns]
        print(matches_with_form[displayable_cols].head(1))


        # Example: Test with a team that has fewer than num_games historical matches
        # This test remains relevant to see how form_xxx_games_played reflects actual games found
        upcoming_match_less_history = pd.DataFrame({
            'match_id': [4],
            'utcDate': [pd.to_datetime("2023-01-03")],
            'home_team_id': [99], # Team with little history
            'away_team_id': [10]  # Team with history
        })
        
        historical_data_less = {
            'match_id': [201, 202],
            'utcDate': pd.to_datetime(["2022-12-01", "2022-12-05"]),
            'home_team_id': [99, 10], 'away_team_id': [10, 99],
            'home_team_score': [1, 0], 'away_team_score': [1, 0] # Team 99: D, L
        }
        historical_df_less = pd.DataFrame(historical_data_less)
        combined_historical_df = pd.concat([historical_df, historical_df_less], ignore_index=True)

        matches_with_form_less_hist = engineer_form_features(upcoming_match_less_history, combined_historical_df, num_games=5)
        print("\nMatch with a team having less than 5 historical games (displaying selected form features):")
        displayable_cols_less_hist = [col for col in cols_to_display if col in matches_with_form_less_hist.columns]
        print(matches_with_form_less_hist[displayable_cols_less_hist].head())

        # Test with empty historical_df
        print("\nTesting with empty historical_df (displaying selected form features):")
        empty_historical_df = pd.DataFrame(columns=historical_df.columns) # Ensure it has same columns for consistency if any part relies on them
        matches_with_empty_hist = engineer_form_features(processed_df_cleaned.head(1).copy(), empty_historical_df, num_games=5)
        displayable_cols_empty_hist = [col for col in cols_to_display if col in matches_with_empty_hist.columns]
        print(matches_with_empty_hist[displayable_cols_empty_hist].head())

    else:
        print("\nNo data processed initially, skipping form feature engineering examples.")


# --- New DB-centric Data Fetching and Preprocessing ---

def get_ml_ready_dataframe_from_db(conn, league_names: list = None, date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """
    Fetches and preprocesses match data from the SQLite database to create an ML-ready DataFrame.

    Args:
        conn: Active SQLite database connection.
        league_names (list, optional): List of league names to filter by.
        date_from (str, optional): Start date for filtering matches (YYYY-MM-DD).
        date_to (str, optional): End date for filtering matches (YYYY-MM-DD).

    Returns:
        pd.DataFrame: A DataFrame ready for machine learning, including engineered features.
    """
    print("\n--- Starting ML-Ready DataFrame Creation from DB ---")

    query = """
    SELECT
        m.match_id,
        m.datetime,
        m.home_team_id,
        ht.name as home_team_name,
        m.away_team_id,
        at.name as away_team_name,
        l.league_id AS league_id, -- Ensuring league_id comes from leagues table and is named 'league_id'
        l.name as league_name,
        m.home_score,
        m.away_score,
        m.status,
        m.source_match_id,
        m.source
    FROM matches m
    JOIN leagues l ON m.league_id = l.league_id
    JOIN teams ht ON m.home_team_id = ht.team_id
    JOIN teams at ON m.away_team_id = at.team_id
    WHERE 1=1
    """
    params = []

    if league_names and isinstance(league_names, list) and len(league_names) > 0:
        placeholders = ','.join('?' for _ in league_names)
        query += f" AND l.name IN ({placeholders})"
        params.extend(league_names)

    if date_from:
        query += " AND DATE(m.datetime) >= DATE(?)" # Ensure datetime is compared as date
        params.append(date_from)

    if date_to:
        query += " AND DATE(m.datetime) <= DATE(?)"
        params.append(date_to)

    query += " ORDER BY m.datetime ASC" # Important for consistent processing if any time-sensitive features are added later

    print(f"Executing query for matches with params: {params}")
    try:
        matches_df = pd.read_sql_query(query, conn, params=params)
    except pd.io.sql.DatabaseError as e:
        print(f"Database query error: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        return pd.DataFrame() # Return empty if query fails

    if matches_df.empty:
        print("No matches found for the given criteria.")
        return pd.DataFrame()

    print(f"Fetched {len(matches_df)} matches from the database.")

    # Convert datetime column to pandas datetime objects
    matches_df['datetime'] = pd.to_datetime(matches_df['datetime'])
    # For form feature engineering, we need a column named 'utcDate'.
    # We'll use the 'datetime' column from DB and rename it for compatibility.
    matches_df.rename(columns={'datetime': 'utcDate'}, inplace=True)


    # --- Feature Engineering: Form ---
    print("Fetching historical matches for form calculation...")
    # Query all finished matches from the DB to serve as historical data
    # This is a simplified approach; for very large datasets, more optimized queries might be needed.
    historical_query = """
    SELECT
        m.match_id,
        m.datetime as utcDate, -- Rename for compatibility with get_team_form_features
        m.home_team_id,
        m.away_team_id,
        m.home_score as home_team_score, -- Rename for compatibility
        m.away_score as away_team_score  -- Rename for compatibility
    FROM matches m
    WHERE m.status = 'FINISHED' AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
    ORDER BY m.datetime DESC
    """ # Order by desc is good for get_team_form_features if it takes .head()

    historical_matches_df_from_db = pd.read_sql_query(historical_query, conn)

    if historical_matches_df_from_db.empty:
        print("Warning: No historical match data found in the database. Form features cannot be calculated.")
        # Add empty form columns if historical data is missing to maintain schema
        prefixes = ['home_form_overall_', 'home_form_home_', 'home_form_away_',
                    'away_form_overall_', 'away_form_home_', 'away_form_away_']
        suffixes = ['W', 'D', 'L', 'games_played']
        for p in prefixes:
            for s in suffixes:
                matches_df[f'{p}{s}'] = 0
    else:
        print(f"Fetched {len(historical_matches_df_from_db)} historical matches for form calculation.")
        # Ensure 'utcDate' is datetime for historical data as well
        historical_matches_df_from_db['utcDate'] = pd.to_datetime(historical_matches_df_from_db['utcDate'])

        # Engineer form features
        # The `engineer_form_features` function expects `utcDate`, `home_team_score`, `away_team_score`
        # in the historical_matches_df. Our query already named them appropriately.
        print("Engineering form features...")
        matches_df = engineer_form_features(matches_df, historical_matches_df_from_db, num_games=5)

    # --- Create Target Variable: match_outcome ---
    # 1 for home win, 0 for draw, -1 for away win
    # Only for 'FINISHED' matches with valid scores
    def calculate_outcome(row):
        if row['status'] == 'FINISHED' and pd.notna(row['home_score']) and pd.notna(row['away_score']):
            if row['home_score'] > row['away_score']:
                return 1  # Home win
            elif row['home_score'] < row['away_score']:
                return -1 # Away win
            else:
                return 0  # Draw
        return None  # For non-finished or matches with missing scores

    matches_df['match_outcome'] = matches_df.apply(calculate_outcome, axis=1)

    print("ML-ready DataFrame created with basic features and form.")

    # TODO: Add Odds and Stats feature integration here if needed later.
    # TODO: Select final set of features for ML. For now, returning all engineered features.

    return matches_df


if __name__ == '__main__':
    # ... (previous example usage of preprocess_match_data and form features) ...
    # Keep the old examples if they are useful for unit testing individual functions.
    # Add new example for get_ml_ready_dataframe_from_db:

    print("\n\n--- Testing get_ml_ready_dataframe_from_db ---")
    conn = None
    try:
        # Create a dummy database for testing if DB_PATH doesn't exist or is empty
        # This is primarily for making the script runnable in isolation if the main DB isn't populated.
        # In a real workflow, the DB would already exist and be populated.
        if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0:
            print(f"Test DB at {DB_PATH} is missing or empty. Creating a dummy DB for this test run.")
            conn_setup = sqlite3.connect(DB_PATH)
            cursor_setup = conn_setup.cursor()
            # Create schema (simplified version of database_setup.py for this test)
            cursor_setup.execute("CREATE TABLE IF NOT EXISTS leagues (league_id INTEGER PRIMARY KEY, name TEXT UNIQUE, sport TEXT, country TEXT, source TEXT)")
            cursor_setup.execute("CREATE TABLE IF NOT EXISTS teams (team_id INTEGER PRIMARY KEY, name TEXT UNIQUE, short_name TEXT, country TEXT, league_id INTEGER, source TEXT, FOREIGN KEY (league_id) REFERENCES leagues(league_id))")
            cursor_setup.execute("CREATE TABLE IF NOT EXISTS matches (match_id INTEGER PRIMARY KEY, datetime TEXT, home_team_id INTEGER, away_team_id INTEGER, league_id INTEGER, status TEXT, home_score INTEGER, away_score INTEGER, source TEXT, source_match_id TEXT, FOREIGN KEY (home_team_id) REFERENCES teams(team_id), FOREIGN KEY (away_team_id) REFERENCES teams(team_id), FOREIGN KEY (league_id) REFERENCES leagues(league_id))")

            # Insert some dummy data
            cursor_setup.execute("INSERT OR IGNORE INTO leagues (league_id, name, sport, country) VALUES (1, 'Premier League', 'Football', 'England')")
            cursor_setup.execute("INSERT OR IGNORE INTO teams (team_id, name, league_id) VALUES (100, 'Arsenal', 1), (101, 'Chelsea', 1), (102, 'Liverpool', 1)")

            # Dummy historical matches for form calculation
            dummy_matches_for_form = [
                (datetime(2022, 12, 1, 15, 0, 0).strftime('%Y-%m-%d %H:%M:%S'), 100, 101, 1, 'FINISHED', 2, 1, 'Test'), # Arsenal beat Chelsea
                (datetime(2022, 12, 5, 15, 0, 0).strftime('%Y-%m-%d %H:%M:%S'), 101, 102, 1, 'FINISHED', 0, 0, 'Test'), # Chelsea draw Liverpool
                (datetime(2022, 12, 10, 15, 0, 0).strftime('%Y-%m-%d %H:%M:%S'), 102, 100, 1, 'FINISHED', 1, 3, 'Test'), # Liverpool lose to Arsenal
            ]
            for m_data in dummy_matches_for_form: cursor_setup.execute("INSERT INTO matches (datetime, home_team_id, away_team_id, league_id, status, home_score, away_score, source) VALUES (?,?,?,?,?,?,?,?)", m_data)

            # Dummy matches to be processed (the ones we want to predict for or analyze)
            dummy_matches_to_process = [
                (datetime(2023, 1, 1, 15, 0, 0).strftime('%Y-%m-%d %H:%M:%S'), 100, 101, 1, 'SCHEDULED', None, None, 'TestFuture'), # Arsenal vs Chelsea (future)
                (datetime(2023, 1, 2, 15, 0, 0).strftime('%Y-%m-%d %H:%M:%S'), 101, 102, 1, 'FINISHED', 2, 2, 'TestFuture'),   # Chelsea vs Liverpool (recent actual)
            ]
            for m_data in dummy_matches_to_process: cursor_setup.execute("INSERT INTO matches (datetime, home_team_id, away_team_id, league_id, status, home_score, away_score, source) VALUES (?,?,?,?,?,?,?,?)", m_data)

            conn_setup.commit()
            conn_setup.close()
            print("Dummy DB created and populated for testing.")

        conn = get_db_connection()

        # Example: Get data for 'Premier League' within a specific date range
        ml_df = get_ml_ready_dataframe_from_db(conn,
                                               league_names=['Premier League'],
                                               date_from='2023-01-01',
                                               date_to='2023-01-31')

        if not ml_df.empty:
            print("\nML-Ready DataFrame Info:")
            ml_df.info()
            print("\nML-Ready DataFrame Head:")
            # Print all columns to see form features
            pd.set_option('display.max_columns', None)
            print(ml_df.head())
        else:
            print("No ML-ready data generated from DB.")

    except Exception as e:
        print(f"An error occurred in the main example: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
