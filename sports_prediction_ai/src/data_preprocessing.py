# src/data_preprocessing.py
import pandas as pd

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
        df['home_team_name'] = df['homeTeam'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        df['away_team_name'] = df['awayTeam'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        
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
            "id": 1,
            "utcDate": "2023-01-01T15:00:00Z",
            "status": "FINISHED",
            "homeTeam": {"id": 10, "name": "Team A", "shortName": "TMA", "tla": "TA"},
            "awayTeam": {"id": 11, "name": "Team B", "shortName": "TMB", "tla": "TB"},
            "score": {"winner": "HOME_TEAM", "fullTime": {"home": 2, "away": 1}},
            "competition": {"name": "Sample League"}
        },
        {
            "id": 2,
            "utcDate": "2023-01-01T17:30:00Z",
            "status": "FINISHED",
            "homeTeam": {"id": 12, "name": "Team C", "shortName": "TMC", "tla": "TC"},
            "awayTeam": {"id": 13, "name": "Team D", "shortName": "TMD", "tla": "TD"},
            "score": {"winner": "AWAY_TEAM", "fullTime": {"home": 0, "away": 3}},
            "competition": {"name": "Sample League"}
        },
        { # A match that might not have score details yet (e.g. scheduled)
            "id": 3,
            "utcDate": "2023-01-02T19:00:00Z",
            "status": "SCHEDULED",
            "homeTeam": {"id": 10, "name": "Team A"},
            "awayTeam": {"id": 13, "name": "Team D"},
            "score": {"winner": None, "fullTime": {"home": None, "away": None}},
            "competition": {"name": "Sample League"}
        }
    ]

    processed_df = preprocess_match_data(sample_matches_data)
    if not processed_df.empty:
        print("\nProcessed DataFrame head:")
        print(processed_df[['home_team_name', 'away_team_name', 'home_team_score', 'away_team_score', 'utcDate']].head())
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


def get_team_form_features(team_id: int, match_date_str: str, historical_matches_df: pd.DataFrame, num_games: int = 5) -> dict:
    """
    Calculates the form of a given team based on its last N games before a specific match date.

    Args:
        team_id (int): The ID of the team.
        match_date_str (str): The date of the upcoming match (e.g., "YYYY-MM-DD").
                              Past games will be considered before this date.
        historical_matches_df (pd.DataFrame): DataFrame of past matches. Must include:
                                              'home_team_id', 'away_team_id', 
                                              'home_team_score', 'away_team_score', 
                                              'utcDate' (or a comparable date column).
        num_games (int): The number of recent games to consider for form calculation.

    Returns:
        dict: Form features (e.g., {'form_W': W, 'form_D': D, 'form_L': L, 'form_games_played': N}).
    """
    default_form = {'form_W': 0, 'form_D': 0, 'form_L': 0, 'form_games_played': 0}

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
    team_matches = historical_df_copy[
        ((historical_df_copy['home_team_id'] == team_id) | (historical_df_copy['away_team_id'] == team_id)) &
        (historical_df_copy['utcDate'] < current_match_date) &
        (historical_df_copy['utcDate'].notna())
    ].copy() # Use .copy() to avoid potential SettingWithCopyWarning later

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
        'form_W': wins,
        'form_D': draws,
        'form_L': losses,
        'form_games_played': actual_games_played # Use the count of games where outcome could be determined
    }


def engineer_form_features(processed_matches_df: pd.DataFrame, historical_matches_df: pd.DataFrame, num_games: int = 5) -> pd.DataFrame:
    """
    Engineers team form features (W, D, L for last N games) for matches.

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
            # Add empty form columns to maintain schema consistency if desired, or return df as is
            for prefix in ['home', 'away']:
                processed_matches_df[f'{prefix}_form_W'] = 0
                processed_matches_df[f'{prefix}_form_D'] = 0
                processed_matches_df[f'{prefix}_form_L'] = 0
                processed_matches_df[f'{prefix}_form_games_played'] = 0
            return processed_matches_df
            
    # Ensure 'utcDate' is datetime for proper comparison by get_team_form_features if it's called with string dates from this df
    # However, get_team_form_features itself converts its match_date_str argument.
    # processed_matches_df['utcDate'] = pd.to_datetime(processed_matches_df['utcDate'], errors='coerce')


    home_form_features_list = []
    away_form_features_list = []

    for _, row in processed_matches_df.iterrows():
        match_date = row['utcDate'] # This should be a datetime object or a string that get_team_form_features can parse
        if pd.isna(match_date):
             print(f"Warning: Match date is NaT for a row. Skipping form calculation for this row.")
             home_form_features_list.append(get_team_form_features(None, None, historical_matches_df, num_games))
             away_form_features_list.append(get_team_form_features(None, None, historical_matches_df, num_games))
             continue

        # Convert match_date to "YYYY-MM-DD" string if it's a datetime object, as get_team_form_features expects a string
        match_date_str = match_date.strftime('%Y-%m-%d') if isinstance(match_date, pd.Timestamp) else str(match_date)


        home_team_id = row.get('home_team_id')
        away_team_id = row.get('away_team_id')

        home_form = get_team_form_features(home_team_id, match_date_str, historical_matches_df, num_games)
        away_form = get_team_form_features(away_team_id, match_date_str, historical_matches_df, num_games)
        
        home_form_features_list.append(home_form)
        away_form_features_list.append(away_form)

    # Create DataFrames from the lists of dictionaries
    home_form_df = pd.DataFrame(home_form_features_list).add_prefix('home_')
    away_form_df = pd.DataFrame(away_form_features_list).add_prefix('away_')

    # Concatenate the new form feature DataFrames with the original DataFrame
    # Ensure indices align for concatenation; if processed_matches_df has a custom index, reset it first or align.
    result_df = pd.concat([processed_matches_df.reset_index(drop=True), 
                           home_form_df.reset_index(drop=True), 
                           away_form_df.reset_index(drop=True)], axis=1)
    
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
        print(processed_df[['home_team_name', 'away_team_name', 'home_team_score', 'away_team_score', 'utcDate']].head())

        # Create dummy historical data for form calculation
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
        # (The existing one does not create home_team_id/away_team_id)
        # For this test, let's manually add them to processed_df if they are missing
        if 'home_team_id' not in processed_df.columns and 'homeTeam' in processed_df.columns:
            processed_df['home_team_id'] = processed_df['homeTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        if 'away_team_id' not in processed_df.columns and 'awayTeam' in processed_df.columns:
            processed_df['away_team_id'] = processed_df['awayTeam'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

        # Drop rows where team IDs might be missing after extraction, or if utcDate is NaT
        processed_df_cleaned = processed_df.dropna(subset=['home_team_id', 'away_team_id', 'utcDate']).copy()


        print("\nCleaned Processed DataFrame for Form Engineering (IDs and Dates):")
        print(processed_df_cleaned[['home_team_id', 'away_team_id', 'utcDate']].head())
        
        print("\nHistorical DataFrame head:")
        print(historical_df.head())

        # Engineer form features
        matches_with_form = engineer_form_features(processed_df_cleaned, historical_df, num_games=5)
        
        print("\nMatches DataFrame with Form Features (first 3 matches):")
        form_cols = [col for col in matches_with_form.columns if 'form' in col]
        print(matches_with_form[['home_team_id', 'away_team_id', 'utcDate'] + form_cols].head(3))

        # Example: Test with a team that has fewer than num_games historical matches
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
        print("\nMatch with a team having less than 5 historical games:")
        print(matches_with_form_less_hist[['home_team_id', 'away_team_id', 'utcDate'] + [col for col in matches_with_form_less_hist.columns if 'form' in col]].head())

        # Test with empty historical_df
        print("\nTesting with empty historical_df:")
        empty_historical_df = pd.DataFrame(columns=historical_df.columns)
        matches_with_empty_hist = engineer_form_features(processed_df_cleaned.head(1).copy(), empty_historical_df, num_games=5)
        print(matches_with_empty_hist[['home_team_id', 'away_team_id', 'utcDate'] + [col for col in matches_with_empty_hist.columns if 'form' in col]].head())

    else:
        print("\nNo data processed initially, skipping form feature engineering examples.")

