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
