# src/prediction_pipeline.py
import os
import pandas as pd
from datetime import datetime

# Assuming src is in PYTHONPATH or the script is run from the project root.
# Adjust imports if necessary based on your execution environment.
try:
    from src.data_collection import get_matches_for_date
    from src.data_preprocessing import preprocess_match_data
    from src.model_training import load_model
except ImportError:
    # Fallback for cases where the script might be run directly and src is not in sys.path
    # This is common in simple scripts but less so in structured projects.
    print("Attempting relative imports for src modules...")
    from data_collection import get_matches_for_date
    from data_preprocessing import preprocess_match_data, engineer_form_features # Updated import
    from model_training import load_model


# Configuration
DEFAULT_MODEL_FILENAME = "random_forest_model.pkl" # Example: use Random Forest by default
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")
HISTORICAL_DATA_CSV = os.path.join(os.path.dirname(__file__), '..', 'data', 'historical_matches_sample.csv')


def generate_features_for_prediction(daily_matches_df: pd.DataFrame, historical_matches_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates features for prediction, primarily by adding team form features.

    Args:
        daily_matches_df (pd.DataFrame): DataFrame of upcoming matches, preprocessed by `preprocess_match_data`.
                                         Must contain 'home_team_id', 'away_team_id', and 'utcDate'.
        historical_matches_df (pd.DataFrame): DataFrame of past matches for form calculation.

    Returns:
        pd.DataFrame: The daily_matches_df augmented with form features.
                      Returns an empty DataFrame if input is empty or required columns are missing.
    """
    if daily_matches_df.empty:
        print("generate_features_for_prediction: daily_matches_df is empty. Returning empty DataFrame.")
        return pd.DataFrame()

    required_cols = ['home_team_id', 'away_team_id', 'utcDate']
    if not all(col in daily_matches_df.columns for col in required_cols):
        print(f"generate_features_for_prediction: daily_matches_df is missing one or more required columns: {required_cols}. Current columns: {daily_matches_df.columns.tolist()}")
        # Return daily_matches_df as is, or an empty one, or add empty form columns for schema consistency
        return daily_matches_df # Or pd.DataFrame() if strict failure is preferred

    print(f"Generating form features for {len(daily_matches_df)} matches...")
    matches_with_form = engineer_form_features(daily_matches_df, historical_matches_df, num_games=5)
    
    print(f"Finished generating form features. Resulting columns: {matches_with_form.columns.tolist()}")
    return matches_with_form


def predict_daily_matches(date_str: str, model_filename: str = DEFAULT_MODEL_FILENAME, api_key: str = FOOTBALL_DATA_API_KEY, source_api: str = 'football-data'):
    """
    Full pipeline: Fetches matches, preprocesses, generates features, predicts, and displays results.

    Args:
        date_str (str): Date for which to predict matches ("YYYY-MM-DD").
        model_filename (str): Filename of the pre-trained model to load.
        api_key (str): API key for the data source.
    """
    print(f"Starting prediction pipeline for date: {date_str} using model: {model_filename} and API: {source_api}")

    # Validate API key before proceeding, especially for football-data
    if source_api == 'football-data':
        if not api_key or api_key == "YOUR_API_TOKEN":
            print("Error: Invalid or missing API key in prediction_pipeline.py. Please set the FOOTBALL_DATA_API_KEY environment variable.")
            return

    # 1. Fetch matches for the day
    # TODO: Implement logic for different source_apis if needed for get_matches_for_date
    print("\nStep 1: Fetching daily matches...")
    if source_api == 'football-data':
        raw_matches = get_matches_for_date(date_str, api_key=api_key)
    # elif source_api == 'apisports': # Example if you had another source
        # raw_matches = get_matches_from_apisports(date_str, api_key=APISPORTS_API_KEY_VAR) 
    else:
        # For unsupported source_api, if it defaults to football-data, key check is still relevant
        # However, the original logic defaults to football-data without an explicit check here.
        # Let's assume if source_api is not 'football-data', it might be a different API
        # or the get_matches_for_date function (or its alternatives) handles its own key.
        # For now, the explicit key check is only for 'football-data' as per instructions.
        print(f"Unsupported source_api: {source_api}. Defaulting to football-data.org if key is available.")
        # If we default to football-data here, the key check above should ideally cover it.
        # To be safe, if it's truly football-data, the key must be valid.
        # This part of the logic might need refinement if multiple API sources are fully implemented.
        raw_matches = get_matches_for_date(date_str, api_key=api_key) # Assumes get_matches_for_date handles its key if not football-data

    if not raw_matches:
        print("No matches found or API error for daily matches. Exiting pipeline.")
        return
    print(f"Fetched {len(raw_matches)} raw daily match entries.")

    # 2. Preprocess match data (basic preprocessing)
    print("\nStep 2: Preprocessing daily match data...")
    daily_matches_processed_df = preprocess_match_data(raw_matches)
    if daily_matches_processed_df.empty:
        print("No match data to process after initial preprocessing. Exiting pipeline.")
        return
    
    # Ensure 'home_team_id' and 'away_team_id' are present after preprocessing.
    # preprocess_match_data should handle this. If not, they need to be added/mapped here.
    # Also ensure 'utcDate' is present and correctly formatted (pd.to_datetime).
    if 'home_team_id' not in daily_matches_processed_df.columns or 'away_team_id' not in daily_matches_processed_df.columns:
        print("Error: 'home_team_id' or 'away_team_id' not found in preprocessed daily matches. These are required for form feature engineering.")
        # Manually add them if they are in homeTeam/awayTeam dicts (example)
        if 'homeTeam' in daily_matches_processed_df.columns and 'id' in daily_matches_processed_df['homeTeam'].iloc[0]:
             daily_matches_processed_df['home_team_id'] = daily_matches_processed_df['homeTeam'].apply(lambda x: x.get('id'))
        if 'awayTeam' in daily_matches_processed_df.columns and 'id' in daily_matches_processed_df['awayTeam'].iloc[0]:
            daily_matches_processed_df['away_team_id'] = daily_matches_processed_df['awayTeam'].apply(lambda x: x.get('id'))
        
        if 'home_team_id' not in daily_matches_processed_df.columns or 'away_team_id' not in daily_matches_processed_df.columns:
             print("Could not derive team IDs. Exiting.")
             return

    if 'utcDate' not in daily_matches_processed_df.columns:
        print("Error: 'utcDate' not found in preprocessed daily matches. Required for form features.")
        return
    daily_matches_processed_df['utcDate'] = pd.to_datetime(daily_matches_processed_df['utcDate'], errors='coerce')
    daily_matches_processed_df.dropna(subset=['utcDate', 'home_team_id', 'away_team_id'], inplace=True)


    print(f"Preprocessed {len(daily_matches_processed_df)} daily matches into DataFrame. Columns: {daily_matches_processed_df.columns.tolist()}")
    
    # Keep essential info for display later
    display_info = daily_matches_processed_df[['home_team_name', 'away_team_name']].copy()

    # 3. Load/Fetch Historical Data
    print("\nStep 3: Loading historical match data...")
    historical_matches_df = pd.DataFrame()
    if os.path.exists(HISTORICAL_DATA_CSV):
        try:
            historical_matches_df = pd.read_csv(HISTORICAL_DATA_CSV)
            # Ensure required columns for get_team_form_features are present
            # 'home_team_id', 'away_team_id', 'home_team_score', 'away_team_score', 'utcDate'
            historical_matches_df['utcDate'] = pd.to_datetime(historical_matches_df['utcDate'], errors='coerce')
            # Basic validation
            required_hist_cols = ['home_team_id', 'away_team_id', 'home_team_score', 'away_team_score', 'utcDate']
            if not all(col in historical_matches_df.columns for col in required_hist_cols):
                print(f"Warning: Historical data CSV is missing one or more required columns: {required_hist_cols}. Proceeding without it or with dummy data.")
                historical_matches_df = pd.DataFrame() # Reset if not valid
            else:
                print(f"Loaded {len(historical_matches_df)} historical matches from {HISTORICAL_DATA_CSV}.")
        except Exception as e:
            print(f"Error loading historical data from {HISTORICAL_DATA_CSV}: {e}. Proceeding with empty historical data.")
            historical_matches_df = pd.DataFrame()
    else:
        print(f"Historical data CSV not found at {HISTORICAL_DATA_CSV}. Using empty historical data.")
        # NOTE: For a robust pipeline, fetching live historical data or a more reliable source is needed.
        # Creating a minimal dummy historical set if CSV is missing (as a fallback for pipeline to run)
        # This is NOT a substitute for a proper historical data source.
        historical_matches_df = pd.DataFrame({
            'match_id': [1,2,3,4,5,6],
            'utcDate': pd.to_datetime(['2022-01-01', '2022-01-05', '2022-01-10', '2022-01-15', '2022-01-20', '2022-01-25']),
            'home_team_id': [10, 12, 10, 11, 13, 10],
            'away_team_id': [11, 13, 12, 10, 10, 11],
            'home_team_score': [1, 0, 2, 2, 3, 1],
            'away_team_score': [0, 0, 1, 2, 1, 1]
        })
        print("Created minimal dummy historical data as no CSV was found.")


    # 4. Generate features for prediction (including form features)
    print("\nStep 4: Generating features for prediction...")
    # `generate_features_for_prediction` now uses `engineer_form_features`
    features_df = generate_features_for_prediction(daily_matches_processed_df, historical_matches_df)
    
    if features_df.empty:
        print("Could not generate features for the matches. Exiting pipeline.")
        return

    # For consistency with model_training.py, add dummy 'feature1', 'feature2', 'feature3'
    # if they are not already produced by engineer_form_features or preprocess_match_data
    # This is a temporary step to align with the current model_training.py example.
    # Ideally, feature generation should be harmonized.
    if 'feature1' not in features_df.columns:
        features_df['feature1'] = features_df.index * 0.5 
    if 'feature2' not in features_df.columns:
        features_df['feature2'] = 1.0 
    if 'feature3' not in features_df.columns:
        features_df['feature3'] = (features_df.index % 3)
    
    print(f"Features for prediction generated. Columns: {features_df.columns.tolist()}")

    # Define expected features based on model_training.py's dummy data
    expected_features = [
        'feature1', 'feature2', 'feature3',
        'home_form_W', 'home_form_D', 'home_form_L', 'home_form_games_played',
        'away_form_W', 'away_form_D', 'away_form_L', 'away_form_games_played'
    ]
    
    # Select only the expected features for the model
    # Also, ensure they are in the correct order if the model is sensitive to it (though most sklearn models are not by name)
    missing_features = [f for f in expected_features if f not in features_df.columns]
    if missing_features:
        print(f"Error: The final feature set is missing expected features: {missing_features}")
        print("Ensure preprocess_match_data and engineer_form_features produce these, or adjust expected_features.")
        print(f"Available features: {features_df.columns.tolist()}")
        return
        
    features_for_model = features_df[expected_features]


    # 5. Load the pre-trained model
    print("\nStep 5: Loading pre-trained model...")
    model = load_model(model_filename)
    if model is None:
        print(f"Failed to load model '{model_filename}'. Ensure it's trained and available in the 'models' directory.")
        print("You might need to run the model_training.py script first to create a dummy model.")
        return

    # 6. Make predictions
    print("\nStep 6: Making predictions...")
    try:
        # Use features_for_model which has the selected and ordered features
        probabilities = model.predict_proba(features_for_model)
    except Exception as e:
        print(f"Error during model prediction: {e}")
        print(f"Features passed to model: {features_for_model.columns.tolist()}")
        print("Ensure the features used for prediction are consistent with model training (e.g. dtypes, number of features).")
        return
        
    # 7. Display results
    print("\nStep 7: Prediction Results:")
    # Assuming classes are [Home Win, Draw, Away Win] - this depends on model training.
    # The dummy model in model_training.py has classes [0, 1, 2].
    # Let's map them: 0=Home, 1=Draw, 2=Away (this is an assumption!)
    class_labels = getattr(model, 'classes_', None)
    if class_labels is None:
        print("Warning: model.classes_ not found. Assuming classes [0, 1, 2] mapped to [Home, Draw, Away]")
        class_mapping = {0: "Home Win", 1: "Draw", 2: "Away Win"} # Default mapping
    else:
        # Define based on how 'target' was encoded during training.
        # For the dummy data in model_training.py, target is [0, 1, 2].
        class_mapping = {
            0: "Home Win",  # Assuming class 0 from training is Home Win
            1: "Draw",      # Assuming class 1 from training is Draw
            2: "Away Win"   # Assuming class 2 from training is Away Win
        }
        if not all(c in class_mapping for c in class_labels):
            print(f"Warning: Model classes {class_labels} not fully covered by defined mapping {class_mapping.keys()}. Results might be misinterpreted.")

    for i, match_idx in enumerate(display_info.index): # display_info should have the same index as features_for_model
        home_team = display_info.loc[match_idx, 'home_team_name']
        away_team = display_info.loc[match_idx, 'away_team_name']
        print(f"\nMatch: {home_team} vs {away_team}")
        
        if probabilities.shape[1] != len(class_mapping):
             print(f"  Warning: Number of probability scores ({probabilities.shape[1]}) does not match number of class labels ({len(class_mapping)}). Probabilities may be misaligned.")

        for class_idx, prob in enumerate(probabilities[i]):
            actual_class_label_from_model = class_labels[class_idx] if class_labels is not None and class_idx < len(class_labels) else class_idx
            label_name = class_mapping.get(actual_class_label_from_model, f"Unknown Class ({actual_class_label_from_model})")
            print(f"  - {label_name}: {prob*100:.1f}%")

if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    
    print("======================================================================")
    print("Running Daily Match Prediction Pipeline")
    print("======================================================================")
    print(f"API Key set: {'Yes' if FOOTBALL_DATA_API_KEY != 'YOUR_API_TOKEN' else 'No (using placeholder - will not fetch real data)'}")
    print(f"Target model: {DEFAULT_MODEL_FILENAME}")
    model_path_check = os.path.join(os.path.dirname(__file__), '..', 'models', DEFAULT_MODEL_FILENAME)
    print(f"Model exists: {'Yes' if os.path.exists(model_path_check) else 'No - you need to train and save a model first!'}")
    print(f"Historical data CSV expected at: {HISTORICAL_DATA_CSV}")
    print(f"Historical data CSV exists: {'Yes' if os.path.exists(HISTORICAL_DATA_CSV) else 'No - will use dummy fallback data.'}")
    print("----------------------------------------------------------------------")

    if not os.path.exists(model_path_check):
        print("\nERROR: Model file not found at expected location:", model_path_check)
        print("Please run the `src/model_training.py` script to generate a sample model,")
        print("or ensure your desired model is correctly placed and named.")
        print("Pipeline cannot proceed without a model.")
    elif FOOTBALL_DATA_API_KEY == "YOUR_API_TOKEN":
        print("\nWARNING: FOOTBALL_DATA_API_KEY is not set or is using the placeholder.")
        print("The pipeline will attempt to run with dummy data for daily matches if `get_matches_for_date` returns empty,")
        print("but this is unlikely to yield meaningful results without actual match data.")
        print("To test with real data, please set the FOOTBALL_DATA_API_KEY environment variable.")
        # For a full test, even with a dummy model, it's better to have some raw match data.
        # We'll proceed, and it will likely say "No matches found".
        predict_daily_matches(today, source_api='football-data') # Specify default API
    else:
        # Run the pipeline for today using football-data by default
        predict_daily_matches(today, source_api='football-data')
        # Example for another API (if implemented and key set)
        # predict_daily_matches(today, source_api='apisports')


    print("\n----------------------------------------------------------------------")
    print("Pipeline execution finished.")
    print("Note: If no matches were fetched or features are misaligned, predictions might be empty or erroneous.")
    print("For meaningful results, ensure a valid API key, a properly trained model, and consistent feature engineering.")
    print("======================================================================")
