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
    from data_preprocessing import preprocess_match_data
    from model_training import load_model


# Configuration
DEFAULT_MODEL_FILENAME = "random_forest_model.pkl" # Example: use Random Forest by default
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_API_TOKEN")


def generate_features_for_prediction(matches_df: pd.DataFrame):
    """
    Simulates feature generation for prediction.
    In a real system, this would involve fetching historical data for the teams involved
    in `matches_df` and calculating features like recent form, head-to-head, etc.
    For this example, we'll create dummy features.

    Args:
        matches_df (pd.DataFrame): DataFrame of upcoming matches, possibly with basic info.

    Returns:
        pd.DataFrame: DataFrame with features ready for the model.
                      Returns an empty DataFrame if input is empty.
    """
    if matches_df.empty:
        return pd.DataFrame()

    # Placeholder: These features need to match what the model was trained on.
    # For the dummy model in model_training.py, it was 'feature1', 'feature2', 'feature3'.
    # Let's assume our real model was trained on features derived from match data.
    # For this pipeline example, we'll generate some arbitrary features.
    # In a real scenario, you'd use team names/IDs from matches_df to look up historical stats.
    
    features_for_prediction = pd.DataFrame(index=matches_df.index)
    
    # Example dummy features (these would be actual computed stats in reality)
    # These names MUST match the feature names used during model training.
    # Our dummy model used 'feature1', 'feature2', 'feature3'.
    # Let's pretend we can derive these from match info.
    features_for_prediction['feature1'] = matches_df.index * 0.5 # Example based on match index
    features_for_prediction['feature2'] = 1.0 # Constant value example
    features_for_prediction['feature3'] = (matches_df.index % 3) # Another example
    
    # Make sure to handle cases where matches_df might be small
    if len(matches_df) == 1: # if only one match, make sure features are still 2D for model
        features_for_prediction = pd.DataFrame(features_for_prediction.iloc[0]).T


    print(f"Generated dummy features for {len(matches_df)} matches.")
    return features_for_prediction


def predict_daily_matches(date_str: str, model_filename: str = DEFAULT_MODEL_FILENAME, api_key: str = FOOTBALL_DATA_API_KEY):
    """
    Full pipeline: Fetches matches, preprocesses, generates features, predicts, and displays results.

    Args:
        date_str (str): Date for which to predict matches ("YYYY-MM-DD").
        model_filename (str): Filename of the pre-trained model to load.
        api_key (str): API key for the data source.
    """
    print(f"Starting prediction pipeline for date: {date_str} using model: {model_filename}")

    # 1. Fetch matches for the day
    print("\nStep 1: Fetching matches...")
    raw_matches = get_matches_for_date(date_str, api_key=api_key)
    if not raw_matches:
        print("No matches found or API error. Exiting pipeline.")
        return
    print(f"Fetched {len(raw_matches)} raw match entries.")

    # 2. Preprocess match data (basic preprocessing)
    print("\nStep 2: Preprocessing match data...")
    # The preprocess_match_data function returns a DataFrame
    matches_df = preprocess_match_data(raw_matches)
    if matches_df.empty:
        print("No match data to process after initial preprocessing. Exiting pipeline.")
        return
    print(f"Preprocessed {len(matches_df)} matches into DataFrame.")
    
    # Keep essential info for display later
    display_info = matches_df[['home_team_name', 'away_team_name']].copy()

    # 3. Generate features for prediction
    # This is a CRITICAL step. The features generated here MUST match the
    # features the model was trained on.
    print("\nStep 3: Generating features for prediction...")
    features_df = generate_features_for_prediction(matches_df)
    if features_df.empty:
        print("Could not generate features for the matches. Exiting pipeline.")
        return
    
    # Ensure feature names match those used in training (example: 'feature1', 'feature2', 'feature3')
    # If the dummy model from model_training.py was saved, it expects these.
    # A real model would have more meaningful feature names.
    expected_features = ['feature1', 'feature2', 'feature3'] # From dummy training
    if not all(f in features_df.columns for f in expected_features):
        print(f"Error: Generated features do not match expected features: {expected_features}")
        print(f"Actual features: {features_df.columns.tolist()}")
        print("The `generate_features_for_prediction` function needs to produce the same features as the training data.")
        return


    # 4. Load the pre-trained model
    print("\nStep 4: Loading pre-trained model...")
    model = load_model(model_filename)
    if model is None:
        print(f"Failed to load model '{model_filename}'. Ensure it's trained and available in the 'models' directory.")
        print("You might need to run the model_training.py script first to create a dummy model.")
        return

    # 5. Make predictions
    print("\nStep 5: Making predictions...")
    try:
        probabilities = model.predict_proba(features_df)
        # predict_proba returns an array of shape (n_samples, n_classes)
        # For 3 classes (e.g., Home Win, Draw, Away Win)
    except Exception as e:
        print(f"Error during model prediction: {e}")
        print("Ensure the features used for prediction are consistent with model training (e.g. dtypes, number of features).")
        return
        
    # 6. Display results
    print("\nStep 6: Prediction Results:")
    # Assuming classes are [Home Win, Draw, Away Win] - this depends on model training.
    # The dummy model in model_training.py has classes [0, 1, 2].
    # Let's map them: 0=Home, 1=Draw, 2=Away (this is an assumption!)
    class_labels = getattr(model, 'classes_', None)
    if class_labels is None:
        # If model.classes_ is not available (e.g. not a scikit-learn classifier, or not fitted)
        # Fallback to a default or raise error. For dummy model, assume 0, 1, 2.
        print("Warning: model.classes_ not found. Assuming classes [0, 1, 2] mapped to [Home, Draw, Away]")
        class_mapping = {0: "Home Win", 1: "Draw", 2: "Away Win"}
    else:
        # Dynamically create mapping based on model's classes
        # This is more robust if your label encoder changes order or values
        # You need to define what each class value (e.g. 0, 1, 2) means
        # For this example, let's assume 0=Home, 1=Draw, 2=Away.
        # A better way is to save the label encoder or class mapping with the model.
        class_mapping = {
            # This mapping needs to be defined based on how 'target' was encoded during training
            # E.g., if 'Home Win' was encoded as 0, 'Draw' as 1, 'Away Win' as 2
            0: "Home Win",  # Assuming class 0 from training is Home Win
            1: "Draw",      # Assuming class 1 from training is Draw
            2: "Away Win"   # Assuming class 2 from training is Away Win
        }
        # Verify that all model classes are in our mapping
        if not all(c in class_mapping for c in class_labels):
            print(f"Warning: Model classes {class_labels} not fully covered by defined mapping {class_mapping.keys()}. Results might be misinterpreted.")


    for i, match_idx in enumerate(display_info.index):
        home_team = display_info.loc[match_idx, 'home_team_name']
        away_team = display_info.loc[match_idx, 'away_team_name']
        print(f"\nMatch: {home_team} vs {away_team}")
        
        if probabilities.shape[1] != len(class_mapping):
             print(f"  Warning: Number of probability scores ({probabilities.shape[1]}) does not match number of class labels ({len(class_mapping)}). Probabilities may be misaligned.")

        for class_idx, prob in enumerate(probabilities[i]):
            # Use model.classes_ to get the actual class label for this probability column
            # This is crucial if the order of classes in predict_proba is not guaranteed
            # or if the class labels are not simple 0, 1, 2.
            actual_class_label_from_model = class_labels[class_idx] if class_labels is not None and class_idx < len(class_labels) else class_idx
            label_name = class_mapping.get(actual_class_label_from_model, f"Unknown Class ({actual_class_label_from_model})")
            print(f"  - {label_name}: {prob*100:.1f}%")

if __name__ == "__main__":
    # Ensure you have a trained model file (e.g., "random_forest_model.pkl") in the "models" directory.
    # You may need to run src/model_training.py first to generate a dummy model.
    # Also, set the FOOTBALL_DATA_API_KEY environment variable to a valid key to fetch real matches.
    # If not set, it will use "YOUR_API_TOKEN" and get_matches_for_date will return an empty list.

    today = datetime.now().strftime("%Y-%m-%d")
    
    print("======================================================================")
    print("Running Daily Match Prediction Pipeline")
    print("======================================================================")
    print(f"API Key set: {'Yes' if FOOTBALL_DATA_API_KEY != 'YOUR_API_TOKEN' else 'No (using placeholder - will not fetch real data)'}")
    print(f"Target model: {DEFAULT_MODEL_FILENAME}")
    model_path_check = os.path.join(os.path.dirname(__file__), '..', 'models', DEFAULT_MODEL_FILENAME)
    print(f"Model exists: {'Yes' if os.path.exists(model_path_check) else 'No - you need to train and save a model first!'}")
    print("----------------------------------------------------------------------")

    if FOOTBALL_DATA_API_KEY == "YOUR_API_TOKEN":
        print("\nWARNING: FOOTBALL_DATA_API_KEY is not set or is using the placeholder.")
        print("The pipeline will attempt to run but `get_matches_for_date` will return no data.")
        print("To test with real data, please set this environment variable.")
        print("To test the pipeline flow with dummy data, ensure a dummy model is generated via `model_training.py`.")
        # To proceed with a dry run using dummy data, one would typically mock the API call.
        # For this example, we'll let it call get_matches_for_date which will return empty.
    
    if not os.path.exists(model_path_check):
        print("\nERROR: Model file not found at expected location:", model_path_check)
        print("Please run the `src/model_training.py` script to generate a sample model,")
        print("or ensure your desired model is correctly placed and named.")
    
    # Run the pipeline for today
    predict_daily_matches(today)

    print("\n----------------------------------------------------------------------")
    print("Pipeline execution finished.")
    print("Note: If no matches were fetched or features are misaligned, predictions might be empty or erroneous.")
    print("For meaningful results, ensure a valid API key, a properly trained model, and consistent feature engineering.")
    print("======================================================================")
