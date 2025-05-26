# src/model_training.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
# It's good practice to import xgboost only if available, or list it as a core dependency.
# For now, we'll assume it will be added to requirements.txt
import xgboost as xgb # Placeholder, will ensure it's in requirements.txt
import joblib
import os

# Define a directory to save models
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
if not os.path.exists(MODELS_DIR):
    os.makedirs(MODELS_DIR)

def split_data(df: pd.DataFrame, target_column: str, test_size: float = 0.2, random_state: int = 42):
    """
    Splits the DataFrame into training and testing sets.

    Args:
        df (pd.DataFrame): The input DataFrame with features and target.
        target_column (str): The name of the target variable column.
        test_size (float): The proportion of the dataset to include in the test split.
        random_state (int): Controls the shuffling applied to the data before splitting.

    Returns:
        tuple: X_train, X_test, y_train, y_test
    """
    if target_column not in df.columns:
        raise ValueError(f"Target column '{target_column}' not found in DataFrame.")
    
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y if y.nunique() > 1 else None)
    print(f"Data split into training and testing sets. X_train shape: {X_train.shape}, X_test shape: {X_test.shape}")
    return X_train, X_test, y_train, y_test

def train_logistic_regression(X_train: pd.DataFrame, y_train: pd.Series, model_filename: str = "logistic_regression_model.pkl"):
    """
    Trains a Logistic Regression model and saves it.

    Args:
        X_train (pd.DataFrame): Training features.
        y_train (pd.Series): Training target.
        model_filename (str): Filename to save the trained model.

    Returns:
        LogisticRegression: The trained model.
    """
    print("Training Logistic Regression model...")
    model = LogisticRegression(random_state=42, max_iter=1000) # Increased max_iter for convergence
    model.fit(X_train, y_train)
    
    model_path = os.path.join(MODELS_DIR, model_filename)
    joblib.dump(model, model_path)
    print(f"Logistic Regression model trained and saved to {model_path}")
    return model

def train_random_forest(X_train: pd.DataFrame, y_train: pd.Series, model_filename: str = "random_forest_model.pkl"):
    """
    Trains a Random Forest Classifier model and saves it.

    Args:
        X_train (pd.DataFrame): Training features.
        y_train (pd.Series): Training target.
        model_filename (str): Filename to save the trained model.

    Returns:
        RandomForestClassifier: The trained model.
    """
    print("Training Random Forest model...")
    model = RandomForestClassifier(random_state=42, n_estimators=100) # Basic parameters
    model.fit(X_train, y_train)
    
    model_path = os.path.join(MODELS_DIR, model_filename)
    joblib.dump(model, model_path)
    print(f"Random Forest model trained and saved to {model_path}")
    return model

def train_xgboost(X_train: pd.DataFrame, y_train: pd.Series, model_filename: str = "xgboost_model.pkl"):
    """
    Trains an XGBoost Classifier model and saves it.

    Args:
        X_train (pd.DataFrame): Training features.
        y_train (pd.Series): Training target.
        model_filename (str): Filename to save the trained model.

    Returns:
        xgb.XGBClassifier: The trained model.
    """
    print("Training XGBoost model...")
    # XGBoost requires target to be 0-indexed if it's multiclass
    # This might need adjustment based on how y_train is encoded.
    # For now, assuming y_train is already appropriately encoded.
    model = xgb.XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='mlogloss') # Basic parameters
    model.fit(X_train, y_train)
    
    model_path = os.path.join(MODELS_DIR, model_filename)
    joblib.dump(model, model_path)
    print(f"XGBoost model trained and saved to {model_path}")
    return model

def load_model(model_filename: str):
    """
    Loads a pre-trained model from disk.

    Args:
        model_filename (str): The filename of the model in the models directory.

    Returns:
        object: The loaded model, or None if the file doesn't exist.
    """
    model_path = os.path.join(MODELS_DIR, model_filename)
    if os.path.exists(model_path):
        print(f"Loading model from {model_path}")
        return joblib.load(model_path)
    else:
        print(f"Model file {model_path} not found.")
        return None

if __name__ == '__main__':
    # Example Usage (requires dummy data and actual features/target)
    # This is a conceptual example. Actual training requires preprocessed data.
    print("Model training module example.")

    # Create dummy data for demonstration
    # In a real scenario, this data would come from data_preprocessing module
    num_samples = 100
    data = {
        'feature1': range(num_samples),
        'feature2': [i * 0.5 for i in range(num_samples)],
        'feature3': [i % 3 for i in range(num_samples)], # A categorical-like feature
        # New form features
        'home_form_W': [i % 3 for i in range(num_samples)], # Dummy wins (0, 1, 2)
        'home_form_D': [i % 2 for i in range(num_samples)], # Dummy draws (0, 1)
        'home_form_L': [(5 - (i % 3) - (i % 2)) for i in range(num_samples)], # Dummy losses
        'home_form_games_played': [5] * num_samples,
        'away_form_W': [(i + 1) % 3 for i in range(num_samples)],
        'away_form_D': [(i + 1) % 2 for i in range(num_samples)],
        'away_form_L': [(5 - ((i + 1) % 3) - ((i + 1) % 2)) for i in range(num_samples)],
        'away_form_games_played': [5] * num_samples,
        'target': [0, 1, 2] * (num_samples // 3) + [0] * (num_samples % 3) # 3 classes: 0, 1, 2
    }
    sample_df = pd.DataFrame(data)
    
    # Correcting home_form_L and away_form_L to be non-negative
    sample_df['home_form_L'] = sample_df.apply(lambda row: max(0, 5 - row['home_form_W'] - row['home_form_D']), axis=1)
    sample_df['away_form_L'] = sample_df.apply(lambda row: max(0, 5 - row['away_form_W'] - row['away_form_D']), axis=1)


    # Ensure 'target' column exists
    if 'target' not in sample_df.columns:
        raise KeyError("Dummy DataFrame must include a 'target' column for training.")
    
    # For XGBoost, features with string names sometimes cause issues with default booster.
    # Ensure feature names are compliant or use a different booster if necessary.
    # Here, we assume simple numeric/boolean features for these examples.
    # If you have categorical features, they need to be encoded (e.g., one-hot or label encoding).
    
    # Ensure all features are numeric for these basic model examples
    # (Real preprocessing would handle this more robustly)
    feature_cols = [
        'feature1', 'feature2', 'feature3',
        'home_form_W', 'home_form_D', 'home_form_L', 'home_form_games_played',
        'away_form_W', 'away_form_D', 'away_form_L', 'away_form_games_played'
    ]
    for col in feature_cols:
        if col in sample_df.columns:
             sample_df[col] = pd.to_numeric(sample_df[col], errors='coerce').fillna(0)
        else:
            print(f"Warning: Expected feature column '{col}' not found in dummy data. Creating it with 0s.")
            sample_df[col] = 0


    if not sample_df.empty and 'target' in sample_df.columns and not sample_df.drop(columns=['target']).empty:
        print(f"Sample DataFrame created with shape: {sample_df.shape}")
        print(f"Target distribution:\n{sample_df['target'].value_counts()}")

        # Split data
        X_train_sample, X_test_sample, y_train_sample, y_test_sample = split_data(sample_df, 'target')

        # Train models (using the sample data)
        if not X_train_sample.empty:
            lr_model = train_logistic_regression(X_train_sample, y_train_sample)
            rf_model = train_random_forest(X_train_sample, y_train_sample)
            
            # XGBoost needs target labels to be 0 to num_class-1.
            # Our dummy data y_train_sample is already like that [0, 1, 2].
            xgb_model_sample = train_xgboost(X_train_sample, y_train_sample)

            # Example of loading a model
            loaded_rf_model = load_model("random_forest_model.pkl")
            if loaded_rf_model:
                print("Successfully loaded Random Forest model.")
                # You could then use loaded_rf_model.predict(X_test_sample)
        else:
            print("Skipping model training as X_train_sample is empty.")
    else:
        print("Sample DataFrame is empty or not configured correctly. Skipping training examples.")
