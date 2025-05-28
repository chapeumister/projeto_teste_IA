# src/model_training.py
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
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
        RandomForestClassifier: The best trained model found by GridSearchCV.
    """
    print("Training Random Forest model with GridSearchCV...")

    param_grid = {
        'n_estimators': [50, 100], # Reduced for faster execution in this context
        'max_depth': [None, 10], # Reduced options
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2]
    }

    # Base model
    rf = RandomForestClassifier(random_state=42)

    # Grid search
    # Using cv=2 for faster execution in this example; recommend 3 or 5 for real use.
    grid_search = GridSearchCV(estimator=rf, param_grid=param_grid, cv=2, 
                               scoring='accuracy', n_jobs=-1, verbose=1)
    
    print(f"Fitting GridSearchCV for Random Forest. This might take a while...")
    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_
    print(f"Best parameters found by GridSearchCV: {grid_search.best_params_}")
    
    model_path = os.path.join(MODELS_DIR, model_filename)
    joblib.dump(best_model, model_path)
    print(f"Best Random Forest model saved to {model_path}")
    
    return best_model

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
    print("Training XGBoost model with early stopping...")

    # Split the provided training data into a new training set and a validation set for early stopping
    # Using a small test_size for validation, e.g., 0.1 or 0.2. Let's use 0.15.
    # Ensure stratify is used if y_train is suitable (e.g., classification target)
    stratify_option = y_train if y_train.nunique() > 1 else None
    X_train_xgb, X_val_xgb, y_train_xgb, y_val_xgb = train_test_split(
        X_train, y_train, test_size=0.15, random_state=42, stratify=stratify_option
    )
    
    print(f"Internal split for XGBoost: X_train_xgb shape: {X_train_xgb.shape}, X_val_xgb shape: {X_val_xgb.shape}")

    model = xgb.XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='mlogloss')
    
    model.fit(X_train_xgb, y_train_xgb,
              eval_set=[(X_val_xgb, y_val_xgb)],
              early_stopping_rounds=10,
              verbose=False)
    
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
        
        # New comprehensive form features
        # Home team
        'home_form_overall_W': [i % 4 for i in range(num_samples)], # Wins in last 5 overall (0-3)
        'home_form_overall_D': [i % 2 for i in range(num_samples)], # Draws in last 5 overall (0-1)
        'home_form_overall_games_played': [5] * num_samples,
        
        'home_form_home_W': [i % 3 for i in range(num_samples)],    # Wins in last 5 home (0-2)
        'home_form_home_D': [i % 2 for i in range(num_samples)],    # Draws in last 5 home (0-1)
        'home_form_home_games_played': [min(5, 2 + i % 3) for i in range(num_samples)], # Plausible home games played

        'home_form_away_W': [i % 3 for i in range(num_samples)],    # Wins in last 5 away (0-2)
        'home_form_away_D': [i % 2 for i in range(num_samples)],    # Draws in last 5 away (0-1)
        'home_form_away_games_played': [min(5, 2 + i % 2) for i in range(num_samples)], # Plausible away games played

        # Away team
        'away_form_overall_W': [(i+1) % 4 for i in range(num_samples)],
        'away_form_overall_D': [(i+1) % 2 for i in range(num_samples)],
        'away_form_overall_games_played': [5] * num_samples,

        'away_form_home_W': [(i+1) % 3 for i in range(num_samples)],
        'away_form_home_D': [(i+1) % 2 for i in range(num_samples)],
        'away_form_home_games_played': [min(5, 2 + (i+1) % 3) for i in range(num_samples)],

        'away_form_away_W': [(i+1) % 3 for i in range(num_samples)],
        'away_form_away_D': [(i+1) % 2 for i in range(num_samples)],
        'away_form_away_games_played': [min(5, 2 + (i+1) % 2) for i in range(num_samples)],
        
        'target': [0, 1, 2] * (num_samples // 3) + [0] * (num_samples % 3) # 3 classes: 0, 1, 2
    }
    sample_df = pd.DataFrame(data)

    # Calculate L from W, D, and games_played for all form categories
    form_categories = [
        'home_form_overall', 'home_form_home', 'home_form_away',
        'away_form_overall', 'away_form_home', 'away_form_away'
    ]
    for cat in form_categories:
        w_col = f'{cat}_W'
        d_col = f'{cat}_D'
        gp_col = f'{cat}_games_played'
        l_col = f'{cat}_L'
        sample_df[l_col] = sample_df.apply(lambda row: max(0, row[gp_col] - row[w_col] - row[d_col]), axis=1)

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
        'home_form_overall_W', 'home_form_overall_D', 'home_form_overall_L', 'home_form_overall_games_played',
        'home_form_home_W', 'home_form_home_D', 'home_form_home_L', 'home_form_home_games_played',
        'home_form_away_W', 'home_form_away_D', 'home_form_away_L', 'home_form_away_games_played',
        'away_form_overall_W', 'away_form_overall_D', 'away_form_overall_L', 'away_form_overall_games_played',
        'away_form_home_W', 'away_form_home_D', 'away_form_home_L', 'away_form_home_games_played',
        'away_form_away_W', 'away_form_away_D', 'away_form_away_L', 'away_form_away_games_played'
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
