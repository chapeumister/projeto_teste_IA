# Sports Prediction AI Project

This project is a Python-based application designed to predict sports match outcomes using machine learning. It integrates data collection from sports APIs, data preprocessing, model training, and a prediction pipeline, based on the research provided in the issue statement.

## Project Overview

The core aim is to build a flexible framework that can:
- Fetch sports data (initially focusing on football/soccer).
- Preprocess raw data into usable features.
- Train various machine learning models (Logistic Regression, Random Forest, XGBoost).
- Evaluate model performance.
- Provide a pipeline to predict outcomes for upcoming matches.

## Project Structure

```
sports_prediction_ai/
├── data/                     # (Optional) For storing raw or processed data files locally.
├── evaluation_reports/       # For saving model evaluation outputs (e.g., confusion matrices).
├── models/                   # For storing trained machine learning models (e.g., .pkl files).
├── notebooks/                # Jupyter notebooks for experimentation and demonstration.
│   ├── 01_data_collection_and_preprocessing.ipynb
│   └── 02_model_training_and_evaluation.ipynb
├── src/                      # Source code for the project.
│   ├── __init__.py
│   ├── data_collection.py    # Module for fetching data from APIs.
│   ├── data_preprocessing.py # Module for cleaning and transforming data.
│   ├── model_training.py     # Module for training ML models.
│   ├── model_evaluation.py   # Module for evaluating model performance.
│   └── prediction_pipeline.py# Main pipeline for daily predictions.
├── .gitignore                # Specifies intentionally untracked files that Git should ignore.
├── README.md                 # This file.
└── requirements.txt          # Lists Python package dependencies.
```

## Setup and Installation

1.  **Clone the repository (if applicable):**
    ```bash
    # git clone <repository_url>
    # cd sports_prediction_ai
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up API Key (for Football-Data.org):**
    This project uses `football-data.org` as an example data source. To fetch live data, you need an API key.
    - Register at [football-data.org](https://www.football-data.org/login) to get a free API token.
    - Set the API token as an environment variable:
      ```bash
      export FOOTBALL_DATA_API_KEY="YOUR_API_TOKEN"
      ```
      On Windows, use `set FOOTBALL_DATA_API_KEY="YOUR_API_TOKEN"` in Command Prompt or `$env:FOOTBALL_DATA_API_KEY="YOUR_API_TOKEN"` in PowerShell.
    - If the API key is not set, the data collection module will use a placeholder and will not fetch real data.
    - (Optional) Set up API Key for API-SPORTS:
      This project also supports `api-football.com` (API-SPORTS) as a data source.
      - Register at [RapidAPI](https://rapidapi.com/api-sports/api/api-football) to subscribe to the API and get your key.
      - Set the API token as an environment variable:
        ```bash
        export APISPORTS_API_KEY="YOUR_APISPORTS_API_KEY"
        ```
        (or equivalent for your OS).
      - If not set, `get_matches_from_apisports` will print a warning and return no data.
    - **Note on API Key Errors**: The system now provides more specific error messages if an API key is invalid, unauthorized (e.g., HTTP 401/403), or if there are SSL issues during connection, helping to diagnose setup problems more easily.


## How to Run

After setting up the project and installing dependencies, you can run the different components as follows:

### 1. Train Models (Example)
To create initial example models (trained on dummy data):
```bash
python src/model_training.py
```
This will save `logistic_regression_model.pkl`, `random_forest_model.pkl`, and `xgboost_model.pkl` to the `models/` directory.

### 2. Run Daily Prediction Pipeline
To make predictions for today's matches (using a pre-trained model, e.g., `random_forest_model.pkl`):
```bash
python src/prediction_pipeline.py
```
- Make sure `FOOTBALL_DATA_API_KEY` is set to get live match data.
- The pipeline will use `random_forest_model.pkl` by default. You can modify `DEFAULT_MODEL_FILENAME` in the script to use another model.
- **Important**: The feature generation in `prediction_pipeline.py` now includes team form features derived from historical data. For meaningful predictions with a custom-trained model, ensure the features generated align with your model's training.
- The pipeline attempts to load historical data from `data/historical_matches_sample.csv`. If not found, it uses a small internal dummy dataset.

### 3. Use Jupyter Notebooks
The `notebooks/` directory contains examples:
-   `01_data_collection_and_preprocessing.ipynb`: Demonstrates fetching and initial preprocessing of data.
-   `02_model_training_and_evaluation.ipynb`: Shows an example of training a model, evaluating it, and running the prediction pipeline within a notebook environment.

Open Jupyter Lab or Jupyter Notebook:
```bash
jupyter lab # or jupyter notebook
```
Then navigate to the `notebooks/` directory and open the `.ipynb` files.

## Modules Description

### `src/data_collection.py`
-   Handles fetching match data from external APIs.
-   `get_matches_for_date(date_str: str, api_key: str)`: Fetches matches for a specific date from `football-data.org`. This function is primarily called by `get_matches_with_fallback`. It includes detailed error handling for API key issues, network problems, and SSL errors.
-   `get_matches_with_fallback(date_str: str, use_mock_data: bool = False, api_key: str)`: The primary function for fetching matches from `football-data.org`.
    -   It first attempts to fetch live data using `get_matches_for_date`.
    -   If live data fetching fails (e.g., due to API errors, no matches found, or network issues) AND the `use_mock_data` parameter is set to `True`, it will attempt to load mock match data from `data/mock_matches.json`. This is useful for development and testing when live API access is unavailable or problematic.
-   `get_matches_from_apisports(date_str: str, api_key: str = None)`: Fetches matches for a specific date from `api-football.com` (API-SPORTS). This function also includes detailed logging regarding the number of matches fetched versus what the API reports, aiding in identifying discrepancies. Requires `APISPORTS_API_KEY`.

### `src/data_preprocessing.py`
-   Responsible for cleaning raw API data and transforming it into a structured format (Pandas DataFrame).
-   `preprocess_match_data`: Performs initial parsing of match details.
-   `engineer_form_features`: Calculates team form features (Wins, Draws, Losses for the last N games) based on historical match data. It expects a DataFrame of current matches and a DataFrame of historical matches.
-   **Note:** The quality of form features heavily depends on the availability and quality of historical data.

### `src/model_training.py`
-   Contains functions for training and saving machine learning models.
    -   `split_data`: Splits data into training and test sets.
    -   `train_logistic_regression`, `train_random_forest`, `train_xgboost`: Train respective models.
    -   `load_model`: Loads a saved model from the `models/` directory.
-   Trained models are saved to the `models/` directory using `joblib`.
-   The script includes a `if __name__ == "__main__":` block that can be run to train and save dummy versions of the models using synthetic data.

### `src/model_evaluation.py`
-   Provides tools to evaluate the performance of trained models.
    -   `get_classification_metrics`: Calculates accuracy, precision, recall, F1-score, and AUC. Generates a classification report.
    -   `plot_confusion_matrix`: Saves a plot of the confusion matrix to the `evaluation_reports/` directory.
-   The script includes a `if __name__ == "__main__":` block with example usage.

### `src/prediction_pipeline.py`
-   Integrates the other modules to provide an end-to-end prediction flow:
    1.  Fetches matches for the current day (or a specified date) using `get_matches_with_fallback` for `football-data.org` or `get_matches_from_apisports` for API-SPORTS. The `predict_daily_matches` function in this module has a `use_mock_data_if_unavailable` parameter (defaulting to `False`) which, if `True`, allows `get_matches_with_fallback` to use `data/mock_matches.json` if live data fetching from `football-data.org` fails.
    2.  Preprocesses the raw match data.
    3.  **Loads historical match data**: Attempts to load from `data/historical_matches_sample.csv`. If this file is not found or is invalid, it falls back to a minimal internal dummy dataset for demonstration purposes. For production use, a robust historical data source is essential.
    4.  **Generates features for prediction**: This step now includes the calculation of team form features (W-D-L over last N games) using `engineer_form_features` from `data_preprocessing.py`. It also adds other placeholder features (`feature1`, `feature2`, `feature3`) to align with the example model training script.
    5.  Loads a pre-trained model from the `models/` directory.
    6.  Makes predictions (outputs probabilities for home win, draw, away win).
    7.  Displays the results.
-   Can be run directly: `python src/prediction_pipeline.py`.
-   Requires a trained model to be present in the `models/` directory (e.g., by running `src/model_training.py` first).
-   The `data/historical_matches_sample.csv` file provides a small, sample dataset of historical matches. It is used by `prediction_pipeline.py` to demonstrate form feature calculation. Users should replace or augment this with more comprehensive historical data for actual model training and better predictions.

## Data Files

-   **`data/historical_matches_sample.csv`**: A sample CSV file containing dummy historical match data. This file is used by `prediction_pipeline.py` and can be used in notebooks to demonstrate the calculation of team form features. It includes columns like `home_team_id`, `away_team_id`, `home_team_score`, `away_team_score`, and `utcDate`. For serious use, this should be replaced with a comprehensive and regularly updated historical dataset.
-   **`data/mock_matches.json`**: A sample JSON file containing mock match data in the `football-data.org` API format. This is used by `get_matches_with_fallback` if live data fetching fails and the `use_mock_data` option is enabled, allowing for development and testing without live API calls.

## Future Enhancements (Based on Research Document)

-   **Advanced Feature Engineering**: While basic form features are now included, further expansion (H2H, player stats, Elo ratings, etc.) is key.
-   **Comprehensive Historical Data Management**: Replace `data/historical_matches_sample.csv` with a robust system for collecting, storing, and accessing historical match data (e.g., a database).
-   **Multiple Data Sources**: Further leverage `API-SPORTS` and integrate other sources like `Sportmonks`, `Sportsipy`.
-   **Model Tuning and Selection**: Implement hyperparameter tuning (e.g., GridSearchCV, RandomizedSearchCV) and more rigorous cross-validation strategies.
-   **Ensemble Models**: Experiment with ensembling techniques (e.g., voting classifiers, stacking).
-   **Time Series Models (LSTMs)**: Explore LSTMs for capturing sequential patterns if sufficient temporal data is available.
-   **Database Integration**: Store collected data and predictions in a database for easier management and historical analysis.
-   **API for Predictions**: Expose the prediction pipeline via a simple API (e.g., using Flask/FastAPI).
-   **Continuous Evaluation and Retraining**: Set up a system for ongoing model performance monitoring and periodic retraining.

## Contributing
(Placeholder for contribution guidelines if this were an open project)

## License
(Placeholder for license information - e.g., MIT License)
