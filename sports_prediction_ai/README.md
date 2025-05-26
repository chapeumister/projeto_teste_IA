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

## Modules Description

### `src/data_collection.py`
-   Handles fetching match data from external APIs.
-   Currently includes a function `get_matches_for_date` for `football-data.org`.
-   Requires the `FOOTBALL_DATA_API_KEY` environment variable for `football-data.org`.

### `src/data_preprocessing.py`
-   Responsible for cleaning raw API data and transforming it into a structured format (Pandas DataFrame).
-   Includes `preprocess_match_data` which performs initial parsing of match details.
-   **Note:** Advanced feature engineering (e.g., team form, head-to-head stats) is a complex part that would be expanded here. The current version provides a basic structure.

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
    1.  Fetches matches for the current day (or a specified date).
    2.  Preprocesses the match data.
    3.  **Generates features for prediction**: Currently, this step (`generate_features_for_prediction`) creates *dummy features*. For a real-world application, this function must be updated to generate features consistent with those used during model training.
    4.  Loads a pre-trained model from the `models/` directory.
    5.  Makes predictions (outputs probabilities for home win, draw, away win).
    6.  Displays the results.
-   Can be run directly: `python src/prediction_pipeline.py`.
-   Requires a trained model to be present in the `models/` directory (e.g., by running `src/model_training.py` first).

## How to Run

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
- **Important**: The current feature generation in `prediction_pipeline.py` is placeholder. For meaningful predictions, the features generated must match how the chosen model was actually trained.

### 3. Use Jupyter Notebooks
The `notebooks/` directory contains examples:
-   `01_data_collection_and_preprocessing.ipynb`: Demonstrates fetching and initial preprocessing of data.
-   `02_model_training_and_evaluation.ipynb`: Shows an example of training a model, evaluating it, and running the prediction pipeline within a notebook environment.

Open Jupyter Lab or Jupyter Notebook:
```bash
jupyter lab # or jupyter notebook
```
Then navigate to the `notebooks/` directory and open the `.ipynb` files.

## Future Enhancements (Based on Research Document)

-   **Advanced Feature Engineering**: Implement calculation of historical statistics (form, H2H, player stats, etc.). This is crucial for model performance.
-   **Multiple Data Sources**: Integrate more APIs and data sources (e.g., `API-SPORTS`, `Sportmonks`, `Sportsipy` library) as outlined in the research.
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
