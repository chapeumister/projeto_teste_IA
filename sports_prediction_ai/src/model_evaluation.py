# src/model_evaluation.py
import pandas as pd
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_auc_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Define a directory to save evaluation outputs, e.g., plots
EVALUATION_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'evaluation_reports')
if not os.path.exists(EVALUATION_OUTPUT_DIR):
    os.makedirs(EVALUATION_OUTPUT_DIR)

def get_classification_metrics(y_true: pd.Series, y_pred: pd.Series, y_prob: np.ndarray = None, average: str = 'weighted', labels: list = None, target_names: list = None):
    """
    Calculates and returns common classification metrics.

    Args:
        y_true (pd.Series): True labels.
        y_pred (pd.Series): Predicted labels.
        y_prob (np.ndarray, optional): Predicted probabilities (for AUC). 
                                      Shape (n_samples, n_classes).
        average (str): Type of averaging for precision, recall, F1-score ('micro', 'macro', 'weighted', 'samples').
        labels (list, optional): The set of labels to include when average is not None.
        target_names (list, optional): Display names for labels in classification report.

    Returns:
        dict: A dictionary containing accuracy, precision, recall, F1-score, and AUC (if y_prob provided).
    """
    accuracy = accuracy_score(y_true, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average=average, labels=labels, zero_division=0)
    
    metrics = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
    }

    print(f"\nClassification Metrics (average='{average}'):")
    for k, v in metrics.items():
        print(f"  {k.capitalize()}: {v:.4f}")

    print("\nDetailed Classification Report:")
    # Ensure labels and target_names are correctly passed if provided
    if labels and not target_names: # If only numeric labels are passed, convert to string for report
        target_names = [str(label) for label in labels]
    
    report = classification_report(y_true, y_pred, labels=labels, target_names=target_names, zero_division=0)
    print(report)
    metrics["classification_report"] = report

    # Calculate AUC if probabilities are provided
    # AUC calculation depends on the number of classes.
    # For multiclass, it's typically one-vs-rest (ovr) or one-vs-one (ovo).
    if y_prob is not None and hasattr(y_true, 'unique'):
        unique_classes = sorted(y_true.unique())
        if len(unique_classes) > 2: # Multiclass
            try:
                # Ensure y_prob has the correct shape for multiclass (n_samples, n_classes)
                if y_prob.ndim == 1 or y_prob.shape[1] != len(unique_classes):
                     print(f"Warning: y_prob shape {y_prob.shape} might be incompatible for multiclass AUC with {len(unique_classes)} classes. Skipping AUC.")
                else:
                    auc_roc = roc_auc_score(y_true, y_prob, multi_class='ovr', average=average, labels=labels)
                    metrics["auc_roc_ovr"] = auc_roc
                    print(f"  AUC (One-vs-Rest, average='{average}'): {auc_roc:.4f}")
            except ValueError as e:
                print(f"Could not calculate AUC (multiclass): {e}. Ensure y_prob is correctly formatted and labels are consistent.")
        elif len(unique_classes) == 2: # Binary classification
            # y_prob for binary should be probability of the positive class
            # Typically, this is the second column if shape is (n_samples, 2)
            prob_positive_class = y_prob[:, 1] if y_prob.ndim > 1 and y_prob.shape[1] == 2 else y_prob
            if prob_positive_class.ndim == 1:
                auc_roc = roc_auc_score(y_true, prob_positive_class) # Default uses roc_auc_score for binary
                metrics["auc_roc"] = auc_roc
                print(f"  AUC: {auc_roc:.4f}")
            else:
                print(f"Warning: y_prob for binary classification has unexpected shape {prob_positive_class.shape}. Skipping AUC.")
        else:
            print("Warning: Only one class present in y_true. AUC cannot be calculated.")
            
    return metrics

def plot_confusion_matrix(y_true: pd.Series, y_pred: pd.Series, labels: list = None, display_labels: list = None, filename: str = "confusion_matrix.png"):
    """
    Computes and plots the confusion matrix.

    Args:
        y_true (pd.Series): True labels.
        y_pred (pd.Series): Predicted labels.
        labels (list, optional): List of labels to index the matrix.
        display_labels (list, optional): Target names to display. If None, uses sorted unique values from y_true and y_pred.
        filename (str): Filename to save the plot.
    """
    if display_labels is None:
        if labels is not None:
            display_labels = [str(l) for l in labels]
        else:
            # Attempt to get sorted unique labels from data itself
            combined_labels = sorted(list(set(y_true) | set(y_pred)))
            display_labels = [str(l) for l in combined_labels]
            if not labels: # if labels were not provided, use these as the indexing labels too
                labels = combined_labels


    cm = confusion_matrix(y_true, y_pred, labels=labels)
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=display_labels, yticklabels=display_labels)
    plt.title("Confusion Matrix")
    plt.ylabel("Actual")
    plt.xlabel("Predicted")
    
    plot_path = os.path.join(EVALUATION_OUTPUT_DIR, filename)
    try:
        plt.savefig(plot_path)
        print(f"Confusion matrix saved to {plot_path}")
    except Exception as e:
        print(f"Error saving confusion matrix plot: {e}")
    plt.close() # Close the plot to free memory

if __name__ == '__main__':
    # Example Usage (using dummy data similar to model_training.py)
    print("Model Evaluation module example.")

    # Create dummy data for demonstration (as if from a model's test set output)
    num_samples = 100
    y_true_sample = pd.Series([0, 1, 2] * (num_samples // 3) + [0] * (num_samples % 3), name="true_labels")
    
    # Simulate predictions - perfect, some errors, more errors
    y_pred_sample_good = y_true_sample.copy() # Perfect prediction for simplicity here
    
    y_pred_sample_errors = y_true_sample.copy()
    if len(y_pred_sample_errors) > 5: # Introduce some errors
        y_pred_sample_errors.iloc[0:5] = (y_pred_sample_errors.iloc[0:5] + 1) % 3 

    # Simulate probabilities (for 3 classes: 0, 1, 2)
    # Probabilities should sum to 1 across classes for each sample
    # This is a very simplistic way to generate probabilities
    y_prob_sample = np.zeros((num_samples, 3))
    for i in range(num_samples):
        true_class = y_true_sample.iloc[i]
        y_prob_sample[i, true_class] = 0.7 # High prob for true class
        other_classes = [c for c in [0,1,2] if c != true_class]
        y_prob_sample[i, other_classes[0]] = 0.2
        y_prob_sample[i, other_classes[1]] = 0.1
        y_prob_sample[i] = y_prob_sample[i] / np.sum(y_prob_sample[i]) # Normalize


    class_labels_numeric = [0, 1, 2]
    class_target_names = ["Home Win (0)", "Draw (1)", "Away Win (2)"] # Example target names

    print("\n--- Evaluating 'Good' Predictions ---")
    metrics_good = get_classification_metrics(y_true_sample, y_pred_sample_good, y_prob_sample, 
                                              average='weighted', labels=class_labels_numeric, target_names=class_target_names)
    plot_confusion_matrix(y_true_sample, y_pred_sample_good, labels=class_labels_numeric, display_labels=class_target_names, filename="cm_good_predictions.png")

    print("\n--- Evaluating Predictions with Some Errors ---")
    metrics_errors = get_classification_metrics(y_true_sample, y_pred_sample_errors, y_prob_sample, 
                                                average='weighted', labels=class_labels_numeric, target_names=class_target_names)
    plot_confusion_matrix(y_true_sample, y_pred_sample_errors, labels=class_labels_numeric, display_labels=class_target_names, filename="cm_errors_predictions.png")

    # Example for binary case (if needed, not directly from this dummy data unless we filter)
    # y_true_binary = y_true_sample[y_true_sample.isin([0,1])].replace({0:0, 1:1}) # filter for two classes
    # y_pred_binary = y_pred_sample_errors[y_true_sample.isin([0,1])].replace({0:0, 1:1})
    # y_prob_binary = y_prob_sample[y_true_sample.isin([0,1])][:, :2] # Prob for class 0 and 1
    # y_prob_binary = y_prob_binary / np.sum(y_prob_binary, axis=1, keepdims=True) # re-normalize
    
    # if not y_true_binary.empty:
    #     print("\n--- Evaluating Binary Subset (Classes 0 and 1) ---")
    #     metrics_binary = get_classification_metrics(y_true_binary, y_pred_binary, y_prob_binary, 
    #                                                 average='binary' if y_true_binary.nunique() <=2 else 'weighted', # 'binary' for positive class if applicable
    #                                                 labels=[0,1], target_names=["Class 0", "Class 1"])
    #     plot_confusion_matrix(y_true_binary, y_pred_binary, labels=[0,1], display_labels=["Class 0", "Class 1"], filename="cm_binary_subset.png")

    print("\nEvaluation module example run complete.")
    print(f"Plots, if generated, are saved in: {EVALUATION_OUTPUT_DIR}")
