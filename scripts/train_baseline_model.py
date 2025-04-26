#!/usr/bin/env python3
import os
import sys
import pandas as pd
import numpy as np # Import numpy
from pathlib import Path
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, log_loss, confusion_matrix
import lightgbm as lgb
import matplotlib.pyplot as plt # For plotting
import seaborn as sns # For plotting

# Add project root to Python path to allow importing from src
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    # Assuming execution from project root or src is in PYTHONPATH
    from src.config import PROCESSED_DATA_DIR
except ImportError:
    logging.error("Could not import config modules. Make sure PYTHONPATH is set correctly or run from project root/scripts.")
    # Fallback paths for basic execution (adjust if needed)
    BASE_DIR = Path(__file__).resolve().parent.parent
    PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- !!! UPDATED FILENAME !!! ---
INPUT_FILENAME = "ml_predictors_target_2H_AH0_5_odds_feat_dataset.csv"
TARGET_AH_LINE = -0.5 # Must match the value in build_ml_dataset script
TARGET_COLUMN = f'target_fav_covers_ah_{str(TARGET_AH_LINE).replace(".","_").replace("-","neg")}_2H'

# --- !!! UPDATED FEATURE COLUMNS !!! ---
# Define which columns are features (differentials + odds features)
FEATURE_COLUMNS = [
    # Differentials
    'ht_diff_fouls', 'ht_diff_red_cards', 'ht_diff_tackles',
    'ht_diff_shots_blocked', 'ht_diff_successful_passes_percentage',
    'ht_diff_ball_possession', 'ht_diff_saves', 'ht_diff_attacks',
    'ht_diff_shots_total', 'ht_diff_shots_insidebox',
    'ht_diff_goals',
    # Odds Features
    'odds_ratio_hw',
]
# --- End Updates ---

# Model Training Parameters
TEST_SIZE = 0.2 # Fraction of data for testing
RANDOM_STATE = 42 # For reproducibility of train/test split

# LightGBM Parameters (Basic)
LGB_PARAMS = {
    'objective': 'binary',
    'metric': 'binary_logloss',
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'seed': RANDOM_STATE,
    'verbose': -1
}

def plot_confusion_matrix(cm, classes, normalize=False, title='Confusion matrix', cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        logging.info("Normalized confusion matrix")
    else:
        logging.info('Confusion matrix, without normalization')

    logging.info(f"\n{cm}")

    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt=".2f" if normalize else "d", cmap=cmap, xticklabels=classes, yticklabels=classes)
    plt.title(title)
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.tight_layout()
    plot_filename = f"confusion_matrix_{TARGET_COLUMN}_oddsfeat.png" # Updated plot filename
    plt.savefig(plot_filename)
    logging.info(f"Confusion matrix plot saved to {plot_filename}")
    plt.close()


def train_baseline():
    """Loads data, trains a baseline LightGBM model using odds features, and evaluates it."""
    logging.info(f"=== Starting Baseline Model Training (Target: {TARGET_COLUMN}, Incl. Odds Features) ===")

    # 1. Load Data
    input_path = PROCESSED_DATA_DIR / INPUT_FILENAME
    if not input_path.exists():
        logging.error(f"Input dataset not found: {input_path}")
        logging.error("Please run the updated build_ml_dataset script first.")
        sys.exit(1)

    logging.info(f"Loading dataset from: {input_path}")
    try:
        df = pd.read_csv(input_path)
        logging.info(f"Dataset loaded successfully with {len(df)} rows.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        sys.exit(1)

    # Verify required columns exist
    if TARGET_COLUMN not in df.columns:
        logging.error(f"Target column '{TARGET_COLUMN}' not found in the dataset.")
        sys.exit(1)
    missing_features = [col for col in FEATURE_COLUMNS if col not in df.columns]
    if missing_features:
        logging.error(f"Missing feature columns in the dataset: {missing_features}")
        sys.exit(1)

    # Handle potential infinite or NaN values in features (important for odds calculations)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    if df[FEATURE_COLUMNS].isnull().any().any():
        missing_cols_report = df[FEATURE_COLUMNS].isnull().sum()
        missing_cols_report = missing_cols_report[missing_cols_report > 0]
        logging.warning(f"NaN/Infinite values found in feature columns after loading. Imputing with 0.\n{missing_cols_report}")
        # Consider more sophisticated imputation, especially for odds, if this happens frequently
        df[FEATURE_COLUMNS] = df[FEATURE_COLUMNS].fillna(0)


    # 2. Prepare Features (X) and Target (y)
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    logging.info(f"Features shape: {X.shape}")
    logging.info(f"Target shape: {y.shape}")
    logging.info(f"Target distribution (0 = No Cover 2H AH {TARGET_AH_LINE}, 1 = Cover 2H AH {TARGET_AH_LINE}):\n{y.value_counts(normalize=True)}")

    # 3. Split Data into Training and Testing sets
    logging.info(f"Splitting data into training ({1-TEST_SIZE:.0%}) and testing ({TEST_SIZE:.0%})...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y
    )
    logging.info(f"Train set size: {len(X_train)}, Test set size: {len(X_test)}")

    # 4. Train LightGBM Model
    logging.info("Training LightGBM baseline model with odds features...")
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

    model = lgb.train(
        LGB_PARAMS,
        lgb_train,
        num_boost_round=500,
        valid_sets=[lgb_train, lgb_eval],
        valid_names=['train', 'eval'],
        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=True)]
    )
    logging.info("Model training completed.")

    model_filename = f"baseline_lgbm_model_{TARGET_COLUMN}_oddsfeat.txt" # Updated model filename
    try:
        model.save_model(model_filename)
        logging.info(f"Model saved to {model_filename}")
    except Exception as e:
        logging.error(f"Error saving model: {e}")


    # 5. Evaluate Model on the Test Set
    logging.info("Evaluating model on the unseen test set...")
    y_pred_proba = model.predict(X_test, num_iteration=model.best_iteration)
    y_pred = (y_pred_proba > 0.5).astype(int)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_pred_proba)
    logloss = log_loss(y_test, y_pred_proba)
    cm = confusion_matrix(y_test, y_pred)

    logging.info(f"--- Test Set Evaluation Metrics ({TARGET_COLUMN}, Incl. Odds Features) ---")
    logging.info(f"Accuracy:  {accuracy:.4f}")
    logging.info(f"Precision: {precision:.4f} (Of those predicted Cover 2H AH {TARGET_AH_LINE}, how many actually did?)")
    logging.info(f"Recall:    {recall:.4f} (Of those that actually Covered 2H AH {TARGET_AH_LINE}, how many were predicted?)")
    logging.info(f"AUC:       {auc:.4f}")
    logging.info(f"Log Loss:  {logloss:.4f}")
    logging.info("--------------------------------------------------------------------")

    plot_confusion_matrix(cm, classes=[f'No Cover 2H AH {TARGET_AH_LINE}', f'Cover 2H AH {TARGET_AH_LINE}'], title=f'Confusion Matrix ({TARGET_COLUMN}, Odds Feat)')


    # 6. Feature Importance
    logging.info("Calculating feature importance...")
    try:
        importance_df = pd.DataFrame({
            'feature': model.feature_name(),
            'importance': model.feature_importance(importance_type='gain'),
        }).sort_values('importance', ascending=False)

        logging.info("Top Feature Importances (Gain):")
        print(importance_df.head(len(FEATURE_COLUMNS)).to_string(index=False)) # Print all features

        plt.figure(figsize=(10, 8)) # Adjusted figure size for more features
        # Plot more features if available
        num_features_to_plot = min(len(FEATURE_COLUMNS), 20)
        sns.barplot(x="importance", y="feature", data=importance_df.head(num_features_to_plot))
        plt.title(f"LGBM Feature Importance (Gain) - Target: {TARGET_COLUMN}, Odds Feat")
        plt.tight_layout()
        importance_plot_filename = f"feature_importance_{TARGET_COLUMN}_oddsfeat.png" # Updated plot filename
        plt.savefig(importance_plot_filename)
        logging.info(f"Feature importance plot saved to {importance_plot_filename}")
        plt.close()

    except Exception as e:
        logging.warning(f"Could not calculate or plot feature importance: {e}")


    logging.info(f"=== Baseline Model Training Completed (Target: {TARGET_COLUMN}, Incl. Odds Features) ===")

if __name__ == "__main__":
    try:
        if 'PROCESSED_DATA_DIR' in locals() or 'PROCESSED_DATA_DIR' in globals():
             PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        else:
             Path(".").mkdir(parents=True, exist_ok=True)
             logging.warning("PROCESSED_DATA_DIR not defined, saving plots to current directory.")
    except NameError:
         Path(".").mkdir(parents=True, exist_ok=True)
         logging.warning("PROCESSED_DATA_DIR not defined due to import error, saving plots to current directory.")
    except Exception as e:
        logging.warning(f"Could not ensure output directory exists: {e}")

    train_baseline()
