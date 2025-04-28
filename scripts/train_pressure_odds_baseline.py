import argparse
import logging
import os
import sys
import re
import pandas as pd
import numpy as np
import lightgbm as lgb # Import LightGBM
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
# Removed LogisticRegression import
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
# from joblib import dump # Uncomment if you want to save the model

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Add project root to Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

# --- Configuration ---
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "data", "database", "football_data.db")
DATABASE_PATH = os.getenv("DATABASE_PATH", DEFAULT_DB_PATH)
DB_URI = f"sqlite:///{DATABASE_PATH}"

ODDS_MARKET_ID = 1  # Match Winner market
ODDS_BOOKMAKER_ID = 20 # Updated Bookmaker ID
STATS_TABLE = "fixture_stats" # Added constant for stats table

SCHEDULES_TABLE = "schedules"
ODDS_TABLE = "fixture_odds"
PRESSURE_TABLE = "fixture_timeline"
FINISHED_MATCH_STATUS = 'FT'

# --- Database Connection ---
try:
    engine = create_engine(DB_URI)
    logging.info(f"Successfully connected to the database: {DB_URI}")
except Exception as e:
    logging.error(f"Failed to connect to the database: {e}")
    sys.exit(1)


# Removed parse_score function as HT scores come from stats


def calculate_target(row):
    """
    Calculates the binary target: 1 if favorite covers -0.5 AH in 2nd half, 0 otherwise.
    Uses ht_home_goals, ht_away_goals (from stats) and ft_home_score, ft_away_score (from schedules).
    Returns None if calculation is not possible.
    """
    # Check for required columns (updated names)
    required = ['ht_home_goals', 'ht_away_goals', 'ft_home_score', 'ft_away_score', 'favorite']
    if any(pd.isna(row[col]) for col in required):
        logging.debug(f"Skipping target calculation for fixture {row.get('fixture_id', 'N/A')} due to missing required data: { {k: row.get(k) for k in required} }")
        return None
    if row['favorite'] == 'Draw': # Exclude cases with no clear favorite
        logging.debug(f"Skipping target calculation for fixture {row.get('fixture_id', 'N/A')} due to no clear favorite (odds equal).")
        return None

    # Ensure scores are numeric before calculation
    try:
        ht_home = int(row['ht_home_goals'])
        ht_away = int(row['ht_away_goals'])
        ft_home = int(row['ft_home_score'])
        ft_away = int(row['ft_away_score'])
    except (ValueError, TypeError) as e:
         logging.warning(f"Could not convert scores to int for fixture {row.get('fixture_id', 'N/A')}. Error: {e}. Scores: HT {row['ht_home_goals']}-{row['ht_away_goals']}, FT {row['ft_home_score']}-{row['ft_away_score']}")
         return None


    # Calculate second half scores
    sh_home_score = ft_home - ht_home
    sh_away_score = ft_away - ht_away

    # Check if favorite covered the handicap
    if row['favorite'] == 'Home':
        if (sh_home_score - sh_away_score) > 0.5:
            return 1
        else:
            return 0
    elif row['favorite'] == 'Away':
        if (sh_away_score - sh_home_score) > 0.5:
            return 1
        else:
            return 0
    else:
        return None # Should not happen


def build_dataset(conn):
    """
    Builds a dataset for predicting if the favorite covers -0.5 AH in the 2nd half,
    using HT goals, fouls, tackles, shots from fixture_stats and LightGBM model preparation.

    Args:
        conn: SQLAlchemy connection object.

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: Features (X).
            - pd.Series: Target variable (y).
            - list: List of fixture IDs included in the dataset.
            - dict: Class mapping {0: 'Did not cover', 1: 'Covered'}.
    """
    logging.info("Starting dataset construction for 2H AH -0.5 target (Stats HT)...")

    # 1. Get fixture IDs with pressure data
    try:
        pressure_query = f"SELECT DISTINCT fixture_id FROM {PRESSURE_TABLE};"
        fixture_ids_with_pressure = pd.read_sql(
            text(pressure_query), conn
        )["fixture_id"].tolist()
        logging.info(
            f"Found {len(fixture_ids_with_pressure)} fixtures with pressure data."
        )
        if not fixture_ids_with_pressure:
            logging.warning("No fixtures found with pressure data. Exiting.")
            return pd.DataFrame(), pd.Series(), [], {}
        fixture_ids_placeholder = ",".join(map(str, map(int, fixture_ids_with_pressure)))
    except Exception as e:
        logging.error(f"Error fetching fixture IDs from {PRESSURE_TABLE}: {e}")
        return pd.DataFrame(), pd.Series(), [], {}

    # 2. Get Prematch Odds (Using updated Bookmaker ID)
    try:
        odds_query = f"""
            SELECT
                fixture_id,
                label,
                value as odd_value
            FROM {ODDS_TABLE}
            WHERE
                fixture_id IN ({fixture_ids_placeholder})
                AND market_id = {ODDS_MARKET_ID}
                AND bookmaker_id = {ODDS_BOOKMAKER_ID} -- Updated ID
                AND label IN ('Home', 'Draw', 'Away');
        """
        odds_df = pd.read_sql(text(odds_query), conn)
        logging.info(
            f"Fetched odds data (Bookmaker {ODDS_BOOKMAKER_ID}) for {odds_df['fixture_id'].nunique()} fixtures."
        )
        if odds_df.empty:
             logging.warning(f"No odds data found in '{ODDS_TABLE}' for Bookmaker {ODDS_BOOKMAKER_ID}.")
             return pd.DataFrame(), pd.Series(), [], {}

        odds_pivot = odds_df.pivot_table(
            index="fixture_id", columns="label", values="odd_value"
        ).reset_index()
        odds_pivot.columns.name = None
        for col in ['Home', 'Away', 'Draw']:
            if col in odds_pivot.columns:
                odds_pivot[col] = pd.to_numeric(odds_pivot[col], errors='coerce')
            else:
                 odds_pivot[col] = np.nan
        odds_pivot = odds_pivot.dropna(subset=['Home', 'Away'])
        logging.info(f"Pivoted odds data shape: {odds_pivot.shape}")

    except Exception as e:
        logging.error(f"Error fetching or processing odds data: {e}")
        return pd.DataFrame(), pd.Series(), [], {}

    # 3. Get FT Scores and Team IDs from Schedules table
    try:
        schedules_query = f"""
            SELECT
                fixture_id,
                home_team_id,
                away_team_id,
                home_score AS ft_home_score, -- Rename for clarity
                away_score AS ft_away_score, -- Rename for clarity
                status
            FROM {SCHEDULES_TABLE}
            WHERE
                fixture_id IN ({fixture_ids_placeholder})
                AND status = '{FINISHED_MATCH_STATUS}';
        """
        schedules_df = pd.read_sql(text(schedules_query), conn)
        # Convert FT scores to numeric early
        schedules_df['ft_home_score'] = pd.to_numeric(schedules_df['ft_home_score'], errors='coerce')
        schedules_df['ft_away_score'] = pd.to_numeric(schedules_df['ft_away_score'], errors='coerce')
        schedules_df = schedules_df.dropna(subset=['ft_home_score', 'ft_away_score'])
        schedules_df[['ft_home_score', 'ft_away_score']] = schedules_df[['ft_home_score', 'ft_away_score']].astype(int)

        logging.info(
            f"Fetched FT scores & team IDs for {schedules_df['fixture_id'].nunique()} finished fixtures."
        )

    except Exception as e:
        logging.error(f"Error fetching schedule/FT score data: {e}")
        return pd.DataFrame(), pd.Series(), [], {}

    # 4. Get HT Stats (Goals, Fouls, Tackles, Shots Total) from Fixture Stats table
    try:
        # Added fouls, tackles, shots_total to the query
        stats_query = f"""
            SELECT
                fixture_id,
                team_id,
                CAST(goals AS INTEGER) AS goals,
                CAST(fouls AS INTEGER) AS fouls,
                CAST(tackles AS INTEGER) AS tackles,
                CAST(shots_total AS INTEGER) AS shots_total
            FROM {STATS_TABLE}
            WHERE
                fixture_id IN ({fixture_ids_placeholder})
                AND period = 'first_half';
        """
        stats_df = pd.read_sql(text(stats_query), conn)
        logging.info(
            f"Fetched {len(stats_df)} stats records for 'first_half' (goals, fouls, tackles, shots_total)."
        )

        # Define stats columns to process
        stats_cols = ['goals', 'fouls', 'tackles', 'shots_total']

        # Check if stats_df is empty or if essential columns are missing
        if stats_df.empty:
            logging.warning(f"No stats records found for 'first_half' in {STATS_TABLE} for the relevant fixtures.")
            # Create empty df with expected columns for merging
            ht_stats_agg = pd.DataFrame(columns=['fixture_id', 'team_id'] + stats_cols)
        else:
            # Ensure all expected stat columns exist and are numeric, fillna before grouping
            for col in stats_cols:
                 if col not in stats_df.columns:
                      logging.warning(f"Stats column '{col}' not found in query result. Adding as 0.")
                      stats_df[col] = 0
                 else:
                      stats_df[col] = pd.to_numeric(stats_df[col], errors='coerce').fillna(0)

            # Aggregate stats per team per fixture for the first half
            group_cols = ['fixture_id', 'team_id']
            agg_funcs = {col: 'sum' for col in stats_cols}
            ht_stats_agg = stats_df.groupby(group_cols).agg(agg_funcs).reset_index()
            logging.info(f"Aggregated first half stats for {ht_stats_agg['fixture_id'].nunique()} fixtures.")

        # Merge with schedules to map team stats to home/away HT stats
        # Merge home team stats
        schedules_with_ht_stats = pd.merge(
            schedules_df,
            ht_stats_agg.rename(columns={col: f'ht_home_{col}' for col in stats_cols}),
            left_on=['fixture_id', 'home_team_id'],
            right_on=['fixture_id', 'team_id'],
            how='left' # Use left join to keep all schedules rows
        ).drop(columns=['team_id'], errors='ignore')

        # Merge away team stats
        schedules_with_ht_stats = pd.merge(
            schedules_with_ht_stats,
            ht_stats_agg.rename(columns={col: f'ht_away_{col}' for col in stats_cols}),
            left_on=['fixture_id', 'away_team_id'],
            right_on=['fixture_id', 'team_id'],
            how='left' # Use left join
        ).drop(columns=['team_id'], errors='ignore')

        # Fill NaN HT stats with 0 (if a team had no stats record)
        for col in stats_cols:
            schedules_with_ht_stats[f'ht_home_{col}'] = schedules_with_ht_stats[f'ht_home_{col}'].fillna(0).astype(int)
            schedules_with_ht_stats[f'ht_away_{col}'] = schedules_with_ht_stats[f'ht_away_{col}'].fillna(0).astype(int)

        logging.info(f"Mapped stats to ht_home/away columns.")

        # Keep only necessary columns for merging later
        # Include the raw HT stats needed for calculating differences
        cols_to_keep = ['fixture_id', 'ft_home_score', 'ft_away_score']
        for col in stats_cols:
            cols_to_keep.extend([f'ht_home_{col}', f'ht_away_{col}'])
        scores_stats_teams_df = schedules_with_ht_stats[cols_to_keep]


    except Exception as e:
        # Log the specific error
        logging.error(f"Error fetching or processing fixture stats for HT: {e}")
        return pd.DataFrame(), pd.Series(), [], {}


    # 5. Get Average Pressure Index for the First Half
    try:
        pressure_index_query = f"""
            SELECT
                fixture_id,
                AVG(pressure_index) as avg_pressure_index_1st_half
            FROM {PRESSURE_TABLE}
            WHERE
                fixture_id IN ({fixture_ids_placeholder})
                AND minute <= 45
            GROUP BY fixture_id;
        """
        pressure_index_df = pd.read_sql(text(pressure_index_query), conn)
        logging.info(
            f"Fetched and averaged 1st half pressure index for {pressure_index_df['fixture_id'].nunique()} fixtures."
        )
        if pressure_index_df.empty:
            logging.warning("No first half pressure data found.")
            # Return empty as pressure is a required feature
            return pd.DataFrame(), pd.Series(), [], {}

    except Exception as e:
        logging.error(f"Error fetching 1st half pressure index data: {e}")
        return pd.DataFrame(), pd.Series(), [], {}

    # 6. Merge DataFrames
    logging.info("Merging datasets...")
    try:
        # Start with fixtures that have valid odds
        final_df = odds_pivot

        # Merge 1st half pressure index (inner join ensures only fixtures with pressure are kept)
        final_df = pd.merge(
            final_df, pressure_index_df, on="fixture_id", how="inner"
        )
        logging.info(f"Shape after merging pressure index: {final_df.shape}")

        # Merge derived scores and HT stats
        final_df = pd.merge(
            final_df, scores_stats_teams_df, on="fixture_id", how="inner"
        )
        logging.info(f"Shape after merging scores & stats: {final_df.shape}")

    except Exception as e:
        logging.error(f"Error during merging: {e}")
        return pd.DataFrame(), pd.Series(), [], {}

    # 7. Feature Engineering & Target Definition
    logging.info("Performing feature engineering and target definition...")

    # Identify Favorite
    def find_favorite(row):
        home_odds = row['Home']
        away_odds = row['Away']
        if pd.isna(home_odds) or pd.isna(away_odds): return 'Draw' # Treat missing as Draw/Exclude
        if home_odds == away_odds: return 'Draw'
        elif home_odds < away_odds: return 'Home'
        else: return 'Away'
    final_df['favorite'] = final_df.apply(find_favorite, axis=1)

    # Calculate Odds Ratio
    def calculate_odds_ratio(row):
        home_odds = row['Home']
        away_odds = row['Away']
        if not all(isinstance(x, (int, float)) and x > 0 for x in [home_odds, away_odds]):
             return np.nan
        if row['favorite'] == 'Home': return home_odds / away_odds
        elif row['favorite'] == 'Away': return away_odds / home_odds
        else: return 1.0
    final_df['odds_ratio'] = final_df.apply(calculate_odds_ratio, axis=1)

    # Calculate HT Stat Differences (Home - Away)
    for col in stats_cols: # ['goals', 'fouls', 'tackles', 'shots_total']
        final_df[f'ht_{col}_diff'] = final_df[f'ht_home_{col}'] - final_df[f'ht_away_{col}']
    logging.info("Calculated HT stat differences (home - away).")

    # Calculate Target Variable using the updated function
    # Need ht_home_goals and ht_away_goals for this function
    final_df['target'] = final_df.apply(calculate_target, axis=1)

    # Filter out rows: no target, or missing features
    # Define features to be used in the model
    features = [
        'odds_ratio',
        'avg_pressure_index_1st_half',
        'ht_goals_diff',
        'ht_fouls_diff',
        'ht_tackles_diff',
        'ht_shots_total_diff'
    ]
    # Check for NaNs in target and all feature columns
    final_df = final_df.dropna(subset=['target'] + features)

    logging.info(f"Calculated target and filtered NaNs/Draws. Final shape: {final_df.shape}")


    if final_df.empty:
        logging.warning("DataFrame is empty after final filtering. Cannot proceed.")
        return pd.DataFrame(), pd.Series(), [], {}

    # Convert target to integer
    final_df['target'] = final_df['target'].astype(int)

    # 8. Select Features (X) and Target (y)
    # Features list already defined above
    target_col = 'target'

    X = final_df[features]
    y = final_df[target_col]
    fixture_ids_used = final_df['fixture_id'].tolist()

    class_mapping = {0: 'Did not cover', 1: 'Covered'}
    logging.info(f"Class Mapping: {class_mapping}")

    logging.info(f"Dataset construction complete. Features shape: {X.shape}, Target shape: {y.shape}")
    logging.info(f"Features used: {features}")
    logging.info(f"Target variable: {target_col} (1 if Fav covers 2H -0.5 AH, 0 otherwise)")
    logging.info(f"Number of fixtures in final dataset: {len(fixture_ids_used)}")

    return X, y, fixture_ids_used, class_mapping


def train_evaluate_model(X, y, class_mapping):
    """
    Trains and evaluates a LightGBM model for binary classification.

    Args:
        X (pd.DataFrame): Features.
        y (pd.Series): Target variable (binary).
        class_mapping (dict): Mapping from class code (0/1) to label name.
    """
    if X.empty or y.empty:
        logging.warning("Input data is empty. Skipping model training.")
        return
    if not class_mapping:
         logging.warning("Class mapping is empty. Cannot proceed with evaluation.")
         return
    if y.nunique() < 2:
        logging.warning(f"Target variable has only {y.nunique()} unique value(s). Cannot train/evaluate.")
        return

    logging.info("Splitting data into training and validation sets...")
    try:
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        logging.info(f"Train set size: {X_train.shape[0]}, Validation set size: {X_val.shape[0]}")
        logging.info(f"Train target distribution:\n{y_train.value_counts(normalize=True)}")
        logging.info(f"Validation target distribution:\n{y_val.value_counts(normalize=True)}")

    except ValueError as e:
         logging.error(f"Error during train/test split: {e}")
         logging.warning("Skipping model training due to split error.")
         return

    logging.info("Training LightGBM model...")
    # Basic LightGBM parameters - adjust as needed or based on train_baseline_model.py
    model = lgb.LGBMClassifier(
        objective='binary', # Explicitly set for binary classification
        random_state=42,
        n_estimators=100, # Default, adjust as needed
        learning_rate=0.1, # Default, adjust as needed
        # Add other parameters like num_leaves, reg_alpha, reg_lambda if desired
        n_jobs=-1 # Use all available CPU cores
        )

    # Log the features being used by the model
    logging.info(f"Training model with features: {X_train.columns.tolist()}")

    model.fit(X_train, y_train)
    logging.info("Model training complete.")

    logging.info("Evaluating model on validation set...")
    y_pred = model.predict(X_val)
    y_pred_proba = model.predict_proba(X_val)[:, 1]

    accuracy = accuracy_score(y_val, y_pred)
    target_names = [class_mapping[i] for i in sorted(class_mapping.keys())]
    report = classification_report(y_val, y_pred, target_names=target_names, zero_division=0)
    cm = confusion_matrix(y_val, y_pred)

    logging.info(f"\n--- Validation Metrics (LightGBM with added features) ---")
    logging.info(f"Class Mapping used for report: {class_mapping}")
    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info(f"Confusion Matrix:\n{cm}")
    logging.info(f"(Rows: Actual, Columns: Predicted)")
    logging.info(f"Classification Report:\n{report}")
    logging.info("----------------------------------------------------------")

    # Optionally save the model
    # model_filename = "pressure_odds_stats_2h_ah_lgbm_baseline_model.joblib"
    # model_path = os.path.join(PROJECT_ROOT, "models", model_filename)
    # os.makedirs(os.path.dirname(model_path), exist_ok=True)
    # dump(model, model_path) # Use joblib or lgbm's save_model
    # # model.booster_.save_model(model_path + '.txt') # Example using lgbm save
    # logging.info(f"Model saved logic placeholder.")


def main():
    """
    Main function to orchestrate dataset building and model training.
    """
    parser = argparse.ArgumentParser(
        description="Build dataset and train LightGBM baseline model (with added stats) for 2nd Half -0.5 AH prediction."
    )
    args = parser.parse_args()

    with engine.connect() as connection:
        X, y, fixture_ids, class_mapping = build_dataset(connection)

        if not X.empty and not y.empty:
             train_evaluate_model(X, y, class_mapping)
        else:
            logging.warning("Dataset construction failed or yielded empty data. Model training skipped.")

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
