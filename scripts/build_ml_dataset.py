#!/usr/bin/env python3
import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Add project root to Python path to allow importing from src
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    # Assuming execution from project root or src is in PYTHONPATH
    from src.config import PROCESSED_DATA_DIR, DATABASE_PATH
    from src.data.storage import get_db_connection
except ImportError:
    logging.error("Could not import config or storage modules. Make sure PYTHONPATH is set correctly or run from project root/scripts.")
    # Fallback paths for basic execution (adjust if needed)
    BASE_DIR = Path(__file__).resolve().parent.parent
    PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
    DATABASE_PATH = BASE_DIR / "data" / "database" / "football_data.db"
    # Minimal get_db_connection if import fails (not recommended for production)
    def get_db_connection():
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            conn.row_factory = sqlite3.Row
            logging.info(f"Connected to database (fallback): {DATABASE_PATH}")
            return conn
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database (fallback): {e}")
            return None

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- !!! UPDATED FILENAME !!! ---
OUTPUT_FILENAME = "ml_predictors_target_2H_AH0_5_odds_feat_dataset.csv" # Added odds_feat
TARGET_AH_LINE = -0.5 # The Asian Handicap line for the target

ODDS_MARKET_ID = 1 # Match Winner market ID
ODDS_BOOKMAKER_ID = 20 # Bookmaker ID for 1x2 odds


# List of half-time stats required for both home and away teams
HALF_TIME_STATS_COLS = [
    "goals", "fouls", "red_cards", "tackles", "shots_blocked",
    "successful_passes_percentage", "ball_possession", "saves",
    "attacks", "shots_total", "shots_insidebox"
]

def build_sql_query():
    """
    Constructs the SQL query to fetch data needed for 2H target calculation.
    Includes HT stats (esp. goals), final scores, and 1x2 odds.
    """
    select_clauses = [
        "s.fixture_id",
        "s.home_team_id",
        "s.away_team_id",
        "s.home_score",   # Final home score
        "s.away_score"    # Final away score
        ]

    # Add clauses for home team stats (MUST include 'goals')
    if 'goals' not in HALF_TIME_STATS_COLS:
        logging.warning("Adding 'goals' to HALF_TIME_STATS_COLS as it's required for 2H calculation.")
        HALF_TIME_STATS_COLS.insert(0, 'goals') # Ensure goals is present

    for stat in HALF_TIME_STATS_COLS:
        select_clauses.append(f"fs_home.{stat} AS ht_home_{stat}")

    # Add clauses for away team stats (MUST include 'goals')
    for stat in HALF_TIME_STATS_COLS:
        select_clauses.append(f"fs_away.{stat} AS ht_away_{stat}")

    # Add clauses for 1x2 odds values (Home, Draw, Away)
    select_clauses.append("fo_home.value AS odds_home")
    select_clauses.append("fo_draw.value AS odds_draw")
    select_clauses.append("fo_away.value AS odds_away")

    # Construct the SQL query with a CTE to identify valid fixtures first
    sql = f"""
    WITH ValidFixtures AS (
        -- Selects fixtures with 'first_half' stats for exactly two teams
        SELECT
            fixture_id
        FROM
            fixture_stats
        WHERE
            period = 'first_half'
        GROUP BY
            fixture_id
        HAVING
            COUNT(DISTINCT team_id) = 2
    )
    SELECT
        {', '.join(select_clauses)}
    FROM
        schedules s
    INNER JOIN
        ValidFixtures vf ON s.fixture_id = vf.fixture_id
    LEFT JOIN
        fixture_stats fs_home ON s.fixture_id = fs_home.fixture_id
                             AND s.home_team_id = fs_home.team_id
                             AND fs_home.period = 'first_half'
    LEFT JOIN
        fixture_stats fs_away ON s.fixture_id = fs_away.fixture_id
                             AND s.away_team_id = fs_away.team_id
                             AND fs_away.period = 'first_half'
    LEFT JOIN
        fixture_odds fo_home ON s.fixture_id = fo_home.fixture_id
                            AND fo_home.market_id = {ODDS_MARKET_ID}
                            AND fo_home.bookmaker_id = {ODDS_BOOKMAKER_ID}
                            AND fo_home.label = 'Home'
    LEFT JOIN
        fixture_odds fo_draw ON s.fixture_id = fo_draw.fixture_id
                            AND fo_draw.market_id = {ODDS_MARKET_ID}
                            AND fo_draw.bookmaker_id = {ODDS_BOOKMAKER_ID}
                            AND fo_draw.label = 'Draw'
    LEFT JOIN
        fixture_odds fo_away ON s.fixture_id = fo_away.fixture_id
                            AND fo_away.market_id = {ODDS_MARKET_ID}
                            AND fo_away.bookmaker_id = {ODDS_BOOKMAKER_ID}
                            AND fo_away.label = 'Away'
    WHERE
        s.status IN ('FT', 'AET', 'FT_PEN') -- Ensure fixture finished
        AND s.home_score IS NOT NULL        -- Need final scores
        AND s.away_score IS NOT NULL
        -- Need first half goals to calculate second half goals
        AND fs_home.goals IS NOT NULL
        AND fs_away.goals IS NOT NULL
    GROUP BY
        s.fixture_id
    ORDER BY
        s.fixture_id;
    """
    return sql

def calculate_odds_features_and_target(df, target_ah_line):
    """
    Calculates odds-based features (implied probabilities, fav/und odds)
    and the 2H target variable.

    Args:
        df (pd.DataFrame): DataFrame containing odds, final scores, HT scores, team IDs.
        target_ah_line (float): The AH line to check coverage for (e.g., -0.5).

    Returns:
        pd.DataFrame: DataFrame with added odds features and 'target_2H' column.
                      Returns None if input df is empty or required columns are missing.
    """
    if df.empty:
        logging.warning("Input DataFrame is empty in calculate_odds_features_and_target.")
        return None
    required_cols = [
        'odds_home', 'odds_draw', 'odds_away', 'home_score', 'away_score',
        'ht_home_goals', 'ht_away_goals', 'home_team_id', 'away_team_id'
    ]
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logging.error(f"Missing required columns for odds/target calculation. Need: {required_cols}. Missing: {missing}")
        return None

    # --- Coerce and Handle Missing Odds ---
    odds_cols = ['odds_home', 'odds_draw', 'odds_away']
    for col in odds_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows if essential 1x2 odds are missing (needed for favorite ID and features)
    initial_rows = len(df)
    df.dropna(subset=odds_cols, inplace=True)
    if len(df) < initial_rows:
        logging.warning(f"Dropped {initial_rows - len(df)} rows due to missing essential 1x2 odds.")

    if df.empty:
        logging.warning("DataFrame empty after dropping rows with missing 1x2 odds.")
        return None
    # --- End Handle Missing Odds ---

    # --- Calculate Odds Features ---
    logging.info("Calculating odds-based features...")
    # Implied Probabilities
    df['implied_prob_home'] = 1 / df['odds_home']
    df['implied_prob_draw'] = 1 / df['odds_draw']
    df['implied_prob_away'] = 1 / df['odds_away']
    # Probability Margin (Bookmaker's Edge)
    df['prob_margin'] = (df['implied_prob_home'] + df['implied_prob_draw'] + df['implied_prob_away']) - 1
    # Odds Ratio
    df['odds_ratio_hw'] = df['odds_home'] / df['odds_away'] # Home/Away ratio

    # Identify favorite
    conditions = [
        (df['odds_home'] <= df['odds_draw']) & (df['odds_home'] <= df['odds_away']), # Home Fav
        (df['odds_away'] < df['odds_draw']) & (df['odds_away'] < df['odds_home']),  # Away Fav
    ]
    choices_id = [df['home_team_id'], df['away_team_id']]
    choices_loc = ['home', 'away']
    df['favorite_team_id'] = np.select(conditions, choices_id, default=np.nan) # Should not be NaN due to dropna above
    df['favorite_location'] = np.select(conditions, choices_loc, default=None)

    # Favorite and Underdog Odds
    df['odds_fav'] = np.where(df['favorite_location'] == 'home', df['odds_home'], df['odds_away'])
    df['odds_und'] = np.where(df['favorite_location'] == 'home', df['odds_away'], df['odds_home'])
    logging.info("Finished calculating odds features.")
    # --- End Calculate Odds Features ---


    # --- Calculate Second Half Scores ---
    score_cols = ['home_score', 'away_score', 'ht_home_goals', 'ht_away_goals']
    for col in score_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    initial_rows = len(df)
    df.dropna(subset=score_cols, inplace=True) # Need scores for target
    if len(df) < initial_rows:
        logging.warning(f"Dropped {initial_rows - len(df)} rows due to missing score components needed for 2H target.")
    if df.empty:
        logging.warning("DataFrame empty after dropping rows with missing score components.")
        return None
    df['sh_home_goals'] = df['home_score'] - df['ht_home_goals']
    df['sh_away_goals'] = df['away_score'] - df['ht_away_goals']
    # --- End Calculate Second Half Scores ---


    # --- Calculate 2H Target ---
    target_col_name = f'target_fav_covers_ah_{str(target_ah_line).replace(".","_").replace("-","neg")}_2H'
    df[target_col_name] = np.nan # Initialize target column

    home_fav_covers_2H = (df['favorite_location'] == 'home') & \
                         (df['sh_home_goals'] > df['sh_away_goals']) # Home wins 2H covers AH -0.5

    away_fav_covers_2H = (df['favorite_location'] == 'away') & \
                         (df['sh_away_goals'] > df['sh_home_goals']) # Away wins 2H covers AH -0.5

    df.loc[home_fav_covers_2H, target_col_name] = 1
    df.loc[away_fav_covers_2H, target_col_name] = 1
    df.loc[df['favorite_location'].notna() & df[target_col_name].isna(), target_col_name] = 0 # Fill remaining with 0
    # --- End Calculate 2H Target ---

    # Log counts
    valid_targets = df[target_col_name].notna().sum()
    covers = int(df[target_col_name].sum())
    no_covers = valid_targets - covers

    logging.info(f"Target '{target_col_name}' calculation (2nd Half Only, AH {target_ah_line}):")
    logging.info(f" - Rows with valid target: {valid_targets}")
    logging.info(f" - Favorite covered AH {target_ah_line} (2H): {covers}")
    logging.info(f" - Favorite did not cover AH {target_ah_line} (2H): {no_covers}")

    # Drop rows where target is still NaN (shouldn't happen if favorite_location is notna)
    df.dropna(subset=[target_col_name], inplace=True)

    return df


def build_dataset():
    """Fetches data, processes, adds odds features, calculates 2H target, saves dataset."""
    logging.info(f"=== Starting ML Predictor Dataset Build (Incl. Odds Features, Target: 2H AH {TARGET_AH_LINE}) ===")

    conn = get_db_connection()
    if not conn:
        logging.critical("Failed to connect to the database. Exiting.")
        sys.exit(1)

    try:
        # 1. Construct and Execute SQL Query
        sql_query = build_sql_query()
        logging.info("Executing SQL query to fetch base data...")
        try:
            df = pd.read_sql_query(sql_query, conn)
            logging.info(f"Successfully fetched {len(df)} base rows.")
        except pd.io.sql.DatabaseError as e:
             logging.error(f"Database error executing query: {e}")
             sys.exit(1)
        except Exception as e:
             logging.error(f"An unexpected error occurred during query execution: {e}")
             sys.exit(1)

        if df.empty:
            logging.warning("Query returned no base data. Exiting.")
            sys.exit(0)

        # 2. Calculate Odds Features and 2H Target Variable
        logging.info(f"Calculating odds features and 2H target for AH {TARGET_AH_LINE}...")
        df = calculate_odds_features_and_target(df.copy(), TARGET_AH_LINE)
        target_col_name = f'target_fav_covers_ah_{str(TARGET_AH_LINE).replace(".","_").replace("-","neg")}_2H'

        if df is None or df.empty:
             logging.error("DataFrame became empty or None during odds/target calculation. Exiting.")
             sys.exit(1)

        # 3. Define Predictor Columns & Impute Missing Stats
        stats_cols_to_impute = []
        cols_for_features = [col for col in HALF_TIME_STATS_COLS if col != 'goals']
        for stat in cols_for_features:
            home_col = f"ht_home_{stat}"
            away_col = f"ht_away_{stat}"
            stats_cols_to_impute.extend([home_col, away_col])

        logging.info(f"Imputing missing values in stats columns ({len(stats_cols_to_impute)} columns) with 0...")
        missing_stats_before = df[stats_cols_to_impute].isnull().any(axis=1).sum()
        logging.info(f"Rows with at least one missing STAT value before imputation: {missing_stats_before}/{len(df)}")
        df[stats_cols_to_impute] = df[stats_cols_to_impute].fillna(0)
        logging.info("Missing stats values imputed with 0.")

        # --- Feature Engineering (Differentials including goal difference) ---
        logging.info("Calculating feature differentials (Favorite - Underdog)...")
        feature_cols_diffs = []
        for stat in cols_for_features: # Use stats excluding goals first
            home_stat_col = f"ht_home_{stat}"
            away_stat_col = f"ht_away_{stat}"
            diff_col = f"ht_diff_{stat}"
            df[diff_col] = np.where(
                df['favorite_location'] == 'home',
                df[home_stat_col] - df[away_stat_col],
                df[away_stat_col] - df[home_stat_col]
            )
            feature_cols_diffs.append(diff_col)
        # Add HT Goal Difference separately
        goal_diff_col = 'ht_diff_goals'
        df[goal_diff_col] = np.where(
                df['favorite_location'] == 'home',
                df['ht_home_goals'] - df['ht_away_goals'],
                df['ht_away_goals'] - df['ht_home_goals']
            )
        feature_cols_diffs.append(goal_diff_col)
        logging.info(f"Created {len(feature_cols_diffs)} differential features.")
        # --- End Feature Engineering ---

        # --- Define Final Feature Set (including odds features) ---
        odds_feature_cols = [
            'odds_home', 'odds_draw', 'odds_away', # Raw odds
            'implied_prob_home', 'implied_prob_draw', 'implied_prob_away', # Implied probabilities
            'prob_margin', # Bookmaker margin
            'odds_ratio_hw', # Home/Away odds ratio
            'odds_fav', 'odds_und' # Favorite/Underdog odds
            ]
        all_feature_cols = feature_cols_diffs + odds_feature_cols
        logging.info(f"Total features defined: {len(all_feature_cols)}")
        # --- End Define Final Feature Set ---

        # 4. Select Final Columns for Output
        final_output_cols = ['fixture_id'] + all_feature_cols + [target_col_name]
        # Ensure all selected columns actually exist in the dataframe
        final_output_cols = [col for col in final_output_cols if col in df.columns]
        missing_final_cols = [col for col in (['fixture_id'] + all_feature_cols + [target_col_name]) if col not in final_output_cols]
        if missing_final_cols:
            logging.warning(f"Columns expected but not found in final selection: {missing_final_cols}")

        final_df = df[final_output_cols].copy()

        final_rows = len(final_df)
        logging.info(f"Final dataset size: {final_rows} rows.")

        if final_df.empty:
            logging.warning("Final DataFrame is empty after feature engineering/selection.")
            sys.exit(0)

        # 5. Save Dataset
        output_path = PROCESSED_DATA_DIR / OUTPUT_FILENAME
        logging.info(f"Saving final predictor dataset (incl. odds features) to: {output_path}")
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False)
            logging.info("Dataset saved successfully.")
        except Exception as e:
            logging.error(f"Error saving dataset to CSV: {e}")

    except Exception as e:
        logging.critical(f"An unexpected error occurred during the dataset build process: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info(f"=== ML Predictor Dataset Build (Incl. Odds Features, Target: 2H AH {TARGET_AH_LINE}) Completed ===")

if __name__ == "__main__":
    try:
        if 'PROCESSED_DATA_DIR' in locals() or 'PROCESSED_DATA_DIR' in globals():
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        else:
            logging.warning("PROCESSED_DATA_DIR not defined, cannot ensure directory exists.")
    except NameError:
         logging.warning("PROCESSED_DATA_DIR not defined due to import error.")
    except Exception as e:
        logging.warning(f"Could not create processed data directory: {e}")

    build_dataset()
