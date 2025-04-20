#!/usr/bin/env python3
import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Add project root to Python path to allow importing from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    # Assuming execution from project root or src is in PYTHONPATH
    from src.config import PROCESSED_DATA_DIR, DATABASE_PATH
    from src.data.storage import get_db_connection
except ImportError:
    logging.error("Could not import config or storage modules. Make sure PYTHONPATH is set correctly or run from project root.")
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

OUTPUT_FILENAME = "ml_predictors_dataset.csv"
ODDS_MARKET_ID = 1
ODDS_BOOKMAKER_ID = 9 # Specify the desired bookmaker ID

# List of half-time stats required for both home and away teams
HALF_TIME_STATS_COLS = [
    "goals",
    "fouls",
    "red_cards",
    "tackles",
    "shots_blocked", # Renamed from shot_blocked
    "successful_passes_percentage",
    "ball_possession",
    "saves",
    "attacks", # Renamed from attack
    "shots_total",
    "shots_insidebox"
]

def build_sql_query():
    """Constructs the SQL query to fetch and join data."""

    select_clauses = ["s.fixture_id"]

    # Add clauses for home team stats
    for stat in HALF_TIME_STATS_COLS:
        select_clauses.append(f"fs_home.{stat} AS ht_home_{stat}")

    # Add clauses for away team stats
    for stat in HALF_TIME_STATS_COLS:
        select_clauses.append(f"fs_away.{stat} AS ht_away_{stat}")

    # Add clauses for odds values
    select_clauses.append("fo_home.value AS home_odds_value")
    select_clauses.append("fo_away.value AS away_odds_value")

    sql = f"""
    SELECT
        {', '.join(select_clauses)}
    FROM
        schedules s
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
        fixture_odds fo_away ON s.fixture_id = fo_away.fixture_id
                            AND fo_away.market_id = {ODDS_MARKET_ID}
                            AND fo_away.bookmaker_id = {ODDS_BOOKMAKER_ID}
                            AND fo_away.label = 'Away'
    WHERE
        -- Optional: Filter for specific fixtures, e.g., finished ones
        -- s.status IN ('FT', 'AET', 'FT_PEN')
        -- Add any other fixture filtering conditions here if needed
        s.fixture_id IS NOT NULL -- Basic check
    GROUP BY -- Ensure one row per fixture if multiple odds entries exist (takes first one)
        s.fixture_id
    ORDER BY
        s.fixture_id;
    """
    return sql

def build_dataset():
    """Fetches data, processes it, and saves the predictor dataset."""
    logging.info("=== Starting ML Predictor Dataset Build ===")

    conn = get_db_connection()
    if not conn:
        logging.critical("Failed to connect to the database. Exiting.")
        sys.exit(1)

    try:
        # 1. Construct and Execute SQL Query
        sql_query = build_sql_query()
        logging.info("Executing SQL query to fetch data...")
        # logging.debug(f"SQL Query:\n{sql_query}") # Uncomment to debug the query
        try:
            df = pd.read_sql_query(sql_query, conn)
            logging.info(f"Successfully fetched {len(df)} raw rows from the database.")
        except pd.io.sql.DatabaseError as e:
             logging.error(f"Database error executing query: {e}")
             logging.error("Potential issues: Missing tables (run sync scripts?), incorrect column names.")
             # Check if tables exist
             cursor = conn.cursor()
             tables = ['schedules', 'fixture_stats', 'fixture_odds']
             for table in tables:
                 try:
                      cursor.execute(f"SELECT 1 FROM {table} LIMIT 1;")
                 except sqlite3.OperationalError:
                      logging.error(f"Table '{table}' does not seem to exist. Please run the corresponding sync script.")
             cursor.close()
             sys.exit(1)
        except Exception as e:
             logging.error(f"An unexpected error occurred during query execution: {e}")
             sys.exit(1)


        if df.empty:
            logging.warning("Query returned no data. Check database content and query filters. Exiting.")
            sys.exit(0)

        # 2. Calculate Odds Ratio
        logging.info("Calculating odds ratio...")
        # Ensure odds columns are numeric, coercing errors to NaN
        df['home_odds_value'] = pd.to_numeric(df['home_odds_value'], errors='coerce')
        df['away_odds_value'] = pd.to_numeric(df['away_odds_value'], errors='coerce')

        # Calculate ratio, handle division by zero or NaN in denominator
        df['odds_ratio'] = df['home_odds_value'] / df['away_odds_value'].replace(0, np.nan)
        logging.info(f"Calculated odds_ratio. Found {df['odds_ratio'].isnull().sum()} rows with missing odds_ratio (due to missing odds or away_odds=0).")

        # 3. Define Final Predictor Columns
        predictor_cols = ['odds_ratio']
        for stat in HALF_TIME_STATS_COLS:
            predictor_cols.append(f"ht_home_{stat}")
        for stat in HALF_TIME_STATS_COLS:
            predictor_cols.append(f"ht_away_{stat}")

        # 4. Handle Missing Data
        logging.info("Checking for missing values in predictor columns...")
        initial_rows = len(df)
        # Check for NaNs in the essential predictor columns
        missing_before = df[predictor_cols].isnull().any(axis=1).sum()
        logging.info(f"Rows with at least one missing predictor before cleaning: {missing_before}/{initial_rows}")

        # Strategy: Drop rows where any of the predictor columns are NaN
        # Alternative strategies: Imputation (mean, median, constant, model-based)
        df_cleaned = df.dropna(subset=predictor_cols)
        final_rows = len(df_cleaned)
        rows_dropped = initial_rows - final_rows
        logging.info(f"Removed {rows_dropped} rows due to missing predictors.")
        logging.info(f"Final dataset size: {final_rows} rows.")

        if df_cleaned.empty:
            logging.warning("No rows remaining after handling missing data. Cannot create dataset.")
            sys.exit(0)

        # 5. Select Final Columns for Output
        output_cols = ['fixture_id'] + predictor_cols
        final_df = df_cleaned[output_cols].copy() # Use copy to avoid SettingWithCopyWarning

        # 6. Save Dataset
        output_path = PROCESSED_DATA_DIR / OUTPUT_FILENAME
        logging.info(f"Saving final predictor dataset to: {output_path}")
        try:
            # Ensure the output directory exists
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

    logging.info("=== ML Predictor Dataset Build Completed ===")

if __name__ == "__main__":
    # Ensure the PROCESSED_DATA_DIR exists before starting
    try:
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    except NameError:
         # Handle case where import failed earlier
         pass
    except Exception as e:
        logging.warning(f"Could not create processed data directory: {e}")

    build_dataset()