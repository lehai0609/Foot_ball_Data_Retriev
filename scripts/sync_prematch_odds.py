#!/usr/bin/env python3
import os
import sys
import time
import json
import sqlite3 # Needed for DB operations
from pathlib import Path
from datetime import datetime
import logging

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.client import APIClient # Import APIClient directly
from src.data.processors import process_prematch_odds_data # Import the new odds processor
from src.data.storage import (
    get_db_connection,
    create_fixture_odds_table, # Import function to create the odds table
    store_data                 # Use the generic store_data function
)
from src.config import RAW_DATA_DIR # For saving raw data

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
ODDS_RAW_DIR = RAW_DATA_DIR / "prematch_odds" # Directory for raw odds responses
ODDS_RAW_DIR.mkdir(parents=True, exist_ok=True)
API_DELAY_SECONDS = 1.5 # Be slightly more conservative for odds endpoints
BATCH_SIZE = 200 # Process and store odds rows in batches
FIXTURE_LIMIT = None # Set to a number (e.g., 50) for testing, None to process all
ODDS_TABLE_NAME = "fixture_odds"

# --- Helper Functions ---
def get_finished_fixture_ids_for_odds(conn, limit=None):
    """
    Fetches fixture IDs from the schedules table where the status indicates finished,
    and optionally excludes fixtures already present in the fixture_odds table.
    """
    fixture_ids = []
    cursor = None
    # Define finished statuses based on STATE_ID_TO_STATUS mapping in processors
    finished_statuses = ('FT', 'FT_PEN', 'AET', 'AWD', 'ABD', 'CANC') # Add others if considered 'finished' for odds retrieval

    try:
        cursor = conn.cursor()
        # Select fixture_id from schedules where status is finished
        # AND the fixture_id does not already exist in fixture_odds table.
        # We check for existence of *any* odd row for that fixture_id.
        # Using LEFT JOIN and checking for NULL is generally efficient.
        query = f"""
            SELECT DISTINCT s.fixture_id
            FROM schedules s
            LEFT JOIN {ODDS_TABLE_NAME} fo ON s.fixture_id = fo.fixture_id
            WHERE s.status IN {finished_statuses} AND fo.fixture_id IS NULL
            ORDER BY s.start_time DESC -- Process more recent fixtures first
        """
        # Alternative: Fetch all finished and let INSERT OR IGNORE handle updates/duplicates
        # query = f"SELECT fixture_id FROM schedules WHERE status IN {finished_statuses} ORDER BY start_time DESC"

        if limit:
            query += f" LIMIT {int(limit)}"

        cursor.execute(query)
        rows = cursor.fetchall()
        fixture_ids = [row['fixture_id'] for row in rows]
        logging.info(f"Found {len(fixture_ids)} finished fixtures without odds data (Limit: {limit}).")

    except sqlite3.Error as e:
        logging.error(f"Error fetching fixture IDs for odds processing: {e}")
        if f"no such table: {ODDS_TABLE_NAME}" in str(e):
             logging.info(f"'{ODDS_TABLE_NAME}' table not found (will be created), fetching all finished fixtures.")
             try:
                 fallback_query = f"SELECT fixture_id FROM schedules WHERE status IN {finished_statuses} ORDER BY start_time DESC"
                 if limit: fallback_query += f" LIMIT {int(limit)}"
                 cursor.execute(fallback_query)
                 rows = cursor.fetchall()
                 fixture_ids = [row['fixture_id'] for row in rows]
                 logging.info(f"Found {len(fixture_ids)} finished fixture IDs (odds table not present, Limit: {limit}).")
             except sqlite3.Error as e2:
                 logging.error(f"Error executing fallback query for finished fixtures: {e2}")
        elif "no such table: schedules" in str(e):
            logging.error("Critical: 'schedules' table not found. Run sync_schedules.py first.")
            return [] # Return empty list
    finally:
        if cursor:
            cursor.close()
    return fixture_ids

def save_raw_odds_data(data, fixture_id):
    """Saves the raw pre-match odds JSON data for a fixture."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = ODDS_RAW_DIR / f"odds_{fixture_id}_{timestamp}.json"
    try:
        save_data = {
            "fetch_timestamp": datetime.now().isoformat(),
            "fixture_id": fixture_id,
            "prematch_odds_data": data # Store the full response
        }
        with open(file_path, "w") as f:
            json.dump(save_data, f, indent=2)
        logging.debug(f"Saved raw odds data for fixture {fixture_id} to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving raw odds data for fixture {fixture_id} to {file_path}: {e}")
        return None

# --- Main Workflow ---
def main(limit=FIXTURE_LIMIT): # Accept limit as argument
    """Fetches pre-match odds for finished fixtures, processes, and stores them."""
    logging.info("=== Starting Pre-Match Odds Sync Workflow ===")

    conn = get_db_connection()
    if not conn:
        logging.critical("Failed to connect to the database. Exiting.")
        sys.exit(1)

    processed_odds_batch = [] # Accumulate processed odds rows for batch insertion
    total_fixtures_processed = 0
    total_odds_rows_stored = 0
    fixtures_with_errors = []
    fixtures_with_no_odds = 0

    try:
        # 1. Ensure Fixture Odds Table Exists
        logging.info(f"Ensuring database table '{ODDS_TABLE_NAME}' exists...")
        create_fixture_odds_table(conn)
        # No trigger needed for this table unless specifically required

        # 2. Get Fixture IDs to Fetch (Applying the limit here)
        fixture_ids_to_process = get_finished_fixture_ids_for_odds(conn, limit=limit)
        if not fixture_ids_to_process:
            logging.info("No new finished fixture IDs found needing odds data. Exiting.")
            sys.exit(0)

        # 3. Initialize API Client
        client = APIClient()
        num_fixtures = len(fixture_ids_to_process)
        logging.info(f"Attempting to fetch pre-match odds for {num_fixtures} fixtures...")

        # 4. Loop Through Fixture IDs, Fetch, Process, Store in Batches
        for i, fixture_id in enumerate(fixture_ids_to_process):
            logging.info(f"\n--- Processing Fixture ID: {fixture_id} ({i+1}/{num_fixtures}) ---")
            # Construct endpoint for pre-match odds
            endpoint = f"v3/football/odds/pre-match/fixtures/{fixture_id}"
            try:
                # Fetch odds data
                logging.info(f"Fetching odds from: {endpoint}")
                raw_data = client.get(endpoint)

                if raw_data: # Check if response is not None
                    # Optional: Save raw data
                    save_raw_odds_data(raw_data, fixture_id)

                    # Process the raw data to extract odds rows
                    logging.info(f"Processing odds for fixture {fixture_id}...")
                    processed_odds_rows = process_prematch_odds_data(raw_data)

                    if processed_odds_rows:
                        processed_odds_batch.extend(processed_odds_rows) # Add rows to batch
                        logging.info(f"Successfully processed {len(processed_odds_rows)} odds rows for fixture {fixture_id}.")
                    else:
                        # This can happen if the API returns data but the list is empty
                        logging.info(f"No valid odds found or processed for fixture {fixture_id}.")
                        fixtures_with_no_odds += 1

                    # Mark fixture as processed regardless of whether odds were found (we attempted it)
                    total_fixtures_processed += 1

                else:
                    # Handle cases where client.get might return None due to repeated errors
                    logging.warning(f"No data returned from API client for fixture {fixture_id} (likely fetch error).")
                    fixtures_with_errors.append(fixture_id)

                # Store data in batches or at the end
                if len(processed_odds_batch) >= BATCH_SIZE or (i == num_fixtures - 1 and processed_odds_batch):
                    logging.info(f"\nStoring batch of {len(processed_odds_batch)} odds rows...")
                    # Use generic store_data with INSERT OR IGNORE semantics due to UNIQUE constraint
                    stored_count = store_data(conn, ODDS_TABLE_NAME, processed_odds_batch, primary_key_column="id", use_insert_ignore=True)
                    total_odds_rows_stored += stored_count
                    logging.info(f"Finished storing batch. Inserted: {stored_count}")
                    processed_odds_batch = [] # Reset batch

                # Optional delay
                if API_DELAY_SECONDS > 0 and i < num_fixtures - 1:
                    time.sleep(API_DELAY_SECONDS)

            except Exception as e:
                logging.error(f"Error processing fixture {fixture_id}: {e}", exc_info=True) # Log traceback
                fixtures_with_errors.append(fixture_id)
                # Decide if you want to continue or stop on error

        print("\n--- Sync Summary ---")
        logging.info(f"Fixture IDs targeted: {num_fixtures}")
        logging.info(f"Fixtures successfully processed (API call attempted): {total_fixtures_processed}")
        logging.info(f"Fixtures processed but had no odds data: {fixtures_with_no_odds}")
        if fixtures_with_errors:
            logging.warning(f"Fixtures with fetch/processing errors: {len(fixtures_with_errors)} -> {fixtures_with_errors}")
        logging.info(f"Total odds rows inserted into DB: {total_odds_rows_stored}")

    except Exception as e:
        logging.critical(f"An unexpected error occurred during the main workflow: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("\nDatabase connection closed.")

    logging.info("=== Pre-Match Odds Sync Workflow Completed ===")

if __name__ == "__main__":
    # You can override the limit via command line argument if needed,
    # otherwise it uses the FIXTURE_LIMIT constant.
    # Example: python scripts/sync_prematch_odds.py 100
    script_limit = None
    if len(sys.argv) > 1:
        try:
            script_limit = int(sys.argv[1])
            logging.info(f"Running with command-line limit: {script_limit}")
        except ValueError:
            logging.warning(f"Invalid command-line argument '{sys.argv[1]}'. Using default limit.")
            script_limit = FIXTURE_LIMIT
    else:
        script_limit = FIXTURE_LIMIT # Use the constant defined above

    main(limit=script_limit)
    # To run without limit: main(limit=None)
