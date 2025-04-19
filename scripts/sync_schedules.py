#!/usr/bin/env python3
import os
import sys
import time
import json
import sqlite3 # Import sqlite3 for error handling
from pathlib import Path
from datetime import datetime
import logging # Import logging

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.client import APIClient # Import APIClient directly
# Import the NEW detailed processor function
from src.data.processors import process_schedule_detailed
from src.data.storage import (
    get_db_connection,
    create_schedules_table, # Import function to create the enhanced schedules table
    create_update_trigger,  # Keep for the schedules table
    store_data              # Generic storage function
)
from src.config import RAW_DATA_DIR # For saving raw data

# Configure basic logging if not done elsewhere
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
SCHEDULES_RAW_DIR = RAW_DATA_DIR / "schedules" # Keep saving raw data here
SCHEDULES_RAW_DIR.mkdir(parents=True, exist_ok=True)
API_DELAY_SECONDS = 0.5 # Optional delay between API calls per season (adjust as needed)

# --- Helper Functions ---
def get_season_ids_from_db(conn):
    """Fetches season IDs from the seasons table to process."""
    season_ids = []
    cursor = None
    try:
        cursor = conn.cursor()
        # Adjust query as needed (e.g., only fetch current/unfinished seasons)
        # Fetching only current or unfinished might be more efficient
        cursor.execute("SELECT season_id FROM seasons WHERE finished = 0 OR is_current = 1 ORDER BY season_id;")
        # cursor.execute("SELECT season_id FROM seasons ORDER BY season_id;") # Or fetch all
        rows = cursor.fetchall()
        season_ids = [row['season_id'] for row in rows]
        logging.info(f"Found {len(season_ids)} season IDs in the database to process.")
    except sqlite3.Error as e:
        logging.error(f"Error fetching season IDs from database: {e}")
        if "no such table: seasons" in str(e):
            logging.error("Critical: 'seasons' table not found. Run sync_leagues.py first.")
            return [] # Return empty list to prevent proceeding
    finally:
        if cursor:
            cursor.close()
    return season_ids

def save_raw_schedule(data, season_id):
    """Saves the raw schedule JSON data."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = SCHEDULES_RAW_DIR / f"schedule_{season_id}_{timestamp}.json"
    try:
        save_data = {
            "fetch_timestamp": datetime.now().isoformat(),
            "season_id": season_id,
            "schedule_data": data
        }
        with open(file_path, "w") as f:
            json.dump(save_data, f, indent=2)
        logging.debug(f"Saved raw schedule for season {season_id} to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving raw schedule for season {season_id} to {file_path}: {e}")
        return None

# --- Main Workflow ---
def main():
    """Fetches schedules for each season, processes detailed fixture info, and stores them."""
    logging.info("=== Starting Enhanced Schedule Sync Workflow ===")
    schedules_table_name = "schedules"
    schedules_primary_key = "fixture_id" # Using fixture_id as the PK for this table

    conn = get_db_connection()
    if not conn:
        logging.critical("Failed to connect to the database. Exiting.")
        sys.exit(1)

    try:
        # 1. Ensure Schedules Table Exists (with the new structure)
        logging.info(f"Ensuring database table '{schedules_table_name}' exists...")
        create_schedules_table(conn) # Calls the function with the new schema
        create_update_trigger(conn, schedules_table_name, schedules_primary_key)

        # 2. Get Season IDs to Fetch
        season_ids = get_season_ids_from_db(conn)
        if not season_ids:
            logging.warning("No season IDs found in the database to process. Run sync_leagues.py first? Exiting.")
            sys.exit(0)

        # 3. Initialize API Client
        client = APIClient()
        total_schedule_entries_processed = 0
        total_schedule_entries_stored = 0
        seasons_processed_count = 0
        seasons_with_errors = []

        # 4. Loop Through Seasons, Fetch, Process, Store
        num_seasons = len(season_ids)
        for i, season_id in enumerate(season_ids):
            logging.info(f"\n--- Processing Season ID: {season_id} ({i+1}/{num_seasons}) ---")
            endpoint = f"v3/football/schedules/seasons/{season_id}"
            try:
                # Fetch schedule data for the current season
                logging.info(f"Fetching schedule from: {endpoint}")
                raw_data = client.get(endpoint) # Using APIClient directly

                if raw_data:
                    # Optional: Save raw data
                    save_raw_schedule(raw_data, season_id)

                    # Process the raw data using the NEW detailed processor
                    logging.info(f"Processing detailed schedule data for season {season_id}...")
                    # *** USE THE NEW PROCESSOR HERE ***
                    processed_schedule_entries = process_schedule_detailed(raw_data)
                    entries_count = len(processed_schedule_entries)
                    total_schedule_entries_processed += entries_count
                    logging.info(f"Found {entries_count} detailed fixture entries for season {season_id}.")

                    # Store the processed entries
                    if processed_schedule_entries:
                        logging.info(f"Storing {entries_count} schedule entries into '{schedules_table_name}'...")
                        # Use generic store_data with fixture_id as PK (will replace if fixture appears again)
                        stored_count = store_data(conn, schedules_table_name, processed_schedule_entries, schedules_primary_key)
                        total_schedule_entries_stored += stored_count
                        logging.info(f"Finished storing schedule entries for season {season_id}. Stored/Updated: {stored_count}")
                    else:
                        logging.info(f"No valid schedule entries processed for season {season_id}.")
                else:
                    logging.warning(f"No data returned from API for season {season_id}.")

                seasons_processed_count += 1
                # Optional delay
                if API_DELAY_SECONDS > 0 and i < num_seasons - 1:
                    logging.debug(f"Waiting {API_DELAY_SECONDS}s before next season...")
                    time.sleep(API_DELAY_SECONDS)

            except Exception as e:
                logging.error(f"Error processing season {season_id}: {e}", exc_info=True) # Log traceback
                seasons_with_errors.append(season_id)

        logging.info("\n--- Sync Summary ---")
        logging.info(f"Seasons processed: {seasons_processed_count}/{num_seasons}")
        if seasons_with_errors:
            logging.warning(f"Seasons with errors: {len(seasons_with_errors)} -> {seasons_with_errors}")
        logging.info(f"Total detailed schedule entries processed: {total_schedule_entries_processed}")
        logging.info(f"Total schedule entries stored/updated in DB: {total_schedule_entries_stored}")

    except Exception as e:
        logging.critical(f"An unexpected error occurred during the main workflow: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("\nDatabase connection closed.")

    logging.info("=== Enhanced Schedule Sync Workflow Completed ===")

if __name__ == "__main__":
    # Setup logging (optional, if not configured globally)
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
