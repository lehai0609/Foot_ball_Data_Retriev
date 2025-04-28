# scripts/sync_pressure.py
import os
import sys
import time
import json
import sqlite3 # Needed for DB operations
from pathlib import Path
from datetime import datetime
import logging

# Add project root to Python path
# Assuming this script is in the 'scripts' directory, the parent is the project root
project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root_path not in sys.path:
    sys.path.insert(0, project_root_path)

# --- Imports from src ---
try:
    from src.api.client import APIClient # Import APIClient directly
    from src.data.processors import process_pressure_data # Import the new pressure processor
    from src.data.storage import (
        get_db_connection,
        create_fixture_timeline_table, # Import function to create the timeline table
        store_data                 # Use the generic store_data function
    )
    from src.config import RAW_DATA_DIR # For saving raw data
except ImportError as e:
     # Basic logging setup if config/imports fail early
     logging.basicConfig(level=logging.ERROR)
     logging.critical(f"Failed to import necessary modules: {e}. Ensure PYTHONPATH is correct or run from project root.", exc_info=True)
     # Attempt relative imports as a last resort if structure is consistent
     try:
         # Assuming the script is run from the 'scripts' directory
         # Adjust relative path if necessary
         sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
         from api.client import APIClient
         from data.processors import process_pressure_data
         from data.storage import get_db_connection, create_fixture_timeline_table, store_data
         # config might be directly accessible if src is in path, try importing directly first
         try:
             from config import RAW_DATA_DIR
         except ImportError:
             # Fallback if config is not directly accessible
             # This assumes src is a package and the script is run from scripts/
             from ..src.config import RAW_DATA_DIR

         logging.warning("Used adjusted imports as fallback.")
     except ImportError as e_rel:
        # If relative imports also fail, exit
        logging.critical(f"Relative imports also failed: {e_rel}. Cannot proceed.")
        sys.exit(1)


# Configure basic logging
# Make sure level is INFO to see progress messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')

# --- Configuration ---
# Ensure RAW_DATA_DIR is a Path object if imported directly
if 'RAW_DATA_DIR' not in locals() or not isinstance(RAW_DATA_DIR, Path):
     # Fallback if RAW_DATA_DIR wasn't imported correctly
     logging.warning("RAW_DATA_DIR not found or not a Path object, using fallback relative path.")
     BASE_DIR = Path(project_root_path) # Use the calculated project root
     RAW_DATA_DIR = BASE_DIR / "data" / "raw"


PRESSURE_RAW_DIR = RAW_DATA_DIR / "fixture_pressure" # Directory for raw pressure responses
PRESSURE_RAW_DIR.mkdir(parents=True, exist_ok=True)
# --- Updated Constants ---
API_DELAY_SECONDS = 1.5 # Increased delay to respect rate limits (3000/hr ~ 1.2s/req)
BATCH_SIZE = 500 # Process and store pressure rows in batches
FIXTURE_LIMIT = None # Limit the number of fixtures processed for testing
# --- End Updated Constants ---
TIMELINE_TABLE_NAME = "fixture_timeline"

# --- Helper Functions ---
def get_fixture_ids_for_pressure(conn, limit=None):
    """
    Fetches fixture IDs from the schedules table suitable for pressure index retrieval.
    Criteria: Finished status ('FT', 'AET', 'FT_PEN') AND not already present in fixture_timeline.
    Applies the specified limit.
    """
    fixture_ids = []
    cursor = None
    # Define finished statuses where pressure index might be available
    finished_statuses = ('FT', 'AET', 'FT_PEN') # Add others if applicable (e.g., 'AWD', 'ABD'?)

    try:
        cursor = conn.cursor()
        # Select fixture_id from schedules where status is finished
        # AND the fixture_id does not already exist with event_type='pressure' in fixture_timeline.
        # Using LEFT JOIN and checking for NULL is generally efficient.
        query = f"""
            SELECT DISTINCT s.fixture_id
            FROM schedules s
            LEFT JOIN (
                SELECT DISTINCT fixture_id
                FROM {TIMELINE_TABLE_NAME}
                WHERE event_type = 'pressure'
            ) pt ON s.fixture_id = pt.fixture_id
            WHERE s.status IN {finished_statuses} AND pt.fixture_id IS NULL
            ORDER BY s.start_time DESC -- Process more recent fixtures first potentially
        """

        # Apply the limit if provided
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        cursor.execute(query)
        rows = cursor.fetchall()
        fixture_ids = [row['fixture_id'] for row in rows]
        logging.info(f"Found {len(fixture_ids)} finished fixtures without pressure data (Limit applied: {limit}).")

    except sqlite3.Error as e:
        logging.error(f"Error fetching fixture IDs for pressure processing: {e}")
        if f"no such table: {TIMELINE_TABLE_NAME}" in str(e).lower(): # Use lower() for case-insensitivity
             logging.info(f"'{TIMELINE_TABLE_NAME}' table not found (will be created), fetching all finished fixtures based on status.")
             try:
                 # Fallback: Fetch all finished fixtures regardless of timeline table presence
                 fallback_query = f"SELECT fixture_id FROM schedules WHERE status IN {finished_statuses} ORDER BY start_time DESC"
                 if limit is not None: fallback_query += f" LIMIT {int(limit)}" # Apply limit in fallback too
                 cursor.execute(fallback_query)
                 rows = cursor.fetchall()
                 fixture_ids = [row['fixture_id'] for row in rows]
                 logging.info(f"Found {len(fixture_ids)} finished fixture IDs (timeline table not present, Limit: {limit}).")
             except sqlite3.Error as e2:
                 logging.error(f"Error executing fallback query for finished fixtures: {e2}")
        elif "no such table: schedules" in str(e).lower():
            logging.error("Critical: 'schedules' table not found. Run sync_schedules.py first.")
            return [] # Return empty list
    finally:
        if cursor:
            cursor.close()
    return fixture_ids

def save_raw_pressure_data(data, fixture_id):
    """Saves the raw fixture pressure JSON data."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Ensure PRESSURE_RAW_DIR is a Path object
    global PRESSURE_RAW_DIR # Allow modification if it wasn't a Path initially
    if not isinstance(PRESSURE_RAW_DIR, Path):
        PRESSURE_RAW_DIR = Path(PRESSURE_RAW_DIR)
    file_path = PRESSURE_RAW_DIR / f"pressure_{fixture_id}_{timestamp}.json"
    try:
        save_data = {
            "fetch_timestamp": datetime.now().isoformat(),
            "fixture_id": fixture_id,
            "fixture_data_with_pressure": data # Store the full response
        }
        with open(file_path, "w") as f:
            json.dump(save_data, f, indent=2)
        logging.debug(f"Saved raw pressure data for fixture {fixture_id} to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving raw pressure data for fixture {fixture_id} to {file_path}: {e}")
        return None

# --- Main Workflow ---
def sync_pressure(limit=FIXTURE_LIMIT): # Accept limit as argument
    """Fetches pressure index for finished fixtures, processes, and stores them in fixture_timeline."""
    logging.info("=== Starting Fixture Pressure Index Sync Workflow ===")
    # Use the limit passed to the function, which defaults to the constant
    effective_limit = limit
    logging.info(f"Effective fixture limit for this run: {effective_limit}")


    conn = get_db_connection()
    if not conn:
        logging.critical("Failed to connect to the database. Exiting.")
        sys.exit(1)

    processed_rows_batch = [] # Accumulate processed rows for batch insertion
    total_fixtures_processed = 0
    total_pressure_rows_stored = 0
    fixtures_with_errors = []
    fixtures_with_no_data = 0

    try:
        # 1. Ensure Fixture Timeline Table Exists
        logging.info(f"Ensuring database table '{TIMELINE_TABLE_NAME}' exists...")
        create_fixture_timeline_table(conn)
        # Consider adding update trigger if manual updates to timeline are expected
        # create_update_trigger(conn, TIMELINE_TABLE_NAME, 'timeline_id')

        # 2. Get Fixture IDs to Fetch (Applying the effective limit here)
        fixture_ids_to_process = get_fixture_ids_for_pressure(conn, limit=effective_limit)
        if not fixture_ids_to_process:
            logging.info("No new finished fixture IDs found needing pressure data. Exiting.")
            # Cleanly exit if no work to do
            conn.close()
            logging.info("Database connection closed.")
            print("=== Fixture Pressure Index Sync Workflow Completed (No Fixtures to Process) ===")
            # Use return instead of sys.exit in functions if possible
            return


        # 3. Initialize API Client
        client = APIClient()
        num_fixtures = len(fixture_ids_to_process)
        logging.info(f"Attempting to fetch pressure index for {num_fixtures} fixtures...")

        # 4. Loop Through Fixture IDs, Fetch, Process, Store in Batches
        for i, fixture_id in enumerate(fixture_ids_to_process):
            # Check if we've already processed the limit (relevant if limit is passed via arg)
            # This check is technically redundant if get_fixture_ids_for_pressure respects the limit,
            # but adds safety if the limit logic changes.
            if effective_limit is not None and i >= effective_limit:
                 logging.info(f"Reached fixture limit ({effective_limit}). Stopping processing loop.")
                 break

            logging.info(f"\n--- Processing Fixture ID: {fixture_id} ({i+1}/{num_fixtures}) ---")
            # Construct endpoint for fixture details including pressure
            endpoint = f"v3/football/fixtures/{fixture_id}?include=pressure"
            try:
                # Fetch fixture data with pressure include
                logging.info(f"Fetching pressure data from: {endpoint}")
                raw_data = client.get(endpoint) # APIClient handles retries internally

                if raw_data and isinstance(raw_data.get('data'), dict): # Check if response is usable
                    # Optional: Save raw data
                    save_raw_pressure_data(raw_data, fixture_id)

                    # Process the raw data to extract pressure rows
                    logging.info(f"Processing pressure data for fixture {fixture_id}...")
                    processed_pressure_rows = process_pressure_data(raw_data) # Use the specific processor

                    if processed_pressure_rows:
                        processed_rows_batch.extend(processed_pressure_rows) # Add rows to batch
                        logging.info(f"Successfully processed {len(processed_pressure_rows)} pressure rows for fixture {fixture_id}.")
                    else:
                        # This can happen if the API returns data but the 'pressure' list is empty or invalid
                        logging.info(f"No valid pressure data found or processed within the response for fixture {fixture_id}.")
                        fixtures_with_no_data += 1

                    # Mark fixture as processed (we attempted it and got a valid-looking response structure)
                    total_fixtures_processed += 1

                else:
                    # Handle cases where client.get might return None (after retries) or invalid structure
                    logging.warning(f"No valid data dictionary returned from API client for fixture {fixture_id} (endpoint: {endpoint}). Skipping.")
                    fixtures_with_errors.append(fixture_id)
                    # Optionally increment fixtures_with_no_data as well if desired
                    # fixtures_with_no_data +=1


                # Store data in batches or at the end (if batch is full or it's the last fixture)
                # Also store if it's the last iteration within the applied limit
                is_last_iteration = (i == num_fixtures - 1) or \
                                    (effective_limit is not None and i == effective_limit - 1)

                if processed_rows_batch and (len(processed_rows_batch) >= BATCH_SIZE or is_last_iteration):
                    logging.info(f"\nStoring batch of {len(processed_rows_batch)} pressure rows into {TIMELINE_TABLE_NAME}...")
                    # Use generic store_data with INSERT OR IGNORE due to UNIQUE constraint
                    # 'timeline_id' is auto-increment, so doesn't need special handling here
                    stored_count = store_data(conn, TIMELINE_TABLE_NAME, processed_rows_batch, primary_key_column="timeline_id", use_insert_ignore=True)
                    if stored_count is not None: # store_data returns count on success, 0 or None on failure/no data
                         total_pressure_rows_stored += stored_count
                         logging.info(f"Finished storing batch. Inserted/Affected: {stored_count}")
                    else:
                         logging.error(f"Failed to store batch for table {TIMELINE_TABLE_NAME}.")
                    processed_rows_batch = [] # Reset batch

                # Apply API Delay (if not the very last iteration)
                if API_DELAY_SECONDS > 0 and not is_last_iteration:
                    logging.debug(f"Waiting {API_DELAY_SECONDS}s before next fixture...")
                    time.sleep(API_DELAY_SECONDS)

            except KeyboardInterrupt:
                 logging.warning("Keyboard interrupt detected. Stopping sync process.")
                 # Optionally store remaining batch before exiting
                 if processed_rows_batch:
                     logging.info(f"Storing remaining batch of {len(processed_rows_batch)} rows before exiting...")
                     store_data(conn, TIMELINE_TABLE_NAME, processed_rows_batch, primary_key_column="timeline_id", use_insert_ignore=True)
                 raise # Re-raise interrupt

            except Exception as e:
                logging.error(f"An unexpected error occurred while processing fixture {fixture_id}: {e}", exc_info=True) # Log traceback
                fixtures_with_errors.append(fixture_id)
                # Decide if you want to continue or stop on error (currently continues)

        print("\n--- Sync Summary ---")
        logging.info(f"Fixture IDs targeted (after limit): {num_fixtures}")
        logging.info(f"Fixtures successfully processed (API call attempted & valid response structure received): {total_fixtures_processed}")
        logging.info(f"Fixtures processed but had no pressure data within response: {fixtures_with_no_data}")
        if fixtures_with_errors:
            logging.warning(f"Fixtures with fetch/processing errors or invalid API response: {len(fixtures_with_errors)} -> {fixtures_with_errors}")
        logging.info(f"Total pressure rows inserted into DB ({TIMELINE_TABLE_NAME}): {total_pressure_rows_stored}")

    except Exception as e:
        logging.critical(f"An unexpected error occurred during the main workflow: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("\nDatabase connection closed.")

    logging.info("=== Fixture Pressure Index Sync Workflow Completed ===")

if __name__ == "__main__":
    # You can override the limit via command line argument if needed,
    # otherwise it uses the FIXTURE_LIMIT constant.
    # Example: python scripts/sync_pressure.py 100
    script_limit = None
    if len(sys.argv) > 1:
        try:
            # Use the command-line argument if provided and valid
            script_limit = int(sys.argv[1])
            logging.info(f"Running with command-line limit: {script_limit}")
        except ValueError:
            # Use the constant if the argument is invalid
            logging.warning(f"Invalid command-line argument '{sys.argv[1]}'. Using default limit: {FIXTURE_LIMIT}.")
            script_limit = FIXTURE_LIMIT
    else:
        # Use the constant if no command-line argument is given
        script_limit = FIXTURE_LIMIT

    try:
        # Pass the determined limit to the main function
        sync_pressure(limit=script_limit)
    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
        sys.exit(1)
    # To run without limit later: sync_pressure(limit=None) or remove the constant/arg logic

