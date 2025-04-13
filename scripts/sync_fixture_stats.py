#!/usr/bin/env python3
import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime
import sqlite3 # Needed for DB operations

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.client import APIClient # Import APIClient directly
from src.data.processors import process_fixture_stats_long # Import the new long format processor
from src.data.storage import (
    get_db_connection,
    create_fixture_stats_table, # Import function to create the stats table
    create_update_trigger,      # Keep for potential use if needed
    store_fixture_stats_long    # Import specific storage function for long stats
)
from src.config import RAW_DATA_DIR # For saving raw data

# --- Configuration ---
FIXTURE_DETAILS_RAW_DIR = RAW_DATA_DIR / "fixture_details" # Directory for raw fixture detail responses
FIXTURE_DETAILS_RAW_DIR.mkdir(parents=True, exist_ok=True)
API_DELAY_SECONDS = 0.5 # Optional delay between API calls per fixture
BATCH_SIZE = 100 # Process and store stats rows in batches (e.g., 100 rows)
FIXTURE_LIMIT = 100 # <<< LIMIT FOR TESTING as requested

# --- Helper Functions ---
def get_finished_round_fixture_ids(conn, limit=None):
    """
    Fetches fixture IDs from the schedules table where the round is marked as finished.
    Optionally excludes fixtures already present in the fixture_stats table.
    Optionally limits the number of fixtures returned.
    """
    fixture_ids = []
    cursor = None
    try:
        cursor = conn.cursor()
        # Select fixture_id from schedules where round_finished is true (1)
        # AND the fixture_id does not already exist in fixture_stats table for the 'full_match' period (or any period)
        # This prevents refetching stats for fixtures already processed.
        # We check for existence of *any* stat row for that fixture_id.
        query = """
            SELECT DISTINCT s.fixture_id
            FROM schedules s
            LEFT JOIN fixture_stats fs ON s.fixture_id = fs.fixture_id
            WHERE s.round_finished = 1 AND fs.fixture_id IS NULL
            ORDER BY s.fixture_id
        """
        # Alternative: Fetch all finished and let INSERT OR IGNORE handle updates
        # query = "SELECT fixture_id FROM schedules WHERE round_finished = 1 ORDER BY fixture_id"

        if limit:
            query += f" LIMIT {int(limit)}"

        cursor.execute(query)
        rows = cursor.fetchall()
        fixture_ids = [row['fixture_id'] for row in rows]
        print(f"Found {len(fixture_ids)} unprocessed fixture IDs from finished rounds (limit applied: {limit}).")

    except sqlite3.Error as e:
        print(f"Error fetching fixture IDs from schedules table: {e}")
        if "no such table: schedules" in str(e):
            print("Error: 'schedules' table not found. Run sync_schedules.py first.")
        elif "no such table: fixture_stats" in str(e):
             print("Info: 'fixture_stats' table not found (will be created), fetching all finished round fixtures.")
             try:
                 fallback_query = "SELECT fixture_id FROM schedules WHERE round_finished = 1 ORDER BY fixture_id"
                 if limit: fallback_query += f" LIMIT {int(limit)}"
                 cursor.execute(fallback_query)
                 rows = cursor.fetchall()
                 fixture_ids = [row['fixture_id'] for row in rows]
                 print(f"Found {len(fixture_ids)} fixture IDs from finished rounds (fixture_stats table not present, limit: {limit}).")
             except sqlite3.Error as e2:
                 print(f"Error executing fallback query: {e2}")
    finally:
        if cursor:
            cursor.close()
    return fixture_ids

def save_raw_fixture_detail(data, fixture_id):
    """Saves the raw fixture detail JSON data."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = FIXTURE_DETAILS_RAW_DIR / f"fixture_{fixture_id}_stats_{timestamp}.json" # Naming convention
    try:
        save_data = {
            "fetch_timestamp": datetime.now().isoformat(),
            "fixture_id": fixture_id,
            "fixture_data_with_stats": data # Store the full response
        }
        with open(file_path, "w") as f:
            json.dump(save_data, f, indent=2)
        return file_path
    except Exception as e:
        print(f"Error saving raw fixture detail for fixture {fixture_id} to {file_path}: {e}")
        return None

# --- Main Workflow ---
def main(limit=FIXTURE_LIMIT): # Accept limit as argument
    """Fetches details for finished fixtures, processes stats (long format), and stores them."""
    print("=== Starting Fixture Statistics Sync Workflow (Long Format) ===")
    stats_table_name = "fixture_stats"
    # stats_primary_key = "id" # Auto-increment ID is the PK for the trigger

    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database. Exiting.")
        sys.exit(1)

    processed_rows_batch = [] # Accumulate processed stat rows for batch insertion
    total_fixtures_processed = 0
    total_stats_rows_stored = 0
    fixtures_with_errors = []

    try:
        # 1. Ensure Fixture Stats Table Exists
        print(f"Ensuring database table '{stats_table_name}' exists...")
        create_fixture_stats_table(conn)
        # Trigger on 'id' might not be very useful if using INSERT OR IGNORE,
        # but doesn't hurt to have if manual updates occur later.
        # create_update_trigger(conn, stats_table_name, stats_primary_key)

        # 2. Get Fixture IDs to Fetch (Applying the limit here)
        fixture_ids_to_process = get_finished_round_fixture_ids(conn, limit=limit)
        if not fixture_ids_to_process:
            print("No new fixture IDs found from finished rounds to process. Exiting.")
            sys.exit(0)

        # 3. Initialize API Client
        client = APIClient()
        num_fixtures = len(fixture_ids_to_process)
        print(f"Attempting to fetch stats for {num_fixtures} fixtures...")

        # 4. Loop Through Fixture IDs, Fetch, Process, Store in Batches
        for i, fixture_id in enumerate(fixture_ids_to_process):
            print(f"\n--- Processing Fixture ID: {fixture_id} ({i+1}/{num_fixtures}) ---")
            # Construct endpoint with periods.statistics include
            endpoint = f"v3/football/fixtures/{fixture_id}?include=periods.statistics.type"
            try:
                # Fetch fixture data
                print(f"Fetching details from: {endpoint}")
                raw_data = client.get(endpoint)

                if raw_data and 'data' in raw_data:
                    # Optional: Save raw data
                    save_raw_fixture_detail(raw_data, fixture_id)

                    # Process the raw data to extract stats in long format (list of rows)
                    print(f"Processing statistics for fixture {fixture_id}...")
                    processed_stat_rows = process_fixture_stats_long(raw_data)

                    if processed_stat_rows:
                        processed_rows_batch.extend(processed_stat_rows) # Add rows to batch
                        total_fixtures_processed += 1 # Count fixture as processed
                        print(f"Successfully processed {len(processed_stat_rows)} stat rows for fixture {fixture_id}.")
                    else:
                        print(f"No valid stats processed for fixture {fixture_id}.")
                        # Don't necessarily mark as error if API just didn't provide stats

                else:
                    print(f"No data or invalid data returned from API for fixture {fixture_id}.")
                    fixtures_with_errors.append(fixture_id)

                # Store data in batches or at the end
                if len(processed_rows_batch) >= BATCH_SIZE or (i == num_fixtures - 1 and processed_rows_batch):
                    print(f"\nStoring batch of {len(processed_rows_batch)} fixture stats rows...")
                    # Use the specific storage function for long stats with INSERT OR IGNORE
                    stored_count = store_fixture_stats_long(conn, processed_rows_batch)
                    total_stats_rows_stored += stored_count
                    print(f"Finished storing batch. Inserted: {stored_count}")
                    processed_rows_batch = [] # Reset batch

                # Optional delay
                if API_DELAY_SECONDS > 0 and i < num_fixtures - 1:
                    time.sleep(API_DELAY_SECONDS)

            except Exception as e:
                print(f"Error processing fixture {fixture_id}: {e}")
                fixtures_with_errors.append(fixture_id)
                # Decide if you want to continue or stop on error

        print("\n--- Sync Summary ---")
        print(f"Fixture IDs attempted: {num_fixtures}")
        print(f"Fixtures successfully processed (stats found): {total_fixtures_processed}")
        if fixtures_with_errors:
            print(f"Fixtures with fetch/processing errors: {len(fixtures_with_errors)} -> {fixtures_with_errors}")
        print(f"Total stats rows inserted into DB: {total_stats_rows_stored}")

    except Exception as e:
        print(f"An unexpected error occurred during the main workflow: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

    print("=== Fixture Statistics Sync Workflow (Long Format) Completed ===")

if __name__ == "__main__":
    # Pass the limit to the main function
    main(limit=FIXTURE_LIMIT)
    # To run without limit later: main(limit=None) or just main() if default is None
