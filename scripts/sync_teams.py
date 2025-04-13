#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.endpoints import EndpointHandler
# Import the new processing and storage functions
from src.data.processors import process_team_data
from src.data.storage import (
    get_db_connection,
    create_teams_table,
    create_update_trigger, # Import trigger function
    store_data
)

# --- Main Workflow ---
def main():
    """Main function to sync team data: fetch, process, store."""
    print("=== Starting Team Data Sync Workflow ===")
    resource_name = "teams"
    endpoint = "v3/football/teams"
    table_name = "teams"
    primary_key = "team_id"

    # 1. Fetch Data
    handler = EndpointHandler(endpoint)
    print(f"Fetching data from endpoint: {endpoint}")
    try:
        # Add include parameters if needed later, e.g., include="country"
        # all_raw_data, metadata = handler.fetch_all_data(include="country", per_page=100)
        all_raw_data, metadata = handler.fetch_all_data(per_page=100) # Fetch basic data for now
        print(f"Fetched {metadata.get('items_fetched', 0)} raw {resource_name} items.")
        if metadata.get('errors'):
            print(f"Warning: {len(metadata['errors'])} errors occurred during fetch.")
            # Consider logging errors more formally
    except Exception as e:
        print(f"Fatal error during data fetch: {e}")
        sys.exit(1) # Exit if fetching fails

    # 2. Process Data
    print(f"Processing fetched {resource_name} data...")
    processed_data = [process_team_data(item) for item in all_raw_data]
    processed_data = [p for p in processed_data if p is not None] # Filter out None values
    print(f"Successfully processed {len(processed_data)} {resource_name}.")

    if not processed_data:
        print("No valid team data processed. Skipping storage.")
        print(f"=== {resource_name.capitalize()} Data Sync Workflow Completed (No Data Stored) ===")
        sys.exit(0)

    # 3. Store Data
    print(f"Storing processed data into SQLite database table '{table_name}'...")
    conn = get_db_connection()
    if conn:
        try:
            # Ensure table and trigger exist
            create_teams_table(conn)
            create_update_trigger(conn, table_name, primary_key) # Create the update trigger

            # Use the generic store function
            total_stored = store_data(conn, table_name, processed_data, primary_key)
            print(f"Finished storing {total_stored} {resource_name}.")
        except Exception as e:
            print(f"An error occurred during database operations: {e}")
            # Optionally rollback if not handled in lower functions
            # conn.rollback()
        finally:
            conn.close()
            print("Database connection closed.")
    else:
        print("Failed to connect to the database. Storage step skipped.")


    print(f"=== {resource_name.capitalize()} Data Sync Workflow Completed ===")

if __name__ == "__main__":
    # Ensure your current working directory is the project root when running
    # Example: python scripts/sync_teams.py
    main()
