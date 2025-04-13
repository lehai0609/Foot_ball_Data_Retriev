#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.endpoints import EndpointHandler
# Import the new processing and storage functions
from src.data.processors import process_league_data
from src.data.storage import get_db_connection, create_leagues_table, store_data

# --- Main Workflow ---
def main():
    """Main function to sync league data: fetch, process, store."""
    print("=== Starting League Data Sync Workflow ===")
    resource_name = "leagues"
    endpoint = "v3/football/leagues"
    table_name = "leagues"
    primary_key = "league_id"

    # 1. Fetch Data
    handler = EndpointHandler(endpoint)
    print(f"Fetching data from endpoint: {endpoint}")
    try:
        # Fetch without country include for now, consistent with original
        all_raw_data, metadata = handler.fetch_all_data(per_page=100)
        print(f"Fetched {metadata.get('items_fetched', 0)} raw {resource_name} items.")
        if metadata.get('errors'):
            print(f"Warning: {len(metadata['errors'])} errors occurred during fetch.")
    except Exception as e:
        print(f"Fatal error during data fetch: {e}")
        sys.exit(1)

    # 2. Process Data
    print(f"Processing fetched {resource_name} data...")
    processed_data = [process_league_data(item) for item in all_raw_data]
    processed_data = [p for p in processed_data if p is not None] # Filter out None values
    print(f"Successfully processed {len(processed_data)} {resource_name}.")

    # 3. Store Data
    print(f"Storing processed data into SQLite database table '{table_name}'...")
    conn = get_db_connection()
    if conn:
        try:
            create_leagues_table(conn) # Ensure table exists (specific setup)
            # Use the generic store function
            total_stored = store_data(conn, table_name, processed_data, primary_key)
            print(f"Finished storing {total_stored} {resource_name}.")
        finally:
            conn.close()
            print("Database connection closed.")
    else:
        print("Failed to connect to the database. Storage step skipped.")


    print(f"=== {resource_name.capitalize()} Data Sync Workflow Completed ===")

if __name__ == "__main__":
    main()