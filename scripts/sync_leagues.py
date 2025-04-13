#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add project root to Python path to allow importing from src
# This assumes the script is run from the project root (e.g., python scripts/sync_leagues.py)
# Or that the project root is already in PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.endpoints import EndpointHandler
# Import the necessary processing and storage functions
from src.data.processors import process_league_data, process_season_data # Import both processors
from src.data.storage import (
    get_db_connection,
    create_leagues_table,
    create_seasons_table, # Import function to create the seasons table
    create_update_trigger, # Import trigger function if needed for seasons
    store_data
)

# --- Main Workflow ---
def main():
    """Main function to sync league and current season data: fetch, process, store."""
    print("=== Starting League & Current Season Data Sync Workflow ===")
    league_endpoint = "v3/football/leagues"
    leagues_table_name = "leagues"
    leagues_primary_key = "league_id"
    seasons_table_name = "seasons"
    seasons_primary_key = "season_id"

    # 1. Fetch Data with Current Season Included
    # The EndpointHandler will use 'leagues' for directory naming based on the endpoint path
    handler = EndpointHandler(league_endpoint)
    print(f"Fetching data from endpoint: {league_endpoint} with include=currentSeason")
    try:
        # Use the include parameter to get current season data embedded
        all_raw_data, metadata = handler.fetch_all_data(include="currentSeason", per_page=100) # Adjust per_page as needed
        print(f"Fetched {metadata.get('items_fetched', 0)} raw league items (with potential season data).")
        if metadata.get('errors'):
            print(f"Warning: {len(metadata['errors'])} errors occurred during fetch.")
            # Consider logging errors more formally or halting execution
    except Exception as e:
        print(f"Fatal error during data fetch: {e}")
        sys.exit(1) # Exit if fetching fails catastrophically

    if not all_raw_data:
        print("No raw data fetched. Exiting.")
        sys.exit(0)

    # 2. Process Data (Leagues and Seasons separately)
    print("Processing fetched data...")
    processed_leagues = []
    processed_seasons = []
    processed_season_ids = set() # Keep track of season IDs already processed to avoid duplicates
    skipped_leagues = 0
    skipped_seasons = 0

    for item in all_raw_data:
        if not item: # Skip if the item itself is null/empty
            skipped_leagues += 1
            continue

        # Process league part
        league = process_league_data(item)
        if league:
            processed_leagues.append(league)
        else:
            skipped_leagues += 1

        # Process season part if it exists and hasn't been processed already
        if 'currentseason' in item and item['currentseason']:
            raw_season = item['currentseason']
            season_id = raw_season.get('id')

            # Check if this season ID has already been added
            if season_id and season_id not in processed_season_ids:
                season = process_season_data(raw_season)
                if season:
                    processed_seasons.append(season)
                    processed_season_ids.add(season_id) # Mark season ID as processed
                else:
                     skipped_seasons +=1
            # else:
                 # Season data missing, invalid, or already processed
                 # print(f"Skipping season processing for league {item.get('id')} - Season ID: {season_id}") # Optional log
        # else:
            # print(f"No 'currentseason' data found for league {item.get('id')}") # Optional log

    print(f"Successfully processed {len(processed_leagues)} leagues (skipped {skipped_leagues}).")
    print(f"Successfully processed {len(processed_seasons)} unique current seasons (skipped {skipped_seasons}).")


    # 3. Store Data
    print("Connecting to database...")
    conn = get_db_connection()
    if conn:
        try:
            # Ensure tables and triggers exist BEFORE storing data
            print(f"Ensuring database table '{leagues_table_name}' exists...")
            create_leagues_table(conn)
            # Add trigger for leagues table if you want updated_at automatically handled
            create_update_trigger(conn, leagues_table_name, leagues_primary_key)

            print(f"Ensuring database table '{seasons_table_name}' exists...")
            create_seasons_table(conn)
            # Add trigger for seasons table
            create_update_trigger(conn, seasons_table_name, seasons_primary_key)

            # Store Leagues
            if processed_leagues:
                print(f"Storing {len(processed_leagues)} processed leagues into '{leagues_table_name}'...")
                leagues_stored = store_data(conn, leagues_table_name, processed_leagues, leagues_primary_key)
                print(f"Finished storing leagues. Stored/Updated: {leagues_stored}")
            else:
                print("No valid league data to store.")

            # Store Seasons
            if processed_seasons:
                print(f"Storing {len(processed_seasons)} processed current seasons into '{seasons_table_name}'...")
                seasons_stored = store_data(conn, seasons_table_name, processed_seasons, seasons_primary_key)
                print(f"Finished storing seasons. Stored/Updated: {seasons_stored}")

            else:
                print("No valid current season data to store.")

        except Exception as e:
            print(f"An error occurred during database operations: {e}")
            # Optionally rollback if not handled in lower functions
            conn.rollback() # Rollback on any exception during DB operations
        finally:
            conn.close()
            print("Database connection closed.")
    else:
        print("Failed to connect to the database. Storage step skipped.")


    print("=== League & Current Season Data Sync Workflow Completed ===")

if __name__ == "__main__":
    # Best practice: Ensure the script is run from the project root directory
    # Example: python scripts/sync_leagues.py
    main()
