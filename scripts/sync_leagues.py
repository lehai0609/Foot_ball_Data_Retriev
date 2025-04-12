#!/usr/bin/env python3
import os
import sys
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Add project root to Python path to allow importing from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.api.endpoints import EndpointHandler
from src.config import DATABASE_PATH # Import the new DB path

# --- Database Setup ---

def create_leagues_table(conn):
    """Creates the leagues table using the adjusted schema if it doesn't exist."""
    cursor = conn.cursor()
    try:
        # Adjusted schema from previous step
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            league_id INTEGER PRIMARY KEY,
            sport_id INTEGER,
            country_id INTEGER,
            name TEXT NOT NULL,
            active BOOLEAN DEFAULT 1,
            short_code TEXT,
            image_path TEXT,
            type TEXT,
            sub_type TEXT,
            last_played_at TEXT,
            category INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create indexes (optional but recommended)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leagues_country ON leagues(country_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leagues_type ON leagues(type, sub_type);")

        # Create the trigger to update 'updated_at'
        # Use TRY/CATCH or check if trigger exists to avoid errors on re-runs
        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_leagues_updated_at
        AFTER UPDATE ON leagues
        FOR EACH ROW
        BEGIN
            UPDATE leagues SET updated_at = CURRENT_TIMESTAMP WHERE league_id = OLD.league_id;
        END;
        """)
        print("Leagues table ensured.")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error during table creation: {e}")
        conn.rollback() # Rollback changes if error occurs
    finally:
        cursor.close()

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # Optional: Set row factory to access columns by name
        conn.row_factory = sqlite3.Row
        print(f"Connected to database: {DATABASE_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1) # Exit if DB connection fails

# --- Data Processing ---

def process_league_data(raw_league_data):
    """
    Transforms a raw league dictionary from the API into a dictionary
    suitable for database insertion, matching the adjusted schema.
    """
    processed = {
        "league_id": raw_league_data.get("id"),
        "sport_id": raw_league_data.get("sport_id"),
        "country_id": raw_league_data.get("country_id"),
        "name": raw_league_data.get("name"),
        "active": raw_league_data.get("active"),
        "short_code": raw_league_data.get("short_code"), # Can be None
        "image_path": raw_league_data.get("image_path"),
        "type": raw_league_data.get("type"),
        "sub_type": raw_league_data.get("sub_type"),
        "last_played_at": raw_league_data.get("last_played_at"), # Can be None
        "category": raw_league_data.get("category")
    }
    # Basic validation: Ensure essential fields are present
    if not processed["league_id"] or not processed["name"]:
        print(f"Warning: Skipping league due to missing ID or name: {raw_league_data}")
        return None
    return processed

# --- Data Storage ---

def store_leagues(conn, processed_leagues):
    """
    Inserts or updates league data into the SQLite database.
    Uses INSERT OR REPLACE to handle existing records based on the primary key (league_id).
    """
    if not processed_leagues:
        print("No processed leagues to store.")
        return 0

    cursor = conn.cursor()
    inserted_count = 0
    replaced_count = 0

    # Using INSERT OR REPLACE: If a row with the same league_id exists, it's deleted and replaced.
    # The 'updated_at' trigger handles the timestamp update automatically for replacements.
    # For new inserts, 'created_at' and 'updated_at' get the default CURRENT_TIMESTAMP.
    sql = """
    INSERT OR REPLACE INTO leagues (
        league_id, sport_id, country_id, name, active, short_code,
        image_path, type, sub_type, last_played_at, category
        -- created_at is set on first insert, updated_at handled by trigger/default
    ) VALUES (
        :league_id, :sport_id, :country_id, :name, :active, :short_code,
        :image_path, :type, :sub_type, :last_played_at, :category
    );
    """

    try:
        for league in processed_leagues:
            if league: # Ensure league is not None (skipped during processing)
                 # Check if record exists to count inserts vs replaces accurately
                cursor.execute("SELECT 1 FROM leagues WHERE league_id = ?", (league['league_id'],))
                exists = cursor.fetchone()

                cursor.execute(sql, league)

                if exists:
                    replaced_count += 1
                else:
                    inserted_count += 1

        conn.commit()
        print(f"Successfully stored leagues. Inserted: {inserted_count}, Replaced/Updated: {replaced_count}")
        return inserted_count + replaced_count
    except sqlite3.Error as e:
        print(f"Database error during storage: {e}")
        conn.rollback() # Rollback changes if error occurs
        return 0
    finally:
        cursor.close()

# --- Main Workflow ---

def main():
    """Main function to run the fetch, process, and store workflow for leagues."""
    print("=== Starting League Data Workflow ===")

    # 1. Fetch Data
    # Use the generic EndpointHandler
    # Consider adding 'include=country' if you want country details later
    leagues_endpoint = "v3/football/leagues"
    handler = EndpointHandler(leagues_endpoint)
    print(f"Fetching data from endpoint: {leagues_endpoint}")
    # Fetch all pages (adjust per_page as needed)
    # Note: EndpointHandler saves raw JSON files automatically
    # We primarily need the returned list of data items here.
    try:
        # Set include=country if you want country data (requires schema adjustment or separate table)
        # all_raw_leagues_data, metadata = handler.fetch_all_data(include="country", per_page=100)
        all_raw_leagues_data, metadata = handler.fetch_all_data(per_page=100) # Fetch without country for now
        print(f"Fetched {metadata.get('items_fetched', 0)} raw league items.")
        if metadata.get('errors'):
             print(f"Warning: {len(metadata['errors'])} errors occurred during fetch.")

    except Exception as e:
        print(f"Fatal error during data fetch: {e}")
        sys.exit(1)

    # 2. Process Data
    print("Processing fetched league data...")
    processed_leagues = [process_league_data(league) for league in all_raw_leagues_data]
    # Filter out any None values from processing failures
    processed_leagues = [p for p in processed_leagues if p is not None]
    print(f"Successfully processed {len(processed_leagues)} leagues.")

    # 3. Store Data
    print("Storing processed data into SQLite database...")
    conn = get_db_connection()
    if conn:
        try:
            create_leagues_table(conn) # Ensure table exists
            total_stored = store_leagues(conn, processed_leagues)
            print(f"Finished storing {total_stored} leagues.")
        finally:
            conn.close()
            print("Database connection closed.")

    print("=== League Data Workflow Completed ===")

if __name__ == "__main__":
    # Place this script in your 'scripts/' directory
    # Ensure your current working directory is the project root when running
    # Example: python scripts/sync_leagues.py
    main()
