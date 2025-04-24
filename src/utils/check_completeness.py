import sqlite3
import os
import sys
from pathlib import Path
import logging

# --- Assuming execution from project root or src is in PYTHONPATH ---
# Add project root if necessary (if running as a standalone script)
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.data.storage import get_db_connection
    # Ensure DATABASE_PATH is accessible if get_db_connection relies on it implicitly
    # from src.config import DATABASE_PATH
except ImportError:
    logging.error("Could not import DB functions. Make sure PYTHONPATH is set.")
    # Basic fallback for get_db_connection if needed for standalone execution
    def get_db_connection():
        # You might need to adjust the path based on where you place/run this script
        db_path = Path("./data/database/football_data.db")
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            logging.info(f"Connected to DB (fallback): {db_path}")
            return conn
        except sqlite3.Error as e:
            logging.error(f"Fallback DB connection error: {e}")
            return None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of required statistic column names in the fixture_stats table [cite: 5]
# (Ensure this list matches the columns you deem essential)
REQUIRED_HALF_TIME_STATS_COLS = [
    "goals", "fouls", "red_cards", "tackles", "shots_blocked",
    "successful_passes_percentage", "ball_possession", "saves",
    "attacks", "shots_total", "shots_insidebox"
]

def find_incomplete_fixtures_revised(conn, required_stats_columns):
    """
    Identifies fixtures considered incomplete in the fixture_stats table (Revised Logic).

    An incomplete fixture is one that meets EITHER of these conditions:
    1. Has fewer than two distinct team entries for the 'first_half' period.
    2. Has 'first_half' entries for a team, but ALL of the statistics listed
       in required_stats_columns are NULL for that team's entry.

    Args:
        conn: An active sqlite3 database connection object.
        required_stats_columns (list): A list of column names where if ALL are NULL
                                        for a first-half entry, it's considered incomplete.

    Returns:
        tuple: (list_of_incomplete_fixture_ids, total_count)
               Returns ([], 0) if no incomplete fixtures are found or on error.
    """
    if not conn:
        logging.error("Database connection is invalid.")
        return [], 0

    cursor = None
    incomplete_fixture_ids = []
    total_count = 0

    try:
        cursor = conn.cursor()

        # --- Build the SQL query ---

        # Part 1: Check for rows where ALL required stat columns are NULL for 'first_half'
        # *** MODIFIED LOGIC: Use AND instead of OR ***
        all_null_checks = " AND ".join([f"{col} IS NULL" for col in required_stats_columns])
        query_part1 = f"""
            SELECT DISTINCT fixture_id
            FROM fixture_stats
            WHERE period = 'first_half'
            AND ({all_null_checks})
        """

        # Part 2: Check for fixtures with less than 2 distinct team entries for 'first_half'
        # (This part remains the same)
        query_part2 = """
            SELECT fixture_id
            FROM fixture_stats
            WHERE period = 'first_half'
            GROUP BY fixture_id
            HAVING COUNT(DISTINCT team_id) < 2
        """

        # Combine using UNION to get all unique incomplete fixture IDs
        final_query = f"""
            SELECT fixture_id FROM (
                {query_part1}
                UNION
                {query_part2}
            )
            ORDER BY fixture_id;
        """

        logging.info("Executing query to find incomplete fixtures (revised logic)...")
        # Uncomment to debug the query
        # logging.debug(f"SQL Query for incomplete fixtures (revised):\n{final_query}")

        cursor.execute(final_query)
        rows = cursor.fetchall()

        incomplete_fixture_ids = [row['fixture_id'] for row in rows]
        total_count = len(incomplete_fixture_ids)

        logging.info(f"Found {total_count} incomplete fixtures (revised logic).")

    except sqlite3.Error as e:
        logging.error(f"Database error while finding incomplete fixtures: {e}")
        if "no such column" in str(e):
            logging.error(f"Potential issue: One of the columns in {required_stats_columns} might not exist in the fixture_stats table.")
        return [], 0
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return [], 0
    finally:
        if cursor:
            cursor.close()

    return incomplete_fixture_ids, total_count

# --- Example Usage ---
if __name__ == "__main__":
    print("=== Checking for Incomplete Fixture Stats (Revised Logic) ===")
    db_conn = get_db_connection()
    if db_conn:
        try:
            # Use the revised function
            ids, count = find_incomplete_fixtures_revised(db_conn, REQUIRED_HALF_TIME_STATS_COLS)
            print(f"\nTotal number of incomplete fixtures found: {count}")
            if ids:
                # Print first 100 IDs for brevity
                print("List of incomplete fixture IDs (first 100):")
                print(ids[:100])
                if count > 100:
                    print(f"... and {count - 100} more.")
            else:
                print("No incomplete fixtures found based on the revised criteria.")
        finally:
            db_conn.close()
            print("\nDatabase connection closed.")
    else:
        print("Could not connect to the database.")

    print("\n=== Check Complete ===")