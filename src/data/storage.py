# src/data/storage.py
import sqlite3
import logging
from pathlib import Path # Assuming DATABASE_PATH is a Path object

# Assuming DATABASE_PATH is imported correctly from src.config
# Example: from src.config import DATABASE_PATH
# Placeholder if run standalone: Adjust path as needed relative to your execution context
try:
    # Assumes execution from project root or src is in PYTHONPATH
    from src.config import DATABASE_PATH
except ImportError:
    # Fallback if the import fails (e.g., running script directly)
    logging.warning("Could not import DATABASE_PATH from src.config. Using relative path.")
    # Adjust this relative path based on where this script might be run from
    # If run from project root:
    DATABASE_PATH = Path("./data/database/football_data.db")
    # If run from scripts directory:
    # DATABASE_PATH = Path("../data/database/football_data.db")


# Configure basic logging if not done elsewhere
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Setup ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        # Ensure the parent directory exists
        if not DATABASE_PATH.parent.exists():
             DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
             logging.info(f"Created database directory: {DATABASE_PATH.parent}")
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        # conn.execute("PRAGMA foreign_keys = ON") # Optional: Enforce foreign keys
        logging.info(f"Connected to database: {DATABASE_PATH}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DB connection: {e}")
        return None


# --- Helper function used by storage functions ---
def create_table(conn, create_table_sql):
     """Creates a table using the provided SQL if it doesn't exist."""
     cursor = None
     try:
         cursor = conn.cursor()
         cursor.execute(create_table_sql)
         conn.commit()
         return True # Indicate success
     except sqlite3.Error as e:
         logging.error(f"Database error during table creation: {e}") # Use logging
         # Avoid rollback if the error is "table already exists" which is expected with IF NOT EXISTS
         if "already exists" not in str(e).lower():
             try:
                 conn.rollback()
             except sqlite3.Error as rb_err:
                 logging.error(f"Rollback failed after table creation error: {rb_err}")
         return False # Indicate failure
     finally:
        if cursor:
            cursor.close()

# --- Table Creation Functions ---

def create_leagues_table(conn):
    """Creates the leagues table."""
    # Based on original storage.py
    sql = """
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
        current_season_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): logging.info("Leagues table ensured.")
    else: logging.error("Failed to ensure leagues table.")

def create_seasons_table(conn):
    """Creates the seasons table."""
    # Based on original storage.py (already included league_name)
    sql = """
    CREATE TABLE IF NOT EXISTS seasons (
        season_id INTEGER PRIMARY KEY,
        league_id INTEGER NOT NULL,
        league_name TEXT,
        sport_id INTEGER,
        name TEXT NOT NULL,
        is_current BOOLEAN DEFAULT 0,
        finished BOOLEAN DEFAULT 0,
        pending BOOLEAN DEFAULT 0,
        starting_at TEXT,
        ending_at TEXT,
        standings_recalculated_at TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): logging.info("Seasons table ensured.")
    else: logging.error("Failed to ensure seasons table.")

def create_teams_table(conn):
    """Creates the teams table."""
    # Based on original storage.py
    sql = """
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        short_code TEXT,
        country_id INTEGER,
        logo_url TEXT,
        venue_id INTEGER,
        founded INTEGER,
        type TEXT,
        national_team BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): logging.info("Teams table ensured.")
    else: logging.error("Failed to ensure teams table.")

# --- REVISED create_schedules_table function ---
def create_schedules_table(conn):
    """
    Creates the schedules table, enhanced with key fixture details
    suitable for analysis and modeling.
    """
    # Schema combining schedule context with core fixture details
    sql = """
    CREATE TABLE IF NOT EXISTS schedules (
        fixture_id INTEGER PRIMARY KEY,     -- Unique ID for the match
        season_id INTEGER NOT NULL,         -- ID of the season
        league_id INTEGER,                  -- ID of the league (Added)
        round_id INTEGER,                   -- ID of the round within the season/stage
        home_team_id INTEGER NOT NULL,      -- ID of the home team (Added)
        away_team_id INTEGER NOT NULL,      -- ID of the away team (Added)
        start_time TEXT NOT NULL,           -- Match start time (Added, recommend storing as 'YYYY-MM-DD HH:MM:SS' TEXT or DATETIME)
        status TEXT,                        -- Match status (Added, e.g., 'FT', 'NS', 'LIVE', 'PST', 'CANC')
        home_score INTEGER,                 -- Final home score (Added, NULL if not finished/available)
        away_score INTEGER,                 -- Final away score (Added, NULL if not finished/available)
        result TEXT,                        -- Standardized result code (Added, 'H', 'D', 'A', or NULL)
        result_info TEXT,                   -- Raw result description from API (Added, optional)
        round_finished BOOLEAN,             -- Status of the round (Kept)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        -- Optional: Define Foreign Key constraints later when teams/leagues tables are stable
        -- FOREIGN KEY (season_id) REFERENCES seasons(season_id),
        -- FOREIGN KEY (league_id) REFERENCES leagues(league_id),
        -- FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
        -- FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
    );"""
    if create_table(conn, sql):
        logging.info("Schedules table (enhanced) ensured.")
    else:
        logging.error("Failed to ensure schedules table.")
# --- End of REVISED function ---


# --- Fixture Stats Table (REVISED SCHEMA - Aligned with Processor using Underscores) ---
def create_fixture_stats_table(conn):
    """
    Creates the fixture_stats table with column names consistently using
    underscores, aligned with the revised processor dictionaries.
    """
    # Schema using underscores for all multi-word stat names
    # Includes all columns from the revised processor dictionaries
    sql = """
    CREATE TABLE IF NOT EXISTS fixture_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,           -- participant_id from the API stat item
        period TEXT NOT NULL,               -- e.g., 'first_half', 'second_half', 'extra_time'

        -- Statistic columns (Aligned with underscore convention)
        goals INTEGER DEFAULT 0,
        shots_on_target INTEGER DEFAULT 0,   -- Corrected: underscore
        shots_off_target INTEGER DEFAULT 0,  -- Corrected: underscore
        ball_possession REAL DEFAULT NULL,   -- Corrected: underscore
        corners INTEGER DEFAULT 0,
        fouls INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,      -- Consistent: underscore
        red_cards INTEGER DEFAULT 0,         -- Consistent: underscore
        shots_total INTEGER DEFAULT 0,       -- Consistent: underscore
        shots_blocked INTEGER DEFAULT 0,
        offsides INTEGER DEFAULT 0,
        saves INTEGER DEFAULT 0,
        hit_woodwork INTEGER DEFAULT 0,
        shots_insidebox INTEGER DEFAULT 0,
        successful_dribbles INTEGER DEFAULT 0,
        successful_dribbles_percentage REAL DEFAULT NULL,
        successful_passes INTEGER DEFAULT 0,
        successful_passes_percentage REAL DEFAULT NULL,
        shots_outsidebox INTEGER DEFAULT 0,
        dribble_attempts INTEGER DEFAULT 0,
        throwins INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        accurate_crosses INTEGER DEFAULT 0,
        total_crosses INTEGER DEFAULT 0,     -- Consistent: underscore
        penalties INTEGER DEFAULT 0,
        passes INTEGER DEFAULT 0,
        attacks INTEGER DEFAULT 0,
        challenges INTEGER DEFAULT 0,
        tackles INTEGER DEFAULT 0,           -- Added from processor dicts
        interceptions INTEGER DEFAULT 0,     -- Added from processor dicts
        long_passes INTEGER DEFAULT 0,
        goal_kicks INTEGER DEFAULT 0,
        key_passes INTEGER DEFAULT 0,        -- Consistent: underscore
        dangerous_attacks INTEGER DEFAULT 0,
        substitutions INTEGER DEFAULT 0,

        -- Timestamps
        timestamp DATETIME,                  -- Timestamp when the stat was recorded/fetched
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        -- Constraints
        UNIQUE (fixture_id, team_id, period) -- Ensure uniqueness for a specific stat record
    );"""
    # Use the existing helper function to execute the SQL
    if create_table(conn, sql):
        logging.info("Fixture_Stats (aligned with underscores) table ensured.")
    else:
        logging.error("Failed to ensure fixture_stats table.")

# --- NEW: Fixture Odds Table ---
def create_fixture_odds_table(conn):
    """
    Creates the fixture_odds table to store pre-match odds.
    Includes all columns processed from the API.
    Uses a UNIQUE constraint to prevent duplicate odds entries.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS fixture_odds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER NOT NULL,
        market_id INTEGER NOT NULL,
        bookmaker_id INTEGER NOT NULL,
        label TEXT NOT NULL,
        name TEXT,
        market_description TEXT,
        value REAL,
        probability REAL,
        dp3 TEXT,                           -- Added: Store as TEXT
        fractional TEXT,                    -- Added: Store as TEXT
        american TEXT,                      -- Added: Store as TEXT
        winning BOOLEAN,                    -- Added: Store as BOOLEAN (0/1)
        stopped BOOLEAN,                    -- Added: Store as BOOLEAN (0/1)
        total REAL,                         -- Added: Store as REAL
        handicap REAL,
        participants TEXT,                  -- Added: Store participants info (e.g., as JSON TEXT)
        api_created_at TEXT,                -- Added: Store API timestamp as TEXT
        original_label TEXT,                -- Added
        latest_bookmaker_update TEXT,       -- Added: Store API timestamp as TEXT
        -- Timestamps for DB tracking
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        -- Constraints: Adjust UNIQUE constraint if needed, maybe 'total' or other fields
        -- are part of the uniqueness for some markets (e.g., Over/Under)
        UNIQUE (fixture_id, market_id, bookmaker_id, label, handicap, total) -- Adjusted UNIQUE constraint example
        -- Optional: Add FOREIGN KEY constraints later if needed
        -- FOREIGN KEY (fixture_id) REFERENCES schedules(fixture_id)
    );"""
    if create_table(conn, sql):
        logging.info("Fixture_Odds table ensured (with all columns).")
    else:
        logging.error("Failed to ensure fixture_odds table.")
# --- End of NEW function ---


# --- Data Storage ---

def store_data(conn, table_name, data_list, primary_key_column="id", use_insert_ignore=False):
    """
    Generic function to insert/replace or insert/ignore data into a specified table.
    Assumes data_list is a list of dictionaries where keys match column names.

    Args:
        conn: Database connection object.
        table_name (str): Name of the table to insert into.
        data_list (list): List of dictionaries representing rows.
        primary_key_column (str): Name of the primary key column (used for logging/counting).
                                   Not directly used for INSERT OR IGNORE logic.
        use_insert_ignore (bool): If True, use INSERT OR IGNORE. Otherwise, use INSERT OR REPLACE.
    """
    if not data_list:
        logging.warning(f"No processed data provided for table {table_name}.")
        return 0

    cursor = conn.cursor()
    inserted_count = 0
    replaced_count = 0 # Only relevant for INSERT OR REPLACE
    ignored_count = 0  # Only relevant for INSERT OR IGNORE
    skipped_count = 0

    # Find the first valid item to determine columns
    first_valid_item = next((item for item in data_list if item and isinstance(item, dict)), None)
    if not first_valid_item:
        logging.warning(f"No valid data items (dictionaries) found for table {table_name}.")
        if cursor: cursor.close()
        return 0

    columns = list(first_valid_item.keys())
    # Exclude auto-increment primary key 'id' if it exists and we are inserting
    if 'id' in columns and (use_insert_ignore or table_name == "fixture_odds"): # Exclude for odds table too
        columns.remove('id')

    placeholders = ', '.join(['?' for _ in columns])
    insert_mode = "INSERT OR IGNORE" if use_insert_ignore else "INSERT OR REPLACE"
    sql = f"{insert_mode} INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"

    item_for_error = None # Store sample item for error reporting
    try:
        for item in data_list:
            item_for_error = item # Keep track of last processed item
            if not item or not isinstance(item, dict):
                skipped_count += 1
                continue

            # Prepare values, ensuring order matches columns derived from first_valid_item
            # Handle potential missing keys in some items gracefully
            values = [item.get(col) for col in columns]

            # Execute the INSERT statement
            cursor.execute(sql, values)

            # Update counts based on rowcount
            if cursor.rowcount > 0:
                # How to differentiate insert vs replace for INSERT OR REPLACE is tricky without extra queries.
                # For INSERT OR IGNORE, rowcount > 0 means inserted.
                # For INSERT OR REPLACE, rowcount > 0 means inserted or replaced.
                # We'll simplify the counting for now.
                inserted_count += 1 # Count successful operations (insert or replace/ignore)
            elif use_insert_ignore:
                 ignored_count += 1 # rowcount is 0 for IGNORE

        conn.commit()
        if use_insert_ignore:
             logging.info(f"Successfully stored data into {table_name} using INSERT OR IGNORE. Inserted: {inserted_count}, Ignored (duplicates/skipped): {ignored_count + skipped_count}")
        else:
             # For INSERT OR REPLACE, inserted_count represents the total number of rows affected (inserted or replaced)
             logging.info(f"Successfully stored data into {table_name} using INSERT OR REPLACE. Affected rows: {inserted_count}, Skipped: {skipped_count}")
        return inserted_count # Return total affected/inserted rows
    except sqlite3.Error as e:
        logging.error(f"Database error during storage in {table_name} ({insert_mode}): {e}") # Use logging
        logging.error(f"SQL attempted: {sql}")
        logging.error(f"Item causing error (potentially): {item_for_error}")
        # Check for common errors like missing columns in the item
        if "has no column named" in str(e):
             logging.error(f"Possible cause: Item dictionary might be missing expected keys matching table columns.")
             logging.error(f"Expected columns based on first item (excluding 'id' if applicable): {columns}")
        conn.rollback()
        return 0
    finally:
        if cursor:
            cursor.close()


# store_fixture_stats_long should work without changes,
# as it dynamically gets columns from the input data.
def store_fixture_stats_long(conn, stats_rows):
    """
    Stores processed fixture statistics rows (long format) into the fixture_stats table.
    Uses INSERT OR IGNORE to handle the UNIQUE constraint on (fixture_id, team_id, period).
    """
    # Use the generic store_data function with use_insert_ignore=True
    return store_data(conn, "fixture_stats", stats_rows, primary_key_column="id", use_insert_ignore=True)


# --- Triggers ---
def create_update_trigger(conn, table_name, pk_column):
    """Creates a trigger to update 'updated_at' timestamp on row update."""
    # Based on original storage.py
    # This trigger works best with single-column primary keys.
    if not isinstance(pk_column, str) or not pk_column:
        logging.warning(f"Skipping trigger creation for {table_name}: pk_column must be a non-empty string.")
        return

    trigger_name = f"update_{table_name}_updated_at"
    # Use IF NOT EXISTS for trigger creation
    # The WHEN clause prevents the trigger from firing during INSERT OR REPLACE's internal UPDATE
    # It should only fire on explicit UPDATE statements where the PK doesn't change.
    # Added check for OLD.updated_at = NEW.updated_at to prevent infinite loops if updated_at is manually set
    # Adjusted WHEN clause: Trigger fires if updated_at is NOT being manually set to a different value
    # OR if updated_at is not present in the UPDATE statement (handled implicitly by SQLite)
    sql = f"""
    CREATE TRIGGER IF NOT EXISTS {trigger_name}
    AFTER UPDATE ON {table_name}
    FOR EACH ROW
    WHEN OLD.{pk_column} = NEW.{pk_column} AND NEW.updated_at = OLD.updated_at
    BEGIN
        UPDATE {table_name}
        SET updated_at = CURRENT_TIMESTAMP
        WHERE {pk_column} = OLD.{pk_column};
    END;
    """
    # --- Alternative WHEN clause (simpler, might fire slightly more often but should be safe):
    # sql = f"""
    # CREATE TRIGGER IF NOT EXISTS {trigger_name}
    # AFTER UPDATE ON {table_name}
    # FOR EACH ROW
    # WHEN OLD.{pk_column} = NEW.{pk_column}
    # BEGIN
    #     UPDATE {table_name}
    #     SET updated_at = CURRENT_TIMESTAMP
    #     WHERE {pk_column} = OLD.{pk_column} AND updated_at != CURRENT_TIMESTAMP; -- Prevent self-triggering loop
    # END;
    # """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        logging.info(f"Trigger '{trigger_name}' ensured for table '{table_name}'.")
    except sqlite3.Error as e:
        # Avoid logging "already exists" if IF NOT EXISTS is used, but log other errors
        # Note: Some older SQLite versions might not support IF NOT EXISTS for triggers
        if "already exists" not in str(e).lower():
            logging.error(f"Database error creating trigger {trigger_name}: {e}")
            # Rollback might not be necessary here, but doesn't hurt
            try:
                conn.rollback()
            except sqlite3.Error as rb_err:
                 logging.error(f"Rollback failed after trigger error: {rb_err}")

    finally:
        if cursor:
            cursor.close()
