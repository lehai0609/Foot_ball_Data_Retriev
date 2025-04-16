# src/data/storage.py
import sqlite3
from src.config import DATABASE_PATH

# --- Database Setup ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        # conn.execute("PRAGMA foreign_keys = ON") # Optional: Enforce foreign keys
        print(f"Connected to database: {DATABASE_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def create_table(conn, create_table_sql):
     """Creates a table using the provided SQL if it doesn't exist."""
     cursor = None
     try:
         cursor = conn.cursor()
         cursor.execute(create_table_sql)
         conn.commit()
     except sqlite3.Error as e:
         print(f"Database error during table creation: {e}")
         conn.rollback()
         return False
     finally:
        if cursor:
            cursor.close()
     return True

# --- Table Creation Functions ---

def create_leagues_table(conn):
    """Creates the leagues table."""
    sql = """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id INTEGER PRIMARY KEY, sport_id INTEGER, country_id INTEGER, name TEXT NOT NULL, active BOOLEAN DEFAULT 1,
        short_code TEXT, image_path TEXT, type TEXT, sub_type TEXT, last_played_at TEXT, category INTEGER, current_season_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): print("Leagues table ensured.")
    else: print("Failed to ensure leagues table.")

def create_seasons_table(conn):
    """Creates the seasons table."""
    sql = """
    CREATE TABLE IF NOT EXISTS seasons (
        season_id INTEGER PRIMARY KEY, league_id INTEGER NOT NULL, sport_id INTEGER, name TEXT NOT NULL, is_current BOOLEAN DEFAULT 0,
        finished BOOLEAN DEFAULT 0, pending BOOLEAN DEFAULT 0, starting_at TEXT, ending_at TEXT, standings_recalculated_at TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): print("Seasons table ensured.")
    else: print("Failed to ensure seasons table.")

def create_teams_table(conn):
    """Creates the teams table."""
    sql = """
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY, name TEXT NOT NULL, short_code TEXT, country_id INTEGER, logo_url TEXT,
        venue_id INTEGER, founded INTEGER, type TEXT, national_team BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): print("Teams table ensured.")
    else: print("Failed to ensure teams table.")

def create_schedules_table(conn):
    """Creates a simplified schedules table linking fixtures to seasons and rounds."""
    sql = """
    CREATE TABLE IF NOT EXISTS schedules (
        fixture_id INTEGER PRIMARY KEY, season_id INTEGER NOT NULL, round_id INTEGER, round_finished BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    if create_table(conn, sql): print("Schedules table ensured.")
    else: print("Failed to ensure schedules table.")

# --- Fixture Stats Table (Long Format - based on planning doc) ---

def create_fixture_stats_table(conn):
    """Creates the fixture_stats table based on Database planning.txt (long format)."""
    # Schema based on Database planning.txt, ensuring stats columns are nullable or have defaults
    sql = """
    CREATE TABLE IF NOT EXISTS fixture_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing primary key for each stat row
        fixture_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,           -- participant_id from the API stat item
        period TEXT NOT NULL,               -- e.g., 'first_half', 'second_half', 'full_match'
        goals INTEGER DEFAULT 0,            -- Mapped from stat code 'goals'
        shots_on_target INTEGER DEFAULT 0,  -- Mapped from 'shots-on-target'
        shots_off_target INTEGER DEFAULT 0, -- Mapped from 'shots-off-target'
        possession REAL DEFAULT NULL,       -- Mapped from 'ball-possession' (allow NULL)
        corners INTEGER DEFAULT 0,          -- Mapped from 'corners'
        fouls INTEGER DEFAULT 0,            -- Mapped from 'fouls'
        yellow_cards INTEGER DEFAULT 0,     -- Mapped from 'yellowcards'
        red_cards INTEGER DEFAULT 0,        -- Mapped from 'redcards'
        -- Add other columns from your planning doc as needed, ensure they allow NULL or have DEFAULT
        shots_total INTEGER DEFAULT 0,
        shots_blocked INTEGER DEFAULT 0,
        offsides INTEGER DEFAULT 0,
        saves INTEGER DEFAULT 0,
        hit_woodwork INTEGER DEFAULT 0,
        timestamp DATETIME,                 -- Timestamp when the stat was recorded/fetched (optional)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (fixture_id, team_id, period) -- Ensure uniqueness for a specific stat record
    );"""
    if create_table(conn, sql): print("Fixture_Stats (long) table ensured.")
    else: print("Failed to ensure fixture_stats table.")


# --- Data Storage ---

def store_data(conn, table_name, data_list, primary_key_column="id"):
    """
    Generic function to insert/replace data into a specified table using a single primary key.
    Assumes data_list is a list of dictionaries where keys match column names.
    Uses INSERT OR REPLACE.
    """
    if not data_list:
        print(f"No processed data provided for table {table_name}.")
        return 0

    cursor = conn.cursor()
    inserted_count = 0
    replaced_count = 0
    skipped_count = 0

    first_valid_item = next((item for item in data_list if item), None)
    if not first_valid_item:
        print(f"No valid data items found for table {table_name}.")
        if cursor: cursor.close()
        return 0

    columns = list(first_valid_item.keys())
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"

    try:
        for item in data_list:
            if not item:
                skipped_count += 1
                continue
            values = [item.get(col) for col in columns]

            pk_value = item.get(primary_key_column)
            exists = False
            if pk_value is not None:
                 try:
                     cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key_column} = ?", (pk_value,))
                     exists = cursor.fetchone()
                 except sqlite3.Error as check_e:
                     print(f"Warning: Could not check existence for PK {pk_value} in {table_name}: {check_e}")

            cursor.execute(sql, values)
            if exists: replaced_count += 1
            else: inserted_count += 1

        conn.commit()
        print(f"Successfully stored data into {table_name}. Inserted: {inserted_count}, Replaced/Updated: {replaced_count}, Skipped: {skipped_count}")
        return inserted_count + replaced_count
    except sqlite3.Error as e:
        print(f"Database error during storage in {table_name}: {e}")
        print(f"SQL attempted: {sql}")
        print(f"Item causing error (potentially): {item}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def store_fixture_stats_long(conn, stats_rows):
    """
    Stores processed fixture statistics rows (long format) into the fixture_stats table.
    Uses INSERT OR IGNORE to handle the UNIQUE constraint on (fixture_id, team_id, period).
    """
    if not stats_rows:
        print("No processed fixture stats provided to store.")
        return 0

    cursor = conn.cursor()
    inserted_count = 0
    ignored_count = 0
    skipped_count = 0
    table_name = "fixture_stats"

    # Get columns from the first valid item, excluding the auto-increment 'id'
    first_valid_item = next((item for item in stats_rows if item), None)
    if not first_valid_item:
        print(f"No valid data items found for table {table_name}.")
        if cursor: cursor.close()
        return 0

    # Exclude 'id' if it exists, as it's auto-incremented
    columns = [col for col in first_valid_item.keys() if col != 'id']
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
    # Example: INSERT OR IGNORE INTO fixture_stats (fixture_id, team_id, period, goals, ...) VALUES (?, ?, ?, ?, ...)

    try:
        for row in stats_rows:
            if not row:
                skipped_count += 1
                continue

            # Prepare values in the correct order corresponding to columns
            values = [row.get(col) for col in columns]

            cursor.execute(sql, values)
            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                ignored_count += 1 # Row was ignored, likely due to UNIQUE constraint violation

        conn.commit()
        print(f"Successfully processed storage for {table_name}. Inserted: {inserted_count}, Ignored (duplicates/invalid): {ignored_count + skipped_count}")
        return inserted_count
    except sqlite3.Error as e:
        print(f"Database error during storage in {table_name}: {e}")
        print(f"SQL attempted: {sql}")
        print(f"Sample row causing error (potentially): {row if 'row' in locals() else 'N/A'}")
        conn.rollback()
        return 0
    finally:
        cursor.close()


# --- Triggers ---
def create_update_trigger(conn, table_name, pk_column):
    """Creates a trigger to update 'updated_at' timestamp on row update."""
    # This trigger works best with single-column primary keys.
    # For fixture_stats (long), the UNIQUE constraint handles upserts better via INSERT OR IGNORE/ON CONFLICT.
    # We might still want an update trigger based on the auto-increment 'id' if we perform manual UPDATEs later.
    if not isinstance(pk_column, str):
        print(f"Skipping trigger creation for {table_name}: pk_column must be a string.")
        return

    trigger_name = f"update_{table_name}_updated_at"
    sql = f"""
    CREATE TRIGGER IF NOT EXISTS {trigger_name}
    AFTER UPDATE ON {table_name}
    FOR EACH ROW
    WHEN OLD.{pk_column} = NEW.{pk_column}
    BEGIN
        UPDATE {table_name} SET updated_at = CURRENT_TIMESTAMP WHERE {pk_column} = OLD.{pk_column};
    END;
    """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f"Trigger '{trigger_name}' ensured for table '{table_name}'.")
    except sqlite3.Error as e:
        if "already exists" not in str(e):
            print(f"Database error creating trigger {trigger_name}: {e}")
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

