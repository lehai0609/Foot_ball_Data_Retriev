# src/data/processors.py
from datetime import datetime
import logging # Import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Existing Processors ---
def process_league_data(raw_league_data):
    """Transforms raw league API data for database insertion."""
    current_season_id = None
    if raw_league_data.get("currentseason") and isinstance(raw_league_data["currentseason"], dict):
        current_season_id = raw_league_data["currentseason"].get("id")
    processed = {
        "league_id": raw_league_data.get("id"), "sport_id": raw_league_data.get("sport_id"),
        "country_id": raw_league_data.get("country_id"), "name": raw_league_data.get("name"),
        "active": raw_league_data.get("active"), "short_code": raw_league_data.get("short_code"),
        "image_path": raw_league_data.get("image_path"), "type": raw_league_data.get("type"),
        "sub_type": raw_league_data.get("sub_type"), "last_played_at": raw_league_data.get("last_played_at"),
        "category": raw_league_data.get("category"), "current_season_id": current_season_id
    }
    if not processed["league_id"] or not processed["name"]:
        logging.warning(f"Skipping league due to missing ID or name: {raw_league_data.get('id')}")
        return None
    return processed

def process_season_data(raw_season_data):
    """Transforms raw season API data for database insertion."""
    if not raw_season_data or not isinstance(raw_season_data, dict): return None
    processed = {
        "season_id": raw_season_data.get("id"), "league_id": raw_season_data.get("league_id"),
        "sport_id": raw_season_data.get("sport_id"), "name": raw_season_data.get("name"),
        "is_current": raw_season_data.get("is_current"), "finished": raw_season_data.get("finished"),
        "pending": raw_season_data.get("pending"), "starting_at": raw_season_data.get("starting_at"),
        "ending_at": raw_season_data.get("ending_at"), "standings_recalculated_at": raw_season_data.get("standings_recalculated_at")
    }
    if not processed["season_id"] or not processed["league_id"] or not processed["name"]:
        logging.warning(f"Skipping season due to missing ID, league_id, or name: {raw_season_data.get('id')}")
        return None
    return processed

def process_team_data(raw_team_data):
    """Transforms raw team API data for database insertion."""
    processed = {
        "team_id": raw_team_data.get("id"), "name": raw_team_data.get("name"),
        "short_code": raw_team_data.get("short_code"), "country_id": raw_team_data.get("country_id"),
        "logo_url": raw_team_data.get("image_path"), "venue_id": raw_team_data.get("venue_id"),
        "founded": raw_team_data.get("founded"), "type": raw_team_data.get("type"),
        "national_team": raw_team_data.get("national_team", False)
    }
    if not processed["team_id"] or not processed["name"]:
        logging.warning(f"Skipping team due to missing ID or name: {raw_team_data.get('id')}")
        return None
    return processed

def process_schedule_simple(raw_schedule_data):
    """Processes schedule data to extract fixture_id, season_id, round_id, round_finished."""
    processed_schedule_entries = []
    if not raw_schedule_data or 'data' not in raw_schedule_data: return processed_schedule_entries
    for stage_data in raw_schedule_data['data']:
        if 'rounds' not in stage_data or not isinstance(stage_data['rounds'], list): continue
        for round_data in stage_data['rounds']:
            if not round_data or not isinstance(round_data, dict): continue
            round_id = round_data.get("id"); season_id = round_data.get("season_id")
            round_finished = round_data.get("finished");
            if not round_id or not season_id: continue
            if 'fixtures' in round_data and isinstance(round_data['fixtures'], list):
                for fixture_data in round_data['fixtures']:
                     if fixture_data and isinstance(fixture_data, dict) and 'id' in fixture_data:
                         fixture_id = fixture_data.get('id')
                         if fixture_id:
                             processed_schedule_entries.append({
                                 "fixture_id": fixture_id, "season_id": season_id,
                                 "round_id": round_id, "round_finished": round_finished
                             })
    return processed_schedule_entries

# --- Fixture Stats Processor (Long Format - Corrected) ---

# Mapping from API stat codes to database column names defined in Database planning.txt
# Add more mappings here as needed based on the table schema and API codes
STAT_CODE_TO_DB_COLUMN = {
    'goals': 'goals',
    'corners': 'corners',
    'ball-possession': 'possession',
    'shots-total': 'shots_total', # Example if you add this column
    'shots-on-target': 'shots_on_target',
    'shots-off-target': 'shots_off_target',
    'shots-blocked': 'shots_blocked', # Example if you add this column
    'fouls': 'fouls',
    'yellowcards': 'yellow_cards', # Note the underscore
    'redcards': 'red_cards',       # Note the underscore
    'offsides': 'offsides',         # Example if you add this column
    'saves': 'saves',               # Example if you add this column
    'hit-woodwork': 'hit_woodwork', # Example if you add this column
    # Add other relevant mappings...
}

# Define expected Python types for database columns (used for safe conversion)
# Add entries for any additional columns you add to fixture_stats table
DB_COLUMN_TYPES = {
    'goals': int,
    'corners': int,
    'possession': float,
    'shots_total': int,
    'shots_on_target': int,
    'shots_off_target': int,
    'shots_blocked': int,
    'fouls': int,
    'yellow_cards': int,
    'red_cards': int,
    'offsides': int,
    'saves': int,
    'hit_woodwork': int,
}


def map_period_description(description):
    """Maps API period description to database period string."""
    # Simple mapping, adjust based on how you want to store period info
    if description == "1st-half": return "first_half"
    if description == "2nd-half": return "second_half"
    if description == "extra-time": return "extra_time"
    if description == "penalties": return "penalties"
    # Consider how to handle full-time stats. Often, the stats in the *last*
    # period object (e.g., '2nd-half' or 'extra-time') represent the full match stats.
    # You might need to check the fixture state_id as well.
    # For simplicity, let's assume for now we only store stats explicitly listed per period.
    # You could calculate full_match stats later by summing half-time stats if needed.
    logging.debug(f"Unmapped period description: {description}")
    return description # Return original if no specific mapping

def process_fixture_stats_long(raw_fixture_data):
    """
    Processes the raw response from the /fixtures/{id}?include=periods.statistics.type endpoint
    into a 'long' format list suitable for the fixture_stats table.
    Each element in the list represents one row (fixture, team, period).
    Handles missing stats gracefully.
    """
    processed_rows = []
    if not raw_fixture_data or 'data' not in raw_fixture_data:
        logging.warning("Invalid or empty fixture data received for stats processing.")
        return processed_rows

    fixture_main_data = raw_fixture_data['data']
    fixture_id = fixture_main_data.get('id')
    if not fixture_id:
        logging.warning("Fixture data missing ID.")
        return processed_rows

    fetch_time = datetime.now().isoformat() # Timestamp for the 'timestamp' column

    if 'periods' not in fixture_main_data or not isinstance(fixture_main_data['periods'], list):
        logging.warning(f"No periods array found for fixture {fixture_id}.")
        return processed_rows

    # Process stats for each period
    for period_data in fixture_main_data['periods']:
        period_id = period_data.get('id')
        period_desc = map_period_description(period_data.get('description'))

        if not period_id or not period_desc:
            logging.warning(f"Skipping period due to missing id or description in fixture {fixture_id}")
            continue

        # Group stats by participant (team) within this period
        stats_by_team = {} # {team_id: {stat_code: value}}
        if 'statistics' in period_data and isinstance(period_data['statistics'], list):
            for stat_item in period_data['statistics']:
                stat_type = stat_item.get('type')
                stat_data = stat_item.get('data')
                participant_id = stat_item.get('participant_id')

                if not stat_type or not stat_data or not participant_id:
                    continue # Skip invalid stat item

                stat_code = stat_type.get('code')
                value = stat_data.get('value')

                if participant_id not in stats_by_team:
                    stats_by_team[participant_id] = {}

                if stat_code:
                    stats_by_team[participant_id][stat_code] = value

        # Create a row for each team in this period
        for team_id, team_stats in stats_by_team.items():
            row = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "period": period_desc,
                "timestamp": fetch_time,
                # Initialize all known stat columns to None
                **{col: None for col in DB_COLUMN_TYPES.keys()}
            }

            # Populate the row with available stats, mapping code to db column name
            for api_code, db_column in STAT_CODE_TO_DB_COLUMN.items():
                if api_code in team_stats:
                    raw_value = team_stats[api_code]
                    target_type = DB_COLUMN_TYPES.get(db_column)

                    if raw_value is None:
                        row[db_column] = None # Keep None if API value is None
                        continue

                    # Attempt safe type conversion
                    if target_type:
                        try:
                            row[db_column] = target_type(raw_value)
                        except (ValueError, TypeError):
                            logging.warning(f"Could not convert value '{raw_value}' to type {target_type.__name__} for {db_column} in fixture {fixture_id}, team {team_id}, period {period_desc}. Setting to NULL.")
                            row[db_column] = None # Set to None on conversion error
                    else:
                        # If type not defined in DB_COLUMN_TYPES, store as is (likely TEXT)
                        row[db_column] = raw_value

            processed_rows.append(row)

    return processed_rows


# --- Placeholder functions for future implementation ---
# def process_fixture_details(raw_fixture_data): ... # To update main fixtures table
# def process_odds_data(raw_odds_data, fixture_id): ...
# def process_timeline_data(raw_event_data, fixture_id): ...
