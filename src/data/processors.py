# src/data/processors.py
from datetime import datetime
import logging

# Configure basic logging (ensure it's configured somewhere in your project)
# Example: logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# If not configured elsewhere, uncomment the line above or configure appropriately.

# --- Existing Processors ---
def process_league_data(raw_league_data):
    """Transforms raw league API data for database insertion."""
    # Based on original processors.py
    if not raw_league_data or not isinstance(raw_league_data, dict):
        logging.warning("Invalid raw league data received.")
        return None

    current_season_id = None
    # Safely access nested 'currentseason' dictionary and its 'id'
    current_season_info = raw_league_data.get("currentseason")
    if current_season_info and isinstance(current_season_info, dict):
        current_season_id = current_season_info.get("id")

    processed = {
        "league_id": raw_league_data.get("id"),
        "sport_id": raw_league_data.get("sport_id"),
        "country_id": raw_league_data.get("country_id"),
        "name": raw_league_data.get("name"),
        "active": raw_league_data.get("active"),
        "short_code": raw_league_data.get("short_code"),
        "image_path": raw_league_data.get("image_path"),
        "type": raw_league_data.get("type"),
        "sub_type": raw_league_data.get("sub_type"),
        "last_played_at": raw_league_data.get("last_played_at"),
        "category": raw_league_data.get("category"),
        "current_season_id": current_season_id
    }
    # Basic validation
    if not processed["league_id"] or not processed["name"]:
        logging.warning(f"Skipping league due to missing ID or name: ID={raw_league_data.get('id')}, Name={raw_league_data.get('name')}")
        return None
    return processed

def process_season_data(raw_season_data, league_name=None):
    """Transforms raw season API data for database insertion."""
    # Based on original processors.py
    if not raw_season_data or not isinstance(raw_season_data, dict):
        logging.warning("Invalid raw season data received.")
        return None

    processed = {
        "season_id": raw_season_data.get("id"),
        "league_id": raw_season_data.get("league_id"),
        "league_name": league_name, # Passed in from context (e.g., sync_leagues)
        "sport_id": raw_season_data.get("sport_id"),
        "name": raw_season_data.get("name"),
        "is_current": raw_season_data.get("is_current"),
        "finished": raw_season_data.get("finished"),
        "pending": raw_season_data.get("pending"),
        "starting_at": raw_season_data.get("starting_at"),
        "ending_at": raw_season_data.get("ending_at"),
        "standings_recalculated_at": raw_season_data.get("standings_recalculated_at")
    }
    # Basic validation
    if not processed["season_id"] or not processed["league_id"] or not processed["name"]:
        logging.warning(f"Skipping season due to missing ID, league_id, or name: ID={raw_season_data.get('id')}, LeagueID={raw_season_data.get('league_id')}, Name={raw_season_data.get('name')}")
        return None
    return processed

def process_team_data(raw_team_data):
    """Transforms raw team API data for database insertion."""
    # Based on original processors.py
    if not raw_team_data or not isinstance(raw_team_data, dict):
        logging.warning("Invalid raw team data received.")
        return None

    processed = {
        "team_id": raw_team_data.get("id"),
        "name": raw_team_data.get("name"),
        "short_code": raw_team_data.get("short_code"),
        "country_id": raw_team_data.get("country_id"),
        "logo_url": raw_team_data.get("image_path"), # Map image_path to logo_url
        "venue_id": raw_team_data.get("venue_id"),
        "founded": raw_team_data.get("founded"),
        "type": raw_team_data.get("type"),
        "national_team": raw_team_data.get("national_team", False) # Default to False if missing
    }
    # Basic validation
    if not processed["team_id"] or not processed["name"]:
        logging.warning(f"Skipping team due to missing ID or name: ID={raw_team_data.get('id')}, Name={raw_team_data.get('name')}")
        return None
    return processed

def process_schedule_simple(raw_schedule_data):
    """Processes schedule data to extract fixture_id, season_id, round_id, round_finished."""
    # Based on original processors.py
    processed_schedule_entries = []
    if not raw_schedule_data or 'data' not in raw_schedule_data or not isinstance(raw_schedule_data['data'], list):
        logging.warning("Invalid or empty schedule data received.")
        return processed_schedule_entries

    # The schedule endpoint structure seems to be a list of stages, each containing rounds, each containing fixtures.
    for stage_data in raw_schedule_data['data']:
        # Check if stage_data is a dictionary and contains 'rounds'
        if not isinstance(stage_data, dict) or 'rounds' not in stage_data or not isinstance(stage_data['rounds'], list):
            # logging.debug(f"Skipping stage due to missing or invalid 'rounds': {stage_data.get('id', 'N/A')}")
            continue

        for round_data in stage_data['rounds']:
            # Check if round_data is a dictionary and contains necessary keys
            if not isinstance(round_data, dict):
                # logging.debug("Skipping invalid round data (not a dict).")
                continue

            round_id = round_data.get("id")
            season_id = round_data.get("season_id")
            round_finished = round_data.get("finished") # Can be True/False or potentially missing

            # Need at least round_id and season_id to link fixtures
            if not round_id or not season_id:
                # logging.debug(f"Skipping round due to missing round_id or season_id: RoundID={round_id}, SeasonID={season_id}")
                continue

            # Check if 'fixtures' exist and is a list
            if 'fixtures' in round_data and isinstance(round_data['fixtures'], list):
                for fixture_data in round_data['fixtures']:
                    # Check if fixture_data is a dictionary and has an 'id'
                     if isinstance(fixture_data, dict) and 'id' in fixture_data:
                         fixture_id = fixture_data.get('id')
                         if fixture_id: # Ensure fixture_id is not None or 0
                             processed_schedule_entries.append({
                                 "fixture_id": fixture_id,
                                 "season_id": season_id,
                                 "round_id": round_id,
                                 "round_finished": round_finished # Store whatever value is provided (True/False/None)
                             })
                         # else:
                             # logging.debug(f"Skipping fixture in round {round_id} due to missing fixture_id.")
                     # else:
                         # logging.debug(f"Skipping invalid fixture data in round {round_id} (not a dict or missing 'id').")
            # else:
                # logging.debug(f"No 'fixtures' list found in round {round_id}.")

    logging.info(f"Processed {len(processed_schedule_entries)} schedule entries.")
    return processed_schedule_entries


# --- Fixture Stats Processor (Long Format - REVISED) ---

# Mapping from API stat codes to database column names (REVISED)
# !!! YOU MUST VERIFY THE API CODES MARKED WITH 'TODO' AGAINST SPORTMONKS DOCS !!!
STAT_CODE_TO_DB_COLUMN = {
    # Standard Stats (likely correct codes)
    'goals': 'goals',
    'corners': 'corners',
    'ball-possession': 'ball-possession', # Note DB column name change
    'shots-total': 'shots-total',
    'shots-on-target': 'shots-on-target',
    'shots-off-target': 'shots-off-target',
    'shots-blocked': 'shots-blocked',
    'fouls': 'fouls',
    'yellowcards': 'yellowcards',
    'redcards': 'redcards',
    'offsides': 'offsides',
    'saves': 'saves',
    'hit-woodwork': 'hit-woodwork',
    'substitutions': 'substitutions',
    'shots-insidebox': 'shots_insidebox',
    'successful-dribbles': 'successful-dribbles',
    'successful-dribbles-percentage': 'successful-dribbles-percentage',
    'successful-passes': 'successful-passes',
    'successful-passes-percentage': 'successful-passes-percentage',
    'shots-outsidebox': 'shots-outsidebox',
    'dribble-attempts': 'dribble-attempts',
    'throwins': 'throwins',
    'assists': 'assists',
    'accurate-crosses': 'accurate-crosses',
    'crosses-total': 'total-crosses',
    'penalties': 'penalties',
    'passes-total': 'passes', 
    'attacks': 'attacks',
    'challenges': 'challenges',
    'key-passes': 'key-passes',
    'dangerous-attacks': 'dangerous-attacks',
}

# Define expected Python types for database columns (REVISED)
# Ensure this matches your new schema exactly
DB_COLUMN_TYPES = {
    'goals': int,
    'shots-on-target': int,
    'shots-off-target': int,
    'ball-possession': float, # Percentage
    'corners': int,
    'fouls': int,
    'yellowcards': int,
    'redcards': int,
    'shots_total': int,
    'shots_blocked': int,
    'offsides': int,
    'saves': int,
    'hit-woodwork': int,
    'shots_insidebox': int,
    'successful-dribbles': int,
    'successful-dribbles-percentage': float, # Percentage
    'successful-passes': int,
    'successful-passes-percentage': float, # Percentage
    'shots-outsidebox': int,
    'dribble-attempts': int,
    'throwins': int,
    'assists': int,
    'accurate-crosses': int,
    'total-crosses': int,
    'penalties': int,
    'passes': int,
    'attacks': int,
    'challenges': int,
    'key-passes': int,
    'dangerous-attacks': int,
    'substitutions': int,
}


def map_period_description(description):
    """Maps API period description to database period string."""
    # Existing mapping, check if SportMonks provides 'full_match' or others
    if not description: return None # Handle null description
    desc_lower = description.lower()
    if "1st-half" in desc_lower or "first half" in desc_lower : return "first_half"
    if "2nd-half" in desc_lower or "second half" in desc_lower: return "second_half"
    if "extra-time" in desc_lower: return "extra_time" # Handle potential extra time stats
    if "penalties" in desc_lower: return "penalties"   # Handle potential penalty shootout stats
    # If API provides a period like 'Full-Time', map it here:
    # if "full-time" in desc_lower or "full time" in desc_lower: return "full_match" # Example
    logging.debug(f"Unmapped period description: {description}")
    return description # Return original if no specific mapping


def process_fixture_stats_long(raw_fixture_data):
    """
    Processes the raw response from the /fixtures/{id}?include=periods.statistics.type endpoint
    into a 'long' format list suitable for the revised fixture_stats table.
    Handles missing stats gracefully.
    """
    processed_rows = []
    if not raw_fixture_data or 'data' not in raw_fixture_data:
        logging.warning("Invalid or empty fixture data received for stats processing.")
        return processed_rows

    fixture_main_data = raw_fixture_data['data']
    # Ensure main data is a dict
    if not isinstance(fixture_main_data, dict):
        logging.warning("Fixture data is not a dictionary.")
        return processed_rows

    fixture_id = fixture_main_data.get('id')
    if not fixture_id:
        logging.warning("Fixture data missing ID.")
        return processed_rows

    fetch_time = datetime.now().isoformat() # Timestamp for the 'timestamp' column

    if 'periods' not in fixture_main_data or not isinstance(fixture_main_data['periods'], list):
        logging.info(f"No periods array found for fixture {fixture_id}. No stats to process.")
        return processed_rows

    # Process stats for each period
    for period_data in fixture_main_data['periods']:
        if not isinstance(period_data, dict):
            logging.warning(f"Skipping invalid period data (not a dict) in fixture {fixture_id}")
            continue

        period_id = period_data.get('id')
        # Use helper function to map standard descriptions
        period_desc = map_period_description(period_data.get('description'))

        if not period_id or not period_desc:
            logging.warning(f"Skipping period due to missing id or unmapped description ('{period_data.get('description')}') in fixture {fixture_id}")
            continue

        # Group stats by participant (team) within this period
        stats_by_team = {} # {team_id: {stat_code: value}}
        if 'statistics' in period_data and isinstance(period_data['statistics'], list):
            for stat_item in period_data['statistics']:
                if not isinstance(stat_item, dict):
                    logging.warning(f"Skipping invalid stat item (not a dict) in fixture {fixture_id}, period {period_desc}")
                    continue

                stat_type = stat_item.get('type')
                stat_data = stat_item.get('data')
                participant_id = stat_item.get('participant_id') # This is the team_id

                # Basic validation of stat item structure
                if not isinstance(stat_type, dict) or stat_data is None or not participant_id:
                    # logging.debug(f"Skipping invalid stat item structure in fixture {fixture_id}, period {period_desc}: {stat_item}")
                    continue

                stat_code = stat_type.get('code')
                # Value can be 0, so check 'value' key exists within stat_data dict
                value = stat_data.get('value') if isinstance(stat_data, dict) else None

                if participant_id not in stats_by_team:
                    stats_by_team[participant_id] = {}

                # Store the stat if the code is known and value is not None
                if stat_code and value is not None:
                    stats_by_team[participant_id][stat_code] = value
                # else:
                    # logging.debug(f"Skipping stat with code {stat_code} or value {value} for team {participant_id} in fixture {fixture_id}")


        # Create a row for each team in this period
        for team_id, team_stats in stats_by_team.items():
            # Initialize row with all possible columns from the DB schema mapping
            # Ensure all keys from DB_COLUMN_TYPES are present
            row = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "period": period_desc,
                "timestamp": fetch_time,
                 # Initialize all stat columns to None
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
                            # Specific handling for boolean-like stats if needed
                            # E.g., if API sends 1/0 for a boolean field mapped to INTEGER
                            # if target_type is bool and db_column == 'some_bool_column':
                            #     row[db_column] = bool(int(raw_value)) # Example
                            # else:
                            row[db_column] = target_type(raw_value)
                        except (ValueError, TypeError) as conv_err:
                            logging.warning(f"Could not convert value '{raw_value}' ({type(raw_value).__name__}) to type {target_type.__name__} for {db_column} (API code: {api_code}) in fixture {fixture_id}, team {team_id}, period {period_desc}. Error: {conv_err}. Setting to NULL.")
                            row[db_column] = None # Set to None on conversion error
                    else:
                        # Should not happen if DB_COLUMN_TYPES is comprehensive
                        row[db_column] = raw_value
                        logging.warning(f"No target type defined in DB_COLUMN_TYPES for column '{db_column}'. Storing raw value.")


            processed_rows.append(row)

    if not processed_rows and 'periods' in fixture_main_data and fixture_main_data['periods']:
         logging.info(f"No processable statistics found within periods for fixture {fixture_id}.")
    elif not processed_rows:
         # This case handles when 'periods' was missing or empty initially
         pass # Already logged earlier

    return processed_rows


# --- Placeholder functions for future implementation ---
# def process_fixture_details(raw_fixture_data): ... # To update main fixtures table
# def process_odds_data(raw_odds_data, fixture_id): ...
# def process_timeline_data(raw_event_data, fixture_id): ...