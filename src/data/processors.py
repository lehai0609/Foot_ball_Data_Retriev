# src/data/processors.py
from datetime import datetime
import logging

# Configure basic logging if not done elsewhere
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# --- REVISED Schedule Processor ---

# Mapping from SportMonks state_id to standardized status codes
# NOTE: This is a sample mapping based on common states.
# You MUST verify and complete this mapping using the official SportMonks documentation.
STATE_ID_TO_STATUS = {
    1: 'NS',        # Not Started
    2: 'LIVE',      # In Progress
    3: 'HT',        # Half Time
    4: 'ET',        # Extra Time
    5: 'FT',        # Finished
    6: 'FT_PEN',    # Finished after Penalties
    7: 'POST',      # Postponed
    8: 'CANC',      # Cancelled
    9: 'ABD',       # Abandoned
    10: 'AWD',      # Awarded (Walkover)
    11: 'INT',      # Interrupted
    # Add other states as needed from SportMonks docs (TBA, DELAYED, etc.)
    17: 'TBA',      # To Be Announced (Time)
    18: 'DEL',      # Delayed
    # ... add more mappings ...
}

def process_schedule_detailed(raw_schedule_data):
    """
    Processes schedule data (from /schedules/seasons/{id}) to extract detailed
    fixture information for the enhanced schedules table.
    """
    processed_schedule_entries = []
    if not raw_schedule_data or 'data' not in raw_schedule_data or not isinstance(raw_schedule_data['data'], list):
        logging.warning("Invalid or empty schedule data received for detailed processing.")
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
            season_id = round_data.get("season_id") # Season ID is at round level
            round_finished = round_data.get("finished", False) # Default to False

            # Need at least round_id and season_id to link fixtures
            if not round_id or not season_id:
                # logging.debug(f"Skipping round due to missing round_id or season_id: RoundID={round_id}, SeasonID={season_id}")
                continue

            # Check if 'fixtures' exist and is a list
            if 'fixtures' in round_data and isinstance(round_data['fixtures'], list):
                for fixture_data in round_data['fixtures']:
                    if not isinstance(fixture_data, dict):
                        logging.warning(f"Skipping invalid fixture data (not a dict) in round {round_id}")
                        continue

                    fixture_id = fixture_data.get('id')
                    if not fixture_id:
                        logging.warning(f"Skipping fixture in round {round_id} due to missing fixture_id.")
                        continue

                    # --- Extract Enhanced Fields ---
                    league_id = fixture_data.get('league_id')
                    start_time = fixture_data.get('starting_at') # 'YYYY-MM-DD HH:MM:SS'
                    state_id = fixture_data.get('state_id')
                    status = STATE_ID_TO_STATUS.get(state_id, 'UNKNOWN') # Map state_id to status
                    result_info = fixture_data.get('result_info')

                    home_team_id = None
                    away_team_id = None
                    home_winner = None # Track winner flag if available
                    away_winner = None

                    participants = fixture_data.get('participants', [])
                    if isinstance(participants, list) and len(participants) == 2:
                        for p in participants:
                            if isinstance(p, dict):
                                p_meta = p.get('meta', {})
                                p_id = p.get('id')
                                if isinstance(p_meta, dict) and p_id:
                                    location = p_meta.get('location')
                                    winner_flag = p_meta.get('winner') # Can be True, False, or None
                                    if location == 'home':
                                        home_team_id = p_id
                                        home_winner = winner_flag
                                    elif location == 'away':
                                        away_team_id = p_id
                                        away_winner = winner_flag
                    else:
                         logging.warning(f"Fixture {fixture_id}: Invalid or missing participants data.")
                         # Skip if we can't determine teams
                         continue

                    # Ensure both teams were found
                    if not home_team_id or not away_team_id:
                        logging.warning(f"Fixture {fixture_id}: Could not determine home/away team IDs.")
                        continue

                    home_score = None
                    away_score = None
                    scores = fixture_data.get('scores', [])
                    if isinstance(scores, list):
                        # Find the score entry representing the final/current score
                        # Prioritize 'CURRENT', then '2ND_HALF' (or others like 'ET', 'PEN')
                        final_score_entry_home = None
                        final_score_entry_away = None
                        score_type_preference = ['CURRENT', 'PENALTIES', 'AET', '2ND_HALF', '1ST_HALF'] # Order of preference

                        for score_type in score_type_preference:
                            for s in scores:
                                if isinstance(s, dict) and s.get('description') == score_type:
                                    s_participant_id = s.get('participant_id')
                                    s_goals = s.get('score', {}).get('goals')
                                    if s_goals is not None: # Score can be 0
                                        if s_participant_id == home_team_id and final_score_entry_home is None:
                                            final_score_entry_home = s_goals
                                        elif s_participant_id == away_team_id and final_score_entry_away is None:
                                            final_score_entry_away = s_goals
                            # Stop if we found both for this score type
                            if final_score_entry_home is not None and final_score_entry_away is not None:
                                home_score = final_score_entry_home
                                away_score = final_score_entry_away
                                break # Exit preference loop

                    # Determine standardized result ('H', 'D', 'A') if scores are available
                    result = None
                    if home_score is not None and away_score is not None:
                        if home_score > away_score:
                            result = 'H'
                        elif away_score > home_score:
                            result = 'A'
                        else:
                            result = 'D'
                    # Fallback using winner flag if scores are missing but winner is known (less reliable)
                    elif result is None and status in ['FT', 'AET', 'FT_PEN', 'AWD']:
                         if home_winner is True and away_winner is False:
                             result = 'H'
                         elif away_winner is True and home_winner is False:
                             result = 'A'
                         elif home_winner is False and away_winner is False: # Check for explicit non-winners for draw
                             result = 'D'
                         # else: winner flags might be null or inconsistent

                    # --- Assemble Processed Entry ---
                    processed_schedule_entries.append({
                        "fixture_id": fixture_id,
                        "season_id": season_id,
                        "league_id": league_id,
                        "round_id": round_id,
                        "home_team_id": home_team_id,
                        "away_team_id": away_team_id,
                        "start_time": start_time,
                        "status": status,
                        "home_score": home_score,
                        "away_score": away_score,
                        "result": result,
                        "result_info": result_info,
                        "round_finished": round_finished
                    })

            # else:
                # logging.debug(f"No 'fixtures' list found in round {round_id}.")

    logging.info(f"Processed {len(processed_schedule_entries)} detailed schedule entries.")
    return processed_schedule_entries

# --- Fixture Stats Processor (Long Format - REVISED) ---

# Mapping from API stat codes to database column names (REVISED & ALIGNED)
# Keys are the exact codes from the API list you provided.
# Values are the corresponding database column names using underscores.
STAT_CODE_TO_DB_COLUMN = {
    'shots-insidebox': 'shots_insidebox',
    'shots-on-target': 'shots_on_target',
    'shots-blocked': 'shots_blocked',
    'shots-total': 'shots_total',
    'successful-dribbles': 'successful_dribbles',
    'goals': 'goals',
    'successful-dribbles-percentage': 'successful_dribbles_percentage',
    'successful-passes': 'successful_passes',
    'successful-passes-percentage': 'successful_passes_percentage',
    'ball-possession': 'ball_possession',
    'redcards': 'red_cards', # Changed to underscore
    'tackles': 'tackles',     # New stat
    'substitutions': 'substitutions',
    'interceptions': 'interceptions', # New stat
    'shots-outsidebox': 'shots_outsidebox',
    'dribble-attempts': 'dribble_attempts',
    'yellowcards': 'yellow_cards', # Changed to underscore
    'throwins': 'throwins',
    'assists': 'assists',
    'accurate-crosses': 'accurate_crosses',
    'corners': 'corners',
    'saves': 'saves',
    'fouls': 'fouls',
    'total-crosses': 'total_crosses', # Changed to underscore
    'hit-woodwork': 'hit_woodwork',
    'long-passes': 'long_passes',
    'penalties': 'penalties',
    'passes': 'passes', # Kept as is (matches 'passes-total' intent if API code is just 'passes') - *Verify API code if needed*
    'shots-off-target': 'shots_off_target',
    'attacks': 'attacks',
    'key-passes': 'key_passes', # Changed to underscore
    'offsides': 'offsides',
    'challenges': 'challenges',
    'goals-kicks': 'goal_kicks', # Changed API code from list ('goals-kicks' -> goal_kicks)
    'dangerous-attacks': 'dangerous_attacks'
}

# Define expected Python types for database columns (REVISED & ALIGNED)
# Keys MUST exactly match the database column names (values from STAT_CODE_TO_DB_COLUMN).
# Values are the target Python types for conversion.
DB_COLUMN_TYPES = {
    'shots_insidebox': int,
    'shots_on_target': int,
    'shots_blocked': int,
    'shots_total': int,
    'successful_dribbles': int,
    'goals': int,
    'successful_dribbles_percentage': float, # Percentage
    'successful_passes': int,
    'successful_passes_percentage': float, # Percentage
    'ball_possession': float,               # Percentage
    'red_cards': int,
    'tackles': int,                         # New stat - Assuming int
    'substitutions': int,
    'interceptions': int,                   # New stat - Assuming int
    'shots_outsidebox': int,
    'dribble_attempts': int,
    'yellow_cards': int,
    'throwins': int,
    'assists': int,
    'accurate_crosses': int,
    'corners': int,
    'saves': int,
    'fouls': int,
    'total_crosses': int,
    'hit_woodwork': int,
    'long_passes': int,
    'penalties': int,
    'passes': int,
    'shots_off_target': int,
    'attacks': int,
    'key_passes': int,
    'offsides': int,
    'challenges': int,
    'goal_kicks': int,                      # Changed from 'goals-kicks'
    'dangerous_attacks': int
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

# --- Deprecated simple processor ---
# def process_schedule_simple(raw_schedule_data):
#     """DEPRECATED: Processes schedule data to extract fixture_id, season_id, round_id, round_finished."""
#     # Based on original processors.py
#     processed_schedule_entries = []
#     # ... (original logic) ...
#     logging.warning("Using deprecated process_schedule_simple function.")
#     return processed_schedule_entries
