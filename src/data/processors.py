import json
import pandas as pd
from pathlib import Path
from src.config import LEAGUES_RAW_DIR, LEAGUES_PROCESSED_DIR

def process_leagues_data():
    """
    Process the downloaded leagues data into a table format with
    id, country_id, name, and short_code.
    
    Returns:
        pandas.DataFrame: Processed leagues data
    """
    leagues_file = LEAGUES_RAW_DIR / "all_leagues.json"
    
    # Check if the file exists
    if not leagues_file.exists():
        print("Leagues data file not found. Download the data first.")
        return None
    
    print("Processing leagues data...")
    
    # Load the downloaded data
    with open(leagues_file, "r") as f:
        leagues_data = json.load(f)
    
    # Extract required fields
    leagues_table = []
    for league in leagues_data:
        league_info = {
            "id": league.get("id"),
            "name": league.get("name"),
            "short_code": league.get("code"),
            "country_id": None
        }
        
        # Extract country_id from the relationships if available
        if "country" in league.get("relationships", {}):
            country_data = league.get("relationships", {}).get("country", {}).get("data")
            if country_data:
                league_info["country_id"] = country_data.get("id")
        
        leagues_table.append(league_info)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(leagues_table)
    
    return df