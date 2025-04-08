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
        # Updated to match the actual API response structure from documentation
        league_info = {
            "id": league.get("id"),
            "name": league.get("name"),
            "short_code": league.get("short_code"),
            "country_id": league.get("country_id"),  # Get country_id directly from the league object
            "sport_id": league.get("sport_id"),
            "active": league.get("active"),
            "type": league.get("type"),
            "sub_type": league.get("sub_type"),
            "last_played_at": league.get("last_played_at"),
            "category": league.get("category")
        }
        
        leagues_table.append(league_info)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(leagues_table)
    
    # Print summary of data loaded
    print(f"Processed {len(df)} leagues")
    if not df.empty:
        print(f"Columns in dataset: {', '.join(df.columns.tolist())}")
        print(f"Missing values check:")
        for col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                print(f"  - {col}: {null_count} missing values")
    
    return df