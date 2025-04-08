from datetime import datetime
from src.config import LEAGUES_PROCESSED_DIR

def export_to_csv(df, name="leagues", include_timestamp=True):
    """
    Export DataFrame to CSV.
    
    Args:
        df (pandas.DataFrame): Data to export
        name (str): Base name for the file
        include_timestamp (bool): Whether to include timestamp in filename
        
    Returns:
        Path: Path to the saved file
    """
    if include_timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.csv"
    else:
        filename = f"{name}.csv"
    
    file_path = LEAGUES_PROCESSED_DIR / filename
    df.to_csv(file_path, index=False)
    
    print(f"Exported {len(df)} rows to {file_path}")
    return file_path