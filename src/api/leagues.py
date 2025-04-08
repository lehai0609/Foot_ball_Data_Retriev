import json
from pathlib import Path
from src.api.client import APIClient
from src.config import LEAGUES_RAW_DIR

class LeaguesAPI:
    """Handler for the leagues endpoint."""
    
    def __init__(self):
        self.client = APIClient()
        # Fixed endpoint to match official API structure - removed extra "api/" segment
        self.endpoint = "v3/football/leagues"
    
    def get_all_leagues(self, include="country", per_page=100):
        """
        Download all leagues data from the API.
        
        Args:
            include (str): Related data to include
            per_page (int): Number of items per page
            
        Returns:
            list: All leagues data
        """
        all_leagues = []
        page = 1
        total_pages = 1  # Will be updated after first request
        
        print("Downloading leagues data...")
        
        while page <= total_pages:
            # Parameters formatted according to SportMonks API requirements
            params = {
                "include": include,
                "per_page": per_page,
                "page": page
            }
            
            try:
                data = self.client.get(self.endpoint, params)
                
                # Update total pages from first response
                if page == 1:
                    # Check for unexpected response format
                    if "pagination" not in data:
                        print(f"Warning: Unexpected API response format. Missing 'pagination'.")
                        print(f"Response preview: {str(data)[:200]}...")
                        # Could implement fallback strategy here if needed
                        break
                        
                    total_pages = data.get("pagination", {}).get("total_pages", 1)
                    print(f"Found {data.get('pagination', {}).get('total', 0)} leagues across {total_pages} pages")
                
                # Extract leagues data
                leagues = data.get("data", [])
                all_leagues.extend(leagues)
                
                print(f"Downloaded page {page}/{total_pages}")
                
                # Save raw data for each page
                self._save_page(page, data)
                
                page += 1
            
            except Exception as e:
                print(f"Error downloading page {page}: {str(e)}")
                print(f"Endpoint: {self.endpoint}, Parameters: {params}")
                # If we fail, still try to save what we've got so far
                break
        
        # Save all leagues to a single file if we got any data
        if all_leagues:
            self._save_all(all_leagues)
            print(f"Downloaded {len(all_leagues)} leagues successfully.")
        else:
            print("No leagues data was downloaded.")
        
        return all_leagues
    
    def _save_page(self, page, data):
        """Save raw page data as JSON."""
        file_path = LEAGUES_RAW_DIR / f"leagues_page_{page}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def _save_all(self, leagues):
        """Save all leagues data as JSON."""
        file_path = LEAGUES_RAW_DIR / "all_leagues.json"
        with open(file_path, "w") as f:
            json.dump(leagues, f, indent=2)