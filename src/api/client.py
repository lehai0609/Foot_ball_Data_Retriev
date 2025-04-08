import requests
import time
from src.config import API_KEY, API_BASE_URL, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF_FACTOR

class APIClient:
    """Base client for the SportMonks API."""
    
    def __init__(self):
        self.base_url = API_BASE_URL
        self.headers = {"Authorization": f"Bearer {API_KEY}"}
    
    def get(self, endpoint, params=None):
        """Make a GET request to the API with retry logic."""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                    verify=False
                )
                requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF_FACTOR ** attempt
                    print(f"Request failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed after {MAX_RETRIES} attempts.")
                    raise