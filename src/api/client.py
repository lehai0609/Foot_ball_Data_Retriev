import requests
import time
import json
from src.config import API_KEY, API_BASE_URL, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF_FACTOR

class APIClient:
    """Base client for the SportMonks API."""
    
    def __init__(self):
        self.base_url = API_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json"  # Explicitly set Accept header
        }
    
    def get(self, endpoint, params=None):
        """Make a GET request to the API with retry logic."""
        url = f"{self.base_url}/{endpoint}"
        
        # Disable SSL warnings if verify=False is used
        if not API_BASE_URL.startswith("https"):
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        
        for attempt in range(MAX_RETRIES):
            try:
                print(f"Making request to: {url}")
                print(f"Parameters: {params}")
                
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                    verify=False  # Changed to True for security
                )
                
                # Log the response status
                print(f"Response status: {response.status_code}")
                
                # For debugging: if there's an error, print the response content
                if response.status_code >= 400:
                    try:
                        error_details = response.json()
                        print(f"Error details: {json.dumps(error_details, indent=2)}")
                    except:
                        print(f"Raw error response: {response.text}")
                
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