#!/usr/bin/env python3
import os
import sys
import requests
import json

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import API_KEY, API_BASE_URL

def test_api_connection():
    """
    Test the connection to the SportMonks API using the correct endpoint structure.
    """
    # Define the URL based on official documentation
    url = f"{API_BASE_URL}/v3/football/leagues"
    
    # Set up parameters with API token
    params = {
        "api_token": API_KEY,
        "per_page": 5  # Limit to just a few results
    }
    
    # Set headers
    headers = {
        "Accept": "application/json"
    }
    
    print(f"Testing endpoint: {url}")
    print(f"API Key (first 5 chars): {API_KEY[:5]}...")
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30,
            verify=True
        )
        
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Connection successful!")
            data = response.json()
            
            print(f"\nAPI Response Structure:")
            if "data" in data:
                print(f"- Contains 'data' array with {len(data['data'])} items")
            if "pagination" in data:
                print(f"- Contains 'pagination' information")
                print(f"  - Total: {data['pagination'].get('total')}")
                print(f"  - Count: {data['pagination'].get('count')}")
                print(f"  - Per Page: {data['pagination'].get('per_page')}")
                print(f"  - Total Pages: {data['pagination'].get('total_pages')}")
            
            if "data" in data and data["data"]:
                print("\nSample of first league data:")
                # Print basic info about the first league
                league = data["data"][0]
                print(f"- ID: {league.get('id')}")
                print(f"- Name: {league.get('name')}")
                print(f"- Short code: {league.get('short_code', 'N/A')}")
                print(f"- Country ID: {league.get('country_id', 'N/A')}")
                print(f"- Sport ID: {league.get('sport_id', 'N/A')}")
                
                # Print all available fields for reference
                print("\nAll fields in league object:")
                for key, value in league.items():
                    print(f"- {key}: {value}")
        else:
            print("❌ Connection failed!")
            try:
                error_data = response.json()
                print("Error details:")
                print(json.dumps(error_data, indent=2))
            except:
                print(f"Raw response: {response.text[:500]}...")
    
    except Exception as e:
        print(f"❌ Exception: {str(e)}")

if __name__ == "__main__":
    test_api_connection()