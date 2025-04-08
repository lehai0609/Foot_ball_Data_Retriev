#!/usr/bin/env python3
import os
import sys
import requests
import json
from pprint import pprint

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import API_KEY, API_BASE_URL

def test_api_connection():
    """
    Test the connection to the SportMonks API with various endpoint structures
    to diagnose the issue.
    """
    # Define the headers
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }
    
    # List of endpoints to try
    endpoints = [
        # Standard endpoint from your code
        "leagues",
        # Alternative endpoint structures to try
        "core/leagues",
        "countries"  # This is often a simpler endpoint that works
    ]
    
    print(f"API Key (first 5 chars): {API_KEY[:5]}...")
    print(f"Base URL: {API_BASE_URL}")
    
    for endpoint in endpoints:
        url = f"{API_BASE_URL}/{endpoint}"
        print(f"\nTesting endpoint: {url}")
        
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"per_page": 5},  # Minimal parameters
                timeout=30,
                verify=True
            )
            
            print(f"Status code: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ Success! This endpoint works.")
                data = response.json()
                
                # Check the structure of the response
                if "data" in data:
                    print(f"Found {len(data['data'])} items in the response")
                    
                    # Show sample of the first item
                    if data["data"]:
                        print("\nSample of first item:")
                        pprint(data["data"][0])
                        
                        # If it's the leagues endpoint, check for relations
                        if endpoint == "leagues" and "relationships" in data["data"][0]:
                            print("\nAvailable relationships:")
                            print(list(data["data"][0]["relationships"].keys()))
                
                # Check pagination info
                if "pagination" in data:
                    print("\nPagination info:")
                    pprint(data["pagination"])
            else:
                print("❌ Failed!")
                try:
                    error_data = response.json()
                    print("Error details:")
                    pprint(error_data)
                except:
                    print(f"Raw response: {response.text[:200]}...")
        
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
    
    # Test including related data
    print("\n\nTesting 'include' parameter with different values:")
    working_endpoint = None
    
    # Find a working endpoint from our tests above
    for endpoint in endpoints:
        url = f"{API_BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                working_endpoint = endpoint
                break
        except:
            continue
    
    if working_endpoint:
        print(f"Using working endpoint: {working_endpoint}")
        
        include_values = ["country", "countries", "season", "seasons"]
        
        for include in include_values:
            url = f"{API_BASE_URL}/{working_endpoint}"
            print(f"\nTesting include={include}")
            
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params={"include": include, "per_page": 5},
                    timeout=30
                )
                
                print(f"Status code: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"✅ Include '{include}' works!")
                else:
                    print(f"❌ Include '{include}' failed")
            
            except Exception as e:
                print(f"❌ Exception: {str(e)}")

if __name__ == "__main__":
    test_api_connection()