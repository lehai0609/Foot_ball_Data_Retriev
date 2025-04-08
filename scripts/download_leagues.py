#!/usr/bin/env python3
#!/usr/bin/env python3
import os
import sys

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.api.leagues import LeaguesAPI

def main():
    """Download leagues data from SportMonks API."""
    leagues_api = LeaguesAPI()
    leagues_api.get_all_leagues()

if __name__ == "__main__":
    main()