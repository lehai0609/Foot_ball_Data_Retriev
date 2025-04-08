#!/usr/bin/env python3
from src.api.leagues import LeaguesAPI

def main():
    """Download leagues data from SportMonks API."""
    leagues_api = LeaguesAPI()
    leagues_api.get_all_leagues()

if __name__ == "__main__":
    main()