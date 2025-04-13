# SportMonks Football Data Pipeline

This project fetches football data from the SportMonks v3 API and stores it in a local SQLite database.

## Project Structure

sportmonks-football/
│
├── src/
│   ├── config.py           # Configuration (API key, paths)
│   ├── api/
│   │   ├── client.py       # Base API client (handles requests, retries)
│   │   └── endpoints.py    # Generic EndpointHandler for fetching data
│   │
│   └── data/
│       ├── processors.py   # Functions to transform raw API data
│       └── storage.py      # Functions for SQLite DB connection, setup, and storage
│
├── data/
│   ├── raw/                # Raw JSON downloaded from API (organized by resource)
│   └── database/
│       └── football_data.db # SQLite database file
│
└── scripts/
    ├── sync_leagues.py     # Fetches, processes, and stores League data
    ├── sync_teams.py       # (Future) Fetches, processes, and stores Team data
    ├── sync_fixtures.py    # (Future) Fetches, processes, and stores Fixture data
    └── ...                 # Other sync scripts for different resources
    └── download.py         # Optional: Ad-hoc script to download raw JSON for any endpoint

## Workflow

The primary workflow involves running resource-specific sync scripts located in the `scripts/` directory (e.g., `python scripts/sync_leagues.py`). Each script typically performs the following steps:

1.  **Fetch:** Uses the `EndpointHandler` from `src.api.endpoints` to download all relevant data for a specific resource (e.g., leagues) from the SportMonks API, handling pagination. Raw data is saved in `data/raw/`.
2.  **Process:** Uses functions from `src.data.processors` to transform the raw JSON data into a format suitable for the database schema.
3.  **Store:** Uses functions from `src.data.storage` to connect to the SQLite database (`data/database/football_data.db`), ensure the necessary table exists, and insert/update the processed data.

## Setup

1.  Clone the repository.
2.  Create a `.env` file in the root directory with your `SPORTMONKS_API_KEY`.
3.  Install dependencies: `pip install -r requirements.txt`
4.  Run a sync script: `python scripts/sync_leagues.py`