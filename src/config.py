import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API configuration
API_KEY = os.getenv("SPORTMONKS_API_KEY")
if not API_KEY:
    raise ValueError("Missing API key! Make sure SPORTMONKS_API_KEY is set in your .env file")

# Base URL for the SportMonks API
# Updated to match official documentation structure
API_BASE_URL = "https://api.sportmonks.com"

# Data directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Endpoint-specific directories
LEAGUES_RAW_DIR = RAW_DATA_DIR / "leagues"
LEAGUES_PROCESSED_DIR = PROCESSED_DATA_DIR / "leagues"

LEAGUES_RAW_DIR.mkdir(exist_ok=True)
LEAGUES_PROCESSED_DIR.mkdir(exist_ok=True)

# Request configuration
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2

# Print configuration for debugging
print(f"Using API base URL: {API_BASE_URL}")
print(f"API key is {'set' if API_KEY else 'NOT SET'}")
print(f"Data will be saved to: {DATA_DIR}")