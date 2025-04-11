#!/usr/bin/env python3
import os
import sys
import argparse
import json
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.api.endpoints import EndpointHandler
from src.config import API_BASE_URL

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download data from SportMonks API")
    
    parser.add_argument(
        "--endpoint", 
        type=str, 
        required=True,
        help="API endpoint to fetch (e.g., 'v3/football/leagues')"
    )
    
    parser.add_argument(
        "--include", 
        type=str, 
        help="Related data to include (e.g., 'country,season')"
    )
    
    parser.add_argument(
        "--per-page", 
        type=int, 
        default=100,
        help="Number of items per page (default: 100)"
    )
    
    parser.add_argument(
        "--filters", 
        type=str, 
        help="JSON string of filters to apply (e.g., '{\"name\":\"Premier League\"}')"
    )
    
    return parser.parse_args()

def main():
    """Download data from the specified SportMonks API endpoint."""
    args = parse_arguments()
    
    # Process filters if provided
    filters = None
    if args.filters:
        try:
            filters = json.loads(args.filters)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format for filters: {args.filters}")
            sys.exit(1)
    
    print(f"=== Downloading data from {API_BASE_URL}/{args.endpoint} ===")
    print(f"Include: {args.include or 'None'}")
    print(f"Filters: {filters or 'None'}")
    print(f"Per page: {args.per_page}")
    print("=" * 50)
    
    # Create endpoint handler
    handler = EndpointHandler(args.endpoint)
    
    try:
        # Fetch all data
        data, metadata = handler.fetch_all_data(
            include=args.include,
            filters=filters,
            per_page=args.per_page
        )
        
        # Print summary
        print("\n=== Download Summary ===")
        print(f"Total items: {len(data)}")
        print(f"Time taken: {metadata.get('duration_seconds', 'N/A')} seconds")
        print(f"File saved: {metadata.get('file_path', 'N/A')}")
        
        # Check for errors
        errors = metadata.get('errors', [])
        if errors:
            print(f"\nWarning: {len(errors)} errors occurred during download")
            for i, error in enumerate(errors[:3], 1):  # Show first 3 errors
                print(f"  {i}. Page {error.get('page')}: {error.get('error')}")
            
            if len(errors) > 3:
                print(f"  ... and {len(errors) - 3} more errors. See metadata file for details.")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
        
    return 0

if __name__ == "__main__":
    sys.exit(main())