#!/usr/bin/env python3
import os
import sys

# Add the parent directory (project root) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data.processors import process_leagues_data
from src.data.exporters import export_to_csv

def main():
    """Process and export leagues data."""
    df = process_leagues_data()
    if df is not None:
        export_to_csv(df)
        print("\nFirst 5 leagues in the table:")
        print(df.head())

if __name__ == "__main__":
    main()