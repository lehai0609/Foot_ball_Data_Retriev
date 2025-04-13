import json
import time
from datetime import datetime
from pathlib import Path
from src.api.client import APIClient
from src.config import RAW_DATA_DIR

class EndpointHandler:
    """Generic handler for any SportMonks API endpoint."""

    def __init__(self, endpoint):
        """
        Initialize the endpoint handler.

        Args:
            endpoint (str): API endpoint path (e.g., 'v3/football/leagues')
        """
        self.client = APIClient()
        self.endpoint = endpoint

        # Determine resource name from endpoint for file naming
        # Handles potential trailing slashes or complex paths
        self.resource_name = endpoint.strip('/').split('/')[-1]

        # Create directory for this resource if it doesn't exist
        self.resource_dir = RAW_DATA_DIR / self.resource_name
        self.resource_dir.mkdir(parents=True, exist_ok=True)
        print(f"Raw data for endpoint '{self.endpoint}' will be saved in: {self.resource_dir}")


    def fetch_all_data(self, include=None, filters=None, per_page=100):
        """
        Fetch all data from the endpoint with pagination, using 'has_more'.

        Args:
            include (str, optional): Related data to include (comma-separated).
            filters (dict, optional): Filters to apply to the API request.
            per_page (int): Number of items per page.

        Returns:
            tuple: (all_data, metadata)
                all_data (list): All data items from the endpoint.
                metadata (dict): Metadata about the fetch operation.
        """
        all_data = []
        page = 1
        has_more_pages = True # Assume there's at least one page

        # Create base params dictionary
        params = {"per_page": per_page}

        # Add include parameter if provided
        if include:
            params["include"] = include

        # Add any filters if provided
        if filters and isinstance(filters, dict):
            params.update(filters)

        # Start timing the operation
        start_time = time.time()

        print(f"Fetching data from endpoint: {self.endpoint}")
        print(f"Initial Params: {params}")

        # Track metadata for the operation
        metadata = {
            "endpoint": self.endpoint,
            "start_time": datetime.now().isoformat(),
            "params_base": params.copy(), # Store base params without page number
            "pages_fetched": 0,
            "items_fetched": 0,
            "errors": [],
            "pagination_info_first_page": None # Store pagination info from first page
        }

        try:
            # Loop as long as the API indicates more pages are available
            while has_more_pages:
                # Add/Update page number for the current request
                current_params = params.copy()
                current_params["page"] = page

                try:
                    print(f"Fetching page {page}...")
                    data = self.client.get(self.endpoint, current_params)

                    # Extract pagination info (do this every time, but store first page's)
                    pagination = data.get("pagination", {})
                    if page == 1:
                         metadata["pagination_info_first_page"] = pagination
                         # Log initial pagination details
                         print(f"Pagination info (page 1): {pagination}")


                    # --- Pagination Logic ---
                    # Use 'has_more' field primarily. Default to False if missing.
                    has_more_pages = pagination.get("has_more", False)
                    # --- End Pagination Logic ---


                    # Extract data items
                    if "data" in data and isinstance(data["data"], list):
                        items = data.get("data", [])
                        all_data.extend(items)

                        # Update metadata
                        metadata["pages_fetched"] += 1
                        metadata["items_fetched"] += len(items)

                        print(f"Fetched page {page} with {len(items)} items. More pages: {has_more_pages}")
                    else:
                        print(f"Warning: No 'data' array found in response for page {page}")
                        # Add to errors in metadata
                        metadata["errors"].append({
                            "page": page,
                            "error": "No data array in response",
                            "time": datetime.now().isoformat()
                        })
                        # Stop if data format is unexpected
                        has_more_pages = False # Assume we can't continue

                    # Save raw page data (optional, can be disabled)
                    # self._save_page(page, data) # Uncomment if you want per-page files

                    # Increment page number for the next iteration ONLY if there are more pages
                    if has_more_pages:
                        page += 1
                    # Add a small delay to avoid hitting rate limits too quickly (optional)
                    # time.sleep(0.1)

                except Exception as e:
                    error_msg = f"Error fetching page {page}: {str(e)}"
                    print(error_msg)

                    # Add to errors in metadata
                    metadata["errors"].append({
                        "page": page,
                        "error": str(e),
                        "time": datetime.now().isoformat()
                    })

                    # Stop fetching if an error occurs
                    has_more_pages = False # Assume we should stop on error

        finally:
            # Complete metadata
            end_time = time.time()
            metadata["end_time"] = datetime.now().isoformat()
            metadata["duration_seconds"] = round(end_time - start_time, 2)

            # Save all data and metadata
            if all_data:
                file_path, metadata_path = self._save_all(all_data, metadata)
                metadata["file_path"] = str(file_path)
                metadata["metadata_path"] = str(metadata_path)

                print(f"\nFetched {metadata['items_fetched']} items across {metadata['pages_fetched']} pages in {metadata['duration_seconds']} seconds.")
                print(f"Consolidated data saved to: {file_path}")
                print(f"Metadata saved to: {metadata_path}")
            else:
                print("\nNo data was fetched or processed.")
            if metadata["errors"]:
                print(f"WARNING: {len(metadata['errors'])} errors occurred during the process. Check metadata file for details.")


        return all_data, metadata

    def _save_page(self, page, data):
        """Save raw page data as JSON."""
        # Ensure resource_dir exists (should be created in __init__)
        self.resource_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.resource_dir / f"{self.resource_name}_page_{page}_{timestamp}.json"

        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            # print(f"Saved page {page} raw data to {file_path}") # Optional log
            return file_path
        except Exception as e:
            print(f"Error saving page {page} data to {file_path}: {e}")
            return None


    def _save_all(self, data, metadata):
        """
        Save all fetched data and metadata as JSON files.

        Returns:
            tuple: (data_path, metadata_path)
        """
         # Ensure resource_dir exists
        self.resource_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.resource_dir / f"{self.resource_name}_{timestamp}.json"
        metadata_path = self.resource_dir / f"{self.resource_name}_{timestamp}_metadata.json"

        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
             print(f"Error saving consolidated data to {file_path}: {e}")
             # Decide if you want to raise the error or just return None
             # raise

        try:
            with open(metadata_path, "w") as f:
                # Convert Path objects to strings for JSON serialization
                if "file_path" in metadata:
                    metadata["file_path"] = str(metadata["file_path"])
                if "metadata_path" in metadata:
                     metadata["metadata_path"] = str(metadata["metadata_path"])
                json.dump(metadata, f, indent=2)
        except Exception as e:
            print(f"Error saving metadata to {metadata_path}: {e}")
            # Decide if you want to raise the error or just return None
            # raise

        return file_path, metadata_path

