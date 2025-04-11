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
        self.resource_name = endpoint.split('/')[-1]
        
        # Create directory for this resource if it doesn't exist
        self.resource_dir = RAW_DATA_DIR / self.resource_name
        self.resource_dir.mkdir(exist_ok=True)
    
    def fetch_all_data(self, include=None, filters=None, per_page=100):
        """
        Fetch all data from the endpoint with pagination.
        
        Args:
            include (str, optional): Related data to include
            filters (dict, optional): Filters to apply to the API request
            per_page (int): Number of items per page
            
        Returns:
            tuple: (all_data, metadata)
                all_data (list): All data items from the endpoint
                metadata (dict): Metadata about the fetch operation
        """
        all_data = []
        page = 1
        total_pages = 1  # Will be updated after first request
        
        # Create params dictionary
        params = {
            "per_page": per_page,
            "page": page
        }
        
        # Add include parameter if provided
        if include:
            params["include"] = include
            
        # Add any filters if provided
        if filters and isinstance(filters, dict):
            params.update(filters)
        
        # Start timing the operation
        start_time = time.time()
        
        print(f"Fetching data from endpoint: {self.endpoint}")
        
        # Track metadata for the operation
        metadata = {
            "endpoint": self.endpoint,
            "start_time": datetime.now().isoformat(),
            "params": params.copy(),
            "pages_fetched": 0,
            "items_fetched": 0,
            "errors": []
        }
        
        try:
            while page <= total_pages:
                # Update page number for each request
                params["page"] = page
                
                try:
                    data = self.client.get(self.endpoint, params)
                    
                    # Update total pages from first response
                    if page == 1:
                        # Check for pagination info
                        if "pagination" not in data:
                            print(f"Warning: No pagination data found. Response format may be unexpected.")
                            print(f"Response preview: {str(data)[:200]}...")
                            
                            # If no pagination but we have data, just process this page
                            total_pages = 1
                        else:
                            pagination = data.get("pagination", {})
                            total_pages = pagination.get("total_pages", 1)
                            total_items = pagination.get("total", 0)
                            
                            print(f"Found {total_items} items across {total_pages} pages")
                            
                            # Add pagination info to metadata
                            metadata["pagination"] = {
                                "total_pages": total_pages,
                                "total_items": total_items,
                                "per_page": pagination.get("per_page", per_page)
                            }
                    
                    # Extract data items
                    if "data" in data and isinstance(data["data"], list):
                        items = data.get("data", [])
                        all_data.extend(items)
                        
                        # Update metadata
                        metadata["pages_fetched"] += 1
                        metadata["items_fetched"] += len(items)
                        
                        print(f"Fetched page {page}/{total_pages} with {len(items)} items")
                    else:
                        print(f"Warning: No 'data' array found in response for page {page}")
                        # Add to errors in metadata
                        metadata["errors"].append({
                            "page": page,
                            "error": "No data array in response",
                            "time": datetime.now().isoformat()
                        })
                        break
                    
                    # Save raw page data
                    self._save_page(page, data)
                    
                    page += 1
                    
                except Exception as e:
                    error_msg = f"Error fetching page {page}: {str(e)}"
                    print(error_msg)
                    
                    # Add to errors in metadata
                    metadata["errors"].append({
                        "page": page,
                        "error": str(e),
                        "time": datetime.now().isoformat()
                    })
                    
                    # If we failed on the first page, re-raise the exception
                    if page == 1:
                        raise
                    
                    # Otherwise, continue with the data we have
                    break
        
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
                
                print(f"Fetched {len(all_data)} items in {metadata['duration_seconds']} seconds")
                print(f"Data saved to: {file_path}")
                print(f"Metadata saved to: {metadata_path}")
            else:
                print("No data was fetched.")
        
        return all_data, metadata
    
    def _save_page(self, page, data):
        """Save raw page data as JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.resource_dir / f"{self.resource_name}_page_{page}_{timestamp}.json"
        
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        
        return file_path
    
    def _save_all(self, data, metadata):
        """
        Save all fetched data and metadata as JSON files.
        
        Returns:
            tuple: (data_path, metadata_path)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.resource_dir / f"{self.resource_name}_{timestamp}.json"
        metadata_path = self.resource_dir / f"{self.resource_name}_{timestamp}_metadata.json"
        
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
            
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
            
        return file_path, metadata_path