import requests
import pandas as pd
import logging
import time

# Configure logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class APIClient:
    """Handles interaction with the external healthcare API."""
    def __init__(self, base_url, api_key=None):
        self.base_url = base_url
        self.api_key = api_key

    def fetch_data(self, limit=100, offset=0, retries=3, wait=2):
        """Fetch a single chunk of data with retry logic"""
        # The base_url should be able to accept limit and offset parameters.
        # Example: "https://data.cdc.gov/resource/example.csv?$limit={limit}&$offset={offset}"
        url = self.base_url.format(limit=limit, offset=offset)
        for attempt in range(retries):
            try:
                df = pd.read_csv(url)
                logging.info(f"Fetched {len(df)} rows from offset {offset}")
                return df
            except requests.RequestException as e:
                logging.error(f"API request failed: {e}")
                if attempt < retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed, retrying in {wait} seconds...")
                    time.sleep(wait)
            except Exception as e:
                logging.error(f"An unexpected error occurred while fetching data: {e}")
                if attempt < retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed, retrying in {wait} seconds...")
                    time.sleep(wait)
        logging.error(f"Failed to fetch data after {retries} attempts.")
        return pd.DataFrame()  # Return empty DataFrame on failure

    def fetch_data_chunked(self, chunk_size=1000, max_rows=None, start_date=None, end_date=None):
        """
        Fetch data in chunks and yield each chunk.
        Parameters:
        chunk_size: int, number of rows to fetch in each chunk
        max_rows: int, maximum number of rows to fetch in total
        start_date: datetime.date, optional, filter data from this date onwards
        end_date: datetime.date, optional, filter data up to this date
        """
        offset = 0
        total_rows = 0
        
        while True:
            if max_rows is not None and total_rows >= max_rows:
                break
            limit = min(chunk_size, max_rows - total_rows) if max_rows else chunk_size
            chunk = self.fetch_data(limit=limit, offset=offset)
            if chunk.empty:
                break

            # Apply date filtering if specified
            if start_date or end_date:
                if 'case_month' in chunk.columns:
                    chunk['case_month'] = pd.to_datetime(chunk['case_month'], errors='coerce')
                    if start_date:
                        chunk = chunk[chunk['case_month'].dt.date >= start_date]
                    if end_date:
                        chunk = chunk[chunk['case_month'].dt.date <= end_date]
                else:
                    logging.warning("Date filtering requested but 'case_month' column not found.")

            if chunk.empty and (start_date or end_date): # If chunk becomes empty after date filtering
                logging.info("Chunk became empty after date filtering. Stopping fetch.")
                break

            yield chunk

            fetched = len(chunk)
            total_rows += fetched
            offset += chunk_size # Increment offset by chunk_size, not fetched, to avoid missing data if a chunk is smaller due to filters

            if fetched < limit:
                logging.info(f"No more data to fetch. Total rows fetched: {total_rows}")
                break

