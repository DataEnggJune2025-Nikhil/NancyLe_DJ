import argparse
import logging
import configparser
from datetime import datetime
import pandas as pd

# Import classes from their respective files
from api_client import APIClient
from data_transformer import DataTransformer
from mysql_handler import MySQLHandler

# Configure logging for the entire application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CLIManager:
    """Manages the command-line interface."""
    def __init__(self, api_client, transformer, db_handler):
        self.api_client = api_client
        self.transformer = transformer
        self.db_handler = db_handler
        self.parser = argparse.ArgumentParser(description="Healthcare of Covid Data ETL & Analysis CLI")
        self._setup_parser()

    def _setup_parser(self):
        subparsers = self.parser.add_subparsers(dest='command', help='Available commands')
        
        # ETL commands
        fetch_parser = subparsers.add_parser('fetch', help='Fetch the CDC Covid data')
        fetch_parser.add_argument('--limit', type=int, default=1000, help='Number of rows to fetch per API call')
        fetch_parser.add_argument('--max-rows', type=int, default=None, help='Maximum number of rows to fetch in total')
        fetch_parser.add_argument('--chunk-size', type=int, default=1000, help='Number of rows to process in each chunk')
        fetch_parser.add_argument('--start-date', type=str, help='Start date for data fetching (YYYY-MM-DD)')
        fetch_parser.add_argument('--end-date', type=str, help='End date for data fetching (YYYY-MM-DD)')
        fetch_parser.set_defaults(func=self._handle_fetch) # Set default function for fetch command
        
        # Query commands
        query_parser = subparsers.add_parser('query_data', help='Query the loaded CDC Covid data')
        query_subparsers = query_parser.add_subparsers(dest='query_type', help='Query types', required=True)
        
        total_cases_parser = query_subparsers.add_parser('total_cases', help='Get total cases by state')
        total_cases_parser.add_argument('--state', type=str, help='State code to filter by (e.g., "NY")')
        total_cases_parser.set_defaults(func=self._handle_query_data)

        cases_by_age_group_parser = query_subparsers.add_parser('cases_by_age_group', help='Get total cases by age group')
        cases_by_age_group_parser.set_defaults(func=self._handle_query_data)

        cases_by_sex_parser = query_subparsers.add_parser('cases_by_sex', help='Get total cases by sex')
        cases_by_sex_parser.set_defaults(func=self._handle_query_data)

    def _handle_fetch(self, args):
        """Handles the 'fetch' command: fetches, transforms, and loads data."""
        logging.info("Starting data fetch process...")
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date() if args.start_date else None
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else None

        total_processed_rows = 0
        for i, chunk in enumerate(self.api_client.fetch_data_chunked(
            chunk_size=args.chunk_size,
            max_rows=args.max_rows,
            start_date=start_date,
            end_date=end_date
        )):
            logging.info(f"Processing chunk {i+1} with {len(chunk)} rows")
            if not chunk.empty:
                transformed_data = self.transformer.clean_and_transform(chunk)
                if not transformed_data.empty:
                    self.db_handler.load_cdc_data(transformed_data)
                    total_processed_rows += len(transformed_data)
                else:
                    logging.warning(f"Chunk {i+1} resulted in empty DataFrame after transformation.")
            else:
                logging.info(f"Chunk {i+1} was empty, stopping fetch.")
                break
        logging.info(f"Data fetch process completed. Total rows processed and loaded: {total_processed_rows}")

    def _handle_query_data(self, args):
        """Handles the 'query_data' command and its sub-commands."""
        logging.info(f"Executing query: {args.query_type}")
        result_df = pd.DataFrame()
        if args.query_type == 'total_cases':
            result_df = self.db_handler.query_total_cases_by_state(args.state)
        elif args.query_type == 'cases_by_age_group':
            result_df = self.db_handler.query_cases_by_age_group()
        elif args.query_type == 'cases_by_sex':
            result_df = self.db_handler.query_cases_by_sex()
        else:
            logging.error(f"Unknown query type: {args.query_type}")
            return

        if not result_df.empty:
            print("\nQuery Results:")
            print(result_df.to_string(index=False))
        else:
            print("\nNo results found for your query.")

    def run(self):
        """Parses arguments and executes the corresponding command."""
        args = self.parser.parse_args()
        if hasattr(args, 'func'):
            args.func(args)
        else:
            self.parser.print_help()
            logging.warning("No command provided. Use 'fetch' or 'query_data'.")


def main():
    # Load configuration from config.ini
    config = configparser.ConfigParser()
    try:
        config.read('config.ini')
        db_config = dict(config['mysql'])
        api_base_url = config['api']['base_url']
    except KeyError as e:
        logging.error(f"Missing section or key in config.ini: {e}. Please ensure 'mysql' and 'api' sections exist.")
        return
    except Exception as e:
        logging.error(f"Error reading config.ini: {e}")
        return

    # Initialize components
    api_client = APIClient(api_base_url)
    transformer = DataTransformer()
    db_handler = None
    try:
        db_handler = MySQLHandler(**db_config)
        db_handler.create_tables() # Ensure tables exist before operations
        cli = CLIManager(api_client, transformer, db_handler)
        cli.run()
    except Exception as e:
        logging.critical(f"Application failed: {e}")
    finally:
        if db_handler:
            db_handler.close()

if __name__ == "__main__":
    main()
