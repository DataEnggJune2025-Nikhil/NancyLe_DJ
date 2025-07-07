# CDC Covid Data ETL & Analysis Pipeline

This project implements an Extract, Transform, Load (ETL) pipeline to fetch COVID-19 case data from the CDC API, clean and transform it, and then load it into a MySQL database. It also provides a command-line interface (CLI) for managing the ETL process and querying the loaded data.

## Features
1. Data Extraction: Fetches CDC COVID-19 case data in chunks from a public API.
2. Data Transformation: Cleans and standardizes raw data, handles missing values, and ensures data types are consistent.
3. Data Loading: Loads transformed data into a MySQL database, with support for ON DUPLICATE KEY UPDATE to handle existing records.
4. Command-Line Interface (CLI):
	* `fetch` command: Automates the ETL process, allowing specification of data limits, chunk sizes, and date ranges.
	* `query_data` command: Enables querying the loaded data for various insights, such as total cases by state, age group, and sex.

## Setup Instructions
### Prerequisites
	* Python 3.x installed.
	* MySQL server installed and running (ensure it's accessible, e.g., on localhost:3306).
	* A MySQL user with permissions to create tables and insert/update data in your chosen database.

## 🚀 Install dependencies:

1. Install Python 3.9+ and MySQL
2. Install dependencies:
   `pip install -r requirements.txt`

## Usage
Navigate to the project's root directory in your terminal to run the `main.py` script.
1. Fetching Data (ETL Process)
To fetch data from the CDC API, transform it, and load it into your MySQL database:

`python main.py fetch --max-rows 5000 --chunk-size 1000 --start-date 2020-01-01 --end-date 2021-12-31`

`--max-rows`: (Optional) Maximum number of rows to fetch in total. If omitted, it will fetch all available data until the API returns an empty chunk.
`--chunk-size`: (Optional) Number of rows to fetch per API call and process in each chunk (default: 1000).
`--start-date`: (Optional) Filters data to include records from this date onwards (format: YYYY-MM-DD).
`--end-date`: (Optional) Filters data to include records up to this date (format: YYYY-MM-DD).

## Querying Data
To query the data already loaded into your MySQL database:

### Get Total Cases by State
`python main.py query_data total_cases --state NY`

`--state`: (Optional) Filter by a specific state code (e.g., NY, CA). If omitted, it will show total cases for all states.

### Get Total Cases by Age Group
`python main.py query_data cases_by_age_group`

### Get Total Cases by Sex
`python main.py query_data cases_by_sex`


