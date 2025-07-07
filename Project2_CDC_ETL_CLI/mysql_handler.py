import mysql.connector
import pandas as pd
import logging

# Configure logging (ensure this is configured once, e.g., in main.py or a common config)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MySQLHandler:
    """Manages MySQL database connections and operations."""
    def __init__(self, host, user, password, database, port=3306):
        self.conn = None
        try:
            self.conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database, port=port
            )
            logging.info("Successfully connected to MySQL database.")
        except mysql.connector.Error as e:
            logging.error(f"Error connecting to MySQL database: {e}")
            raise

    def create_tables(self):
        """Creates the cdc_covid_cases table if it does not exist."""
        if not self.conn:
            logging.error("Database connection not established. Cannot create tables.")
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cdc_covid_cases (
            id INT AUTO_INCREMENT PRIMARY KEY,
            case_month DATE,
            res_state VARCHAR(50),
            state_fips_code INT,
            age_group VARCHAR(50),
            sex VARCHAR(20),
            race VARCHAR(100),
            ethnicity VARCHAR(100),
            case_positive_specimen_interval INT,
            case_onset_interval INT,
            process VARCHAR(50),
            exposure_yn VARCHAR(20),
            current_status VARCHAR(50),
            symptom_status VARCHAR(50),
            hosp_yn VARCHAR(20),
            icu_yn VARCHAR(20),
            death_yn VARCHAR(20),
            underlying_conditions_yn VARCHAR(20),
            UNIQUE KEY unique_case (case_month, res_state, age_group, sex, race, ethnicity)
        );
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(create_table_sql)
            self.conn.commit()
            logging.info("Table 'cdc_covid_cases' checked/created successfully.")
        except mysql.connector.Error as e:
            logging.error(f"Error creating table: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def load_cdc_data(self, df):
        """Insert DataFrame rows into MySQL table."""
        if not self.conn:
            logging.error("Database connection not established. Cannot load data.")
            return

        cursor = self.conn.cursor()
        insert_sql = """
        INSERT INTO cdc_covid_cases
        (case_month, res_state, state_fips_code, age_group,
        sex, race, ethnicity, case_positive_specimen_interval, case_onset_interval,
        process, exposure_yn, current_status, symptom_status,
        hosp_yn, icu_yn, death_yn, underlying_conditions_yn)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            death_yn=VALUES(death_yn),
            hosp_yn=VALUES(hosp_yn),
            icu_yn=VALUES(icu_yn),
            underlying_conditions_yn=VALUES(underlying_conditions_yn)
        """
        rows = []
        # Define expected columns and their default values if missing
        expected_cols = {
            'case_month': None,
            'res_state': 'Unknown',
            'state_fips_code': 0,
            'age_group': 'Unknown',
            'sex': 'Unknown',
            'race': 'Unknown',
            'ethnicity': 'Unknown',
            'case_positive_specimen_interval': 0,
            'case_onset_interval': 0,
            'process': 'Unknown',
            'exposure_yn': 'Unknown',
            'current_status': 'Unknown',
            'symptom_status': 'Unknown',
            'hosp_yn': 'Unknown',
            'icu_yn': 'Unknown',
            'death_yn': 'Unknown',
            'underlying_conditions_yn': 'Unknown'
        }
        for _, row in df.iterrows():
            # Create a dictionary for the current row, filling missing columns with defaults
            row_data = {col: row[col] if col in row else default_val for col, default_val in expected_cols.items()}

            # Convert Timestamp to date string for MySQL DATE type
            case_month_str = row_data['case_month'].strftime('%Y-%m-%d') if pd.notnull(row_data['case_month']) else None

            rows.append((
                case_month_str,
                row_data['res_state'],
                row_data['state_fips_code'],
                row_data['age_group'],
                row_data['sex'],
                row_data['race'],
                row_data['ethnicity'],
                row_data['case_positive_specimen_interval'],
                row_data['case_onset_interval'],
                row_data['process'],
                row_data['exposure_yn'],
                row_data['current_status'],
                row_data['symptom_status'],
                row_data['hosp_yn'],
                row_data['icu_yn'],
                row_data['death_yn'],
                row_data['underlying_conditions_yn']
            ))

        try:
            cursor.executemany(insert_sql, rows)
            self.conn.commit()
            logging.info(f"Inserted/updated {cursor.rowcount} rows into cdc_covid_cases table.")
        except mysql.connector.Error as e:
            logging.error(f"Error inserting data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def query_total_cases_by_state(self, state_code=None):
        """Queries total cases by state, optionally filtered by state code."""
        if not self.conn:
            logging.error("Database connection not established. Cannot query data.")
            return pd.DataFrame()

        cursor = self.conn.cursor(dictionary=True)
        query_sql = """
        SELECT res_state, COUNT(*) AS total_cases
        FROM cdc_covid_cases
        """
        params = [] 
        if state_code:
            query_sql += " WHERE res_state = %s"
            params.append(state_code)
        query_sql += " GROUP BY res_state ORDER BY total_cases DESC;"

        try:
            cursor.execute(query_sql, params)
            result = pd.DataFrame(cursor.fetchall())
            logging.info(f"Query 'total_cases_by_state' executed. Rows found: {len(result)}")
            return result
        except mysql.connector.Error as e:
            logging.error(f"Error querying total cases by state: {e}")
            return pd.DataFrame()
        finally:
            cursor.close()

    def query_cases_by_age_group(self):
        """Queries total cases by age group."""
        if not self.conn:
            logging.error("Database connection not established. Cannot query data.")
            return pd.DataFrame()

        cursor = self.conn.cursor(dictionary=True)
        query_sql = """
        SELECT age_group, COUNT(*) AS total_cases
        FROM cdc_covid_cases
        GROUP BY age_group
        ORDER BY total_cases DESC;
        """
        try:
            cursor.execute(query_sql)
            result = pd.DataFrame(cursor.fetchall())
            logging.info(f"Query 'cases_by_age_group' executed. Rows found: {len(result)}")
            return result
        except mysql.connector.Error as e:
            logging.error(f"Error querying cases by age group: {e}")
            return pd.DataFrame()
        finally:
            cursor.close()

    def query_cases_by_sex(self):
        """Queries total cases by sex."""
        if not self.conn:
            logging.error("Database connection not established. Cannot query data.")
            return pd.DataFrame()

        cursor = self.conn.cursor(dictionary=True)
        query_sql = """
        SELECT sex, COUNT(*) AS total_cases
        FROM cdc_covid_cases
        GROUP BY sex
        ORDER BY total_cases DESC;
        """
        try:
            cursor.execute(query_sql)
            result = pd.DataFrame(cursor.fetchall())
            logging.info(f"Query 'cases_by_sex' executed. Rows found: {len(result)}")
            return result
        except mysql.connector.Error as e:
            logging.error(f"Error querying cases by sex: {e}")
            return pd.DataFrame()
        finally:
            cursor.close()

    def close(self):
        """Closes the database connection."""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logging.info("MySQL connection closed.")