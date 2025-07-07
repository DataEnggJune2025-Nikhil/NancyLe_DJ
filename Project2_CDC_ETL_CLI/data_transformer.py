import pandas as pd
import logging

# Configure logging (ensure this is configured once, e.g., in main.py or a common config)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataTransformer:
    """Handles data cleaning and transformation."""
    def __init__(self):
        # This constructor is intentionally left empty because no initialization is required
        # for the DataTransformer class at this time. It serves as a placeholder for future
        # enhancements or initialization logic if needed.
        pass

    def clean_and_transform(self, df, expected_schema=None):
        """
        Clean and transform raw API DataFrame into a standardized format.
        Parameters:
        - df: DataFrame (already loaded, not a file path)
        - expected_schema: Optional dict of expected columns and types
        """
        # Check if DataFrame is empty
        if df.empty:
            logging.warning("Received an empty DataFrame. No data to transform.")
            return df

        # convert 'case_month' column to datetime format
        if 'case_month' in df.columns:
            df['case_month'] = pd.to_datetime(df['case_month'], errors='coerce')
            df = df.dropna(subset=['case_month'])

        # Convert numerical columns to integers, handle missing values
        numerical_cols = ['case_positive_specimen_interval', 'case_onset_interval']
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        # Standardize string columns
        str_cols = ['res_state', 'age_group', 'sex', 'race', 'ethnicity', 'exposure_yn','current_status',
                    'symptom_status', 'death_yn', 'hosp_yn', 'icu_yn', 'underlying_conditions_yn']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown').astype(str).str.strip()

        # Drop location missing values
        location_cols = ['res_state', 'state_fips_code']
        for col in location_cols:
            if col in df.columns:
                df = df.dropna(subset=[col]) # Apply dropna directly to the DataFrame

        # Ensure 'process' column exists and has a default value if not provided by API
        if 'process' not in df.columns:
            df['process'] = 'Unknown' # Default value for process column

        # Logging the transformation
        logging.info(f"Cleaned data: {len(df)} rows remain after cleaning")
        return df
