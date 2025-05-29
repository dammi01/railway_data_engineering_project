import os
import requests
import pandas as pd
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from io import BytesIO, TextIOWrapper
import gzip

# --- Configuration ---

# Base directory for the raw data (Bronze Layer)
BRONZE_LAYER_PATH = "data/bronze"
os.makedirs(BRONZE_LAYER_PATH, exist_ok=True) # Ensure the directory exists

# Dictionary containing details for each dataset to be ingested
DATASETS = {
    "train_disruptions": {
        "url": "https://opendata.rijdendetreinen.nl/public/disruptions/disruptions-2024.csv",
        "file_name": "disruptions-2024.csv",
        "delta_table_name": "disruptions_bronze",
        "format": "csv",
        "compression": None
    },
    "railway_stations": {
        "url": "https://opendata.rijdendetreinen.nl/public/stations/stations-2023-09.csv",
        "file_name": "stations-2023-09.csv",
        "delta_table_name": "stations_bronze",
        "format": "csv",
        "compression": None
    },
    "train_archive": {
        "url": "https://opendata.rijdendetreinen.nl/public/services/services-2024.csv.gz",
        "file_name": "services-2024.csv.gz",
        "delta_table_name": "train_archive_bronze",
        "format": "csv",
        "compression": "gzip"
    },
    "station_distances": {
        "url": "https://opendata.rijdendetreinen.nl/public/tariff-distances/tariff-distances-2022-01.csv",
        "file_name": "tariff-distances-2022-01.csv",
        "delta_table_name": "distances_bronze",
        "format": "csv",
        "compression": None
    }
}

# --- Utility Functions ---

def download_file(url: str, file_path: str):
    """
    Downloads a file from a given URL and saves it to a specified local path.

    Args:
        url (str): The URL of the file to download.
        file_path (str): The local path where the file will be saved.
    """
    print(f"Downloading {url} to {file_path}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise # Re-raise the exception for upstream handling

def load_data_to_delta(dataset_name: str, dataset_info: dict):
    """
    Loads data from a downloaded CSV file into a DuckDB temporary table,
    then writes it to a Delta Lake table using the deltalake Python library.

    Args:
        dataset_name (str): The logical name of the dataset (e.g., "train_disruptions").
        dataset_info (dict): A dictionary containing 'url', 'file_name', and 'delta_table_name'.
    """
    print(f"\n--- Processing {dataset_name} ---")
    local_file_path = os.path.join(BRONZE_LAYER_PATH, dataset_info["file_name"])
    delta_table_path = os.path.join(BRONZE_LAYER_PATH, dataset_info["delta_table_name"])

    # 1. Download the raw file
    download_file(dataset_info["url"], local_file_path)

    # 2. Establish a DuckDB connection
    con = duckdb.connect()

    try:
        # Load the DuckDB 'delta' extension (install if not present)
        # This is required for DuckDB to understand Delta Lake format for scans.
        try:
            con.execute("INSTALL 'delta'") # Attempt to install (no-op if already installed)
        except Exception: # Catch any error during install, as it might already be there
            pass # Suppress error if already installed
        con.execute("LOAD 'delta'") # Load the extension for the current session
        print("DuckDB 'delta' extension loaded.")

        print(f"Reading CSV from {local_file_path} and writing to Delta Lake table at {delta_table_path} using deltalake library...")

        # Generate a unique temporary table name for each dataset
        temp_table_name = f"{dataset_name}_raw_data"

        # Read the CSV directly into a DuckDB temporary table.
        # READ_CSV_AUTO handles schema inference and compression (e.g., .gz files).
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
            SELECT * FROM read_csv_auto('{local_file_path}');
        """)

        # Fetch data from DuckDB into a PyArrow Table.
        # This is an efficient way to transfer data from DuckDB to Python for deltalake library.
        arrow_table = con.execute(f"SELECT * FROM {temp_table_name}").arrow()

        # Write the PyArrow Table to Delta Lake format using the deltalake library
        # mode="overwrite" will replace the table if it exists.
        write_deltalake(delta_table_path, arrow_table, mode="overwrite")

        print(f"Successfully wrote {dataset_name} to Delta Lake at {delta_table_path}")
        print("Files written to:", os.listdir(delta_table_path)) # List files in the Delta table directory

        # Optional: Verify the created Delta table and row count
        try:
            delta_table = DeltaTable(delta_table_path)
            print(f"Delta table details for {dataset_name}: {delta_table.schema().to_pyarrow()}")

            # Get absolute path for delta_scan if running from /app in Docker
            abs_path = os.path.abspath(delta_table_path)
            print("Absolute path to delta table:", abs_path)
            print("Files in that path:", os.listdir(abs_path))

            # Count rows using DuckDB's delta_scan function to read the Delta table
            count_query_result = con.execute(f"""
                SELECT COUNT(*) FROM delta_scan('{abs_path}')
            """).fetchone()

            if count_query_result:
                print(f"Number of rows in Delta table: {count_query_result[0]}")

        except Exception as e_check:
            print(f"Warning: Could not verify Delta table {dataset_name} after write: {e_check}")
            # This specific check is optional and might fail if deltalake lib has issues or path is wrong.

    except Exception as e:
        print(f"Error processing {dataset_name} and writing to Delta Lake: {e}")
        raise # Re-raise the exception to be caught by the main function's try-except

    finally:
        if 'con' in locals() and con:
            con.close() # Ensure DuckDB connection is closed

# --- Main Execution Flow ---

def main():
    """
    Orchestrates the ingestion process for all defined datasets into the Bronze Layer.
    """
    print("Starting Bronze Layer Ingestion...")
    for name, info in DATASETS.items():
        try:
            load_data_to_delta(name, info)
        except Exception as e:
            print(f"Failed to process {name}: {e}")
            # In a real-world pipeline, you might add retry logic or notification here.
            continue # Continue to the next dataset even if one fails
    print("\nBronze Layer Ingestion Finished.")

if __name__ == "__main__":
    main()