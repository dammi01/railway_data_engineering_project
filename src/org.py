import os
import requests
import pandas as pd  # Ainda útil para outras operações, mas não para a leitura inicial de grandes CSVs
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from io import BytesIO, TextIOWrapper
import gzip

# Definir o diretório base para os dados brutos (Bronze Layer)
BRONZE_LAYER_PATH = "data/bronze"
os.makedirs(BRONZE_LAYER_PATH, exist_ok=True)

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

def download_file(url, file_path):
    print(f"Downloading {url} to {file_path}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise

def load_data_to_delta(dataset_name, dataset_info):
    print(f"\n--- Processing {dataset_name} ---")
    local_file_path = os.path.join(BRONZE_LAYER_PATH, dataset_info["file_name"])
    delta_table_path = os.path.join(BRONZE_LAYER_PATH, dataset_info["delta_table_name"])

    download_file(dataset_info["url"], local_file_path)
    con = duckdb.connect()

    try:
        try:
            con.execute("INSTALL 'delta'")
        except:
            pass
        con.execute("LOAD 'delta'")
        print("DuckDB 'delta' extension loaded.")

        print(f"Reading CSV from {local_file_path} and writing to Delta Lake table at {delta_table_path}...")
        temp_table_name = f"{dataset_name}_raw_data"

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
            SELECT * FROM read_csv_auto('{local_file_path}');
        """)

        arrow_table = con.execute(f"SELECT * FROM {temp_table_name}").arrow()
        write_deltalake(delta_table_path, arrow_table, mode="overwrite")

        print(f"Successfully wrote {dataset_name} to Delta Lake at {delta_table_path}")
        print("Files written to:", os.listdir(delta_table_path))

        try:
            delta_table = DeltaTable(delta_table_path)
            print(f"Delta table details for {dataset_name}: {delta_table.schema().to_pyarrow()}")

            abs_path = os.path.abspath(delta_table_path)
            print("Absolute path to delta table:", abs_path)
            print("Files in that path:", os.listdir(abs_path))

            count_query_result = con.execute(f"""
                SELECT COUNT(*) FROM delta_scan('{abs_path}')
            """).fetchone()

            if count_query_result:
                print(f"Number of rows in Delta table: {count_query_result[0]}")

        except Exception as e_check:
            print(f"Warning: Could not verify Delta table {dataset_name} after write: {e_check}")

    except Exception as e:
        print(f"Error processing {dataset_name} and writing to Delta Lake: {e}")
        raise

    finally:
        if 'con' in locals() and con:
            con.close()

def main():
    print("Starting Bronze Layer Ingestion...")
    for name, info in DATASETS.items():
        try:
            load_data_to_delta(name, info)
        except Exception as e:
            print(f"Failed to process {name}: {e}")
            continue
    print("\nBronze Layer Ingestion Finished.")

if __name__ == "__main__":
    main()

