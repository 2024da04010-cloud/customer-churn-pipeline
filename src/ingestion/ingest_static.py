import pandas as pd
from datetime import datetime
import os
import logging
import src.utils.log_config as log_config

def fetch_static_data(source_path):

    try:
        df = pd.read_csv(source_path)
    except Exception as e:
        logging.error(f"[INGESTION] Static ingestion failed: {e}", exc_info=True)
        print(f"Error during static ingestion: {e}")

    raw_static_file_storage_directory = f'data/raw/static/'
    os.makedirs(raw_static_file_storage_directory, exist_ok=True)
    storage_path = os.path.join(raw_static_file_storage_directory, f"{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')} static_data.csv")
    df.to_csv(storage_path, index=False)

    logging.info(f"[INGESTION] Static ingestion successful. Rows: {len(df)}")
    logging.info(f"[INGESTION] Static data saved at: {storage_path}")
    print(f'Static data ingestion complete. Rows: {len(df)}')

    return df