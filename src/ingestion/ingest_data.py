import random
import pandas as pd
import logging
from datetime import datetime
import os

import src.utils.log_config as log_config
from src.ingestion.ingest_static import fetch_static_data
from src.ingestion.ingest_live import generate_live_data


def main():
    try:
        fetch_static_data('data/Telco-Customer-Churn.csv')

        generate_live_data(n=random.randint(5,25))

        logging.info(f"[INGESTION] Ingestion complete.")
        print(f'Ingestion complete.')

    except Exception as e:
        logging.error(f"[INGESTION] Ingestion failed: {e}", exc_info=True)
        print(f"Error during ingestion: {e}")

if __name__=='__main__':
    main()