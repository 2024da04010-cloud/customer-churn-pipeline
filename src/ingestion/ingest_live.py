import pandas as pd
import random
import string
import os
import logging
from datetime import datetime
import src.utils.log_config as log_config

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_PATH = os.path.join(BASE_DIR, "data/Telco-Customer-Churn.csv")

def get_static_df():
    return pd.read_csv(DATA_PATH)

try:
    static_df = get_static_df()
    logging.info("[INGESTION] Static data loaded successfully.")
except Exception as e:
    logging.error(f"[INGESTION] Live ingestion failed: {e}", exc_info=True)
    raise  # re-raise so Airflow marks task as failed properly

categorical_cols = static_df.select_dtypes(include=['object']).columns.tolist()
numerical_cols = static_df.select_dtypes(include=['int64', 'float64']).columns.tolist()
categorical_values = {col: static_df[col].dropna().unique().tolist() for col in categorical_cols}
numerical_ranges = {col: (static_df[col].min(), static_df[col].max()) for col in numerical_cols}

def generate_customer_id():
    """Generate a random live-data customer ID"""
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"LIVE-{suffix}"

def generate_live_data(n=10):
    live_rows = []
    for _ in range(n):
        row = {}
        for col in static_df.columns:
            if col == "customerID":
                row[col] = generate_customer_id()
            elif col in categorical_cols:
                row[col] = random.choice(categorical_values[col])
            elif col in numerical_cols:
                min_val, max_val = numerical_ranges[col]
                if pd.api.types.is_float_dtype(static_df[col]):
                    row[col] = round(random.uniform(min_val, max_val), 2)
                else:
                    row[col] = random.randint(int(min_val), int(max_val))
            else:
                row[col] = None
        live_rows.append(row)

    df = pd.DataFrame(live_rows)

    today = datetime.now().strftime("%Y-%m-%d")
    raw_dir = f"data/raw/live/"
    os.makedirs(raw_dir, exist_ok=True)
    filename = os.path.join(raw_dir, f"{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')} live_data.csv")
    df.to_csv(filename, index=False)

    logging.info(f"[INGESTION] Live ingestion successful. Rows: {len(df)}")
    print(f'Live data ingestion complete. Rows: {len(df)}')

    logging.info(f"[INGESTION] Live data saved at: {filename}")

    return df
