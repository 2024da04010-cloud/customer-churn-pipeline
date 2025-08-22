import pandas as pd
from datetime import datetime
import os
import glob
import logging

import src.utils.log_config as log_config
from src.utils.data_versioning import log_data_version

from src.ingestion.ingest_static import fetch_static_data
from src.ingestion.ingest_live import generate_live_data

MASTER_PATH = "data/raw/combined/master_combined_raw_data.csv"
REPORT_PATH = "data_validation_reports/"
os.makedirs(REPORT_PATH, exist_ok=True)

# Expected domains for categorical columns
EXPECTED_CATEGORIES = {
    "gender": ["Male", "Female"],
    "SeniorCitizen": [0, 1],
    "Partner": ["Yes", "No"],
    "Dependents": ["Yes", "No"],
    "PhoneService": ["Yes", "No"],
    "MultipleLines": ["Yes", "No", "No phone service"],
    "InternetService": ["DSL", "Fiber optic", "No"],
    "OnlineSecurity": ["Yes", "No", "No internet service"],
    "OnlineBackup": ["Yes", "No", "No internet service"],
    "DeviceProtection": ["Yes", "No", "No internet service"],
    "TechSupport": ["Yes", "No", "No internet service"],
    "StreamingTV": ["Yes", "No", "No internet service"],
    "StreamingMovies": ["Yes", "No", "No internet service"],
    "Contract": ["Month-to-month", "One year", "Two year"],
    "PaperlessBilling": ["Yes", "No"],
    "PaymentMethod": [
        "Electronic check",
        "Mailed check",
        "Bank transfer (automatic)",
        "Credit card (automatic)",
    ],
    "Churn": ["Yes", "No"],
}

# Expected schema and types
EXPECTED_SCHEMA = {
    "customerID": "object",
    "gender": "object",
    "SeniorCitizen": "int64",
    "Partner": "object",
    "Dependents": "object",
    "tenure": "int64",
    "PhoneService": "object",
    "MultipleLines": "object",
    "InternetService": "object",
    "OnlineSecurity": "object",
    "OnlineBackup": "object",
    "DeviceProtection": "object",
    "TechSupport": "object",
    "StreamingTV": "object",
    "StreamingMovies": "object",
    "Contract": "object",
    "PaperlessBilling": "object",
    "PaymentMethod": "object",
    "MonthlyCharges": "float64",
    "TotalCharges": "float64",
    "Churn": "object",
}

# Approximate expected ranges
EXPECTED_RANGES = {
    "tenure": (0, 100),               # 0–100 months
    "MonthlyCharges": (0, 1000),      # realistic monthly charges
    "TotalCharges": (0, 100000),      # total shouldn’t exceed this
}


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run validation checks and return a dataframe report.
    """
    report = []

     # Data Type Checks
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if actual_type == expected_type:
                report.append((f"Type - {col}", "Pass", f"{col} type {actual_type} is correct"))
            else:
                report.append((f"Type - {col}", "Fail", f"{col} type {actual_type}, expected {expected_type}"))
        else:
            report.append((f"Type - {col}", "Fail", f"Missing column {col}"))

    # Missing values
    missing = df.isnull().sum()
    for col, cnt in missing.items():
        if cnt > 0:
            report.append((f"Missing - {col}", "Fail", f"{cnt} missing values"))
        else:
            report.append((f"Missing - {col}", "Pass", "No missing values"))

    # Integrity Checks
    # Null ID
    if df["customerID"].isnull().any():
        report.append(("Integrity - Null ID", "Fail", "Null customerID found"))
    else:
        report.append(("Integrity - Null ID", "Pass", "No null customerIDs"))

    # Duplicate ID
    if df["customerID"].duplicated().any():
        report.append(("Integrity - Duplicates", "Fail", "Duplicate customerID found"))
    else:
        report.append(("Integrity - Duplicates", "Pass", "No duplicate customerIDs"))

    # Range Checks
    for col, (low, high) in EXPECTED_RANGES.items():
        if col in df.columns:
            below = (df[col] < low).sum()
            above = (df[col] > high).sum()
            if below > 0 or above > 0:
                report.append((f"Range - {col}", "Fail", f"{below} below {low}, {above} above {high}"))
            else:
                report.append((f"Range - {col}", "Pass", f"All values within {low}-{high}"))

    # --- Categorical domains ---
    for col, expected_vals in EXPECTED_CATEGORIES.items():
        if col in df.columns:
            invalid_vals = set(df[col].dropna().unique()) - set(expected_vals)
            if invalid_vals:
                report.append((f"Domain - {col}", "Fail", f"Invalid values: {invalid_vals}"))
            else:
                report.append((f"Domain - {col}", "Pass", "All values valid"))
        else:
            report.append((f"Domain - {col}", "Fail", "Column missing"))

    return pd.DataFrame(report, columns=["Check", "Status", "Details"])

def get_latest_file(folder: str, pattern: str) -> str:
    """
    Return the latest file in folder matching pattern.
    Example: get_latest_file("data/raw/static", "*static_data.csv")
    """
    files = glob.glob(os.path.join(folder, pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {folder} matching {pattern}")
    return max(files, key=os.path.getctime)  # newest based on creation time


def main():

    if not os.path.exists(MASTER_PATH):
        logging.info("[VALIDATION] Master raw data not found. Creating fresh master raw data...")

        static_file = get_latest_file("data/raw/static", "*static_data.csv")
        logging.info(f"[VALIDATION] Using latest static data: {static_file}")
        static_df = pd.read_csv(static_file)

        live_file = get_latest_file("data/raw/live", "*live_data.csv")
        logging.info(f"[VALIDATION] Using latest live data: {live_file}")
        live_df = pd.read_csv(live_file)

        combined_df = pd.concat([static_df, live_df])
    
        report_df = validate(combined_df)
        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')} validation_report_combined_data.csv")
        report_df.to_csv(filename, index=False)
        logging.info(f"[VALIDATION] Validation of master data complete. Report saved at: {filename}")

        if (report_df["Status"] == "Fail").any():
            logging.error("[VALIDATION] Validation failed. Master not created. See report.")
        else:
            combined_df.to_csv(MASTER_PATH, index=False)
            logging.info(f"[VALIDATION] Master data created. Rows: {len(combined_df)}")
            logging.info(f"[VALIDATION] Master dataset saved at: {MASTER_PATH}")
            log_data_version(
                dataset_name="master_combined_raw_data.csv",
                file_path=MASTER_PATH,
                source="static data, live data",
                changelog="updated master_data with static and live data."
            )
    else:
        logging.info("[VALIDATION] Master data found. Appending new live data...")
        master_df = pd.read_csv(MASTER_PATH)

        live_file = get_latest_file("data/raw/live", "*live_data.csv")
        logging.info(f"[VALIDATION] Using latest live data: {live_file}")
        live_df = pd.read_csv(live_file)

        # Validate live data first
        live_report = validate(live_df)
        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')} validation_report_live_data.csv")
        live_report.to_csv(filename, index=False)

        if (live_report["Status"] == "Fail").any():
            logging.error("[VALIDATION] Live data validation failed. Skipping update.")
            return
        else:
            logging.info(f"[VALIDATION] Validation of live data complete. Report saved at: {filename}")

        new_master = pd.concat([master_df, live_df])
        report_df = validate(new_master)
        #filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')} validation_report_combined_data.csv")
        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_validation_report_live_data.csv")
        report_df.to_csv(filename, index=False)
        logging.info(f"[VALIDATION] Validation of updated master data complete. Report saved at: {filename}")

        if (report_df["Status"] == "Fail").any():
            logging.error("[VALIDATION] Combined data invalid. Master not updated. See report.")
        else:
            new_master.to_csv(MASTER_PATH, index=False)
            logging.info(f"[VALIDATION] Master updated. Rows: {len(new_master)}")
            logging.info(f"[VALIDATION] Master dataset saved at: {MASTER_PATH}")
            log_data_version(
                dataset_name="master_combined_raw_data.csv",
                file_path=MASTER_PATH,
                source="static data, live data",
                changelog="updated master_data with live data."
            )


if __name__ == "__main__":
    main()