import pandas as pd
from datetime import datetime
import os
import glob
import logging

import src.utils.log_config as log_config
from src.utils.data_versioning import log_data_version

from src.ingestion.ingest_static import fetch_static_data
from src.ingestion.ingest_live import generate_live_data

# ------------------- PATHS -------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_PATH, "data", "raw")
STATIC_RAW_PATH = os.path.join(DATA_PATH, "static")
LIVE_RAW_PATH = os.path.join(DATA_PATH, "live")
COMBINED_PATH = os.path.join(DATA_PATH, "combined")
MASTER_PATH = os.path.join(COMBINED_PATH, "master_combined_raw_data.csv")
REPORT_PATH = os.path.join(BASE_PATH, "data_validation_reports")

# Ensure directories exist
os.makedirs(REPORT_PATH, exist_ok=True)
os.makedirs(COMBINED_PATH, exist_ok=True)

# ------------------- EXPECTED VALUES -------------------
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

EXPECTED_RANGES = {
    "tenure": (0, 100),
    "MonthlyCharges": (0, 1000),
    "TotalCharges": (0, 100000),
}

# ------------------- FUNCTIONS -------------------
def validate(df: pd.DataFrame) -> pd.DataFrame:
    report = []

    # Data type checks
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            status = "Pass" if actual_type == expected_type else "Fail"
            details = f"{col} type {actual_type}, expected {expected_type}" if status=="Fail" else f"{col} type {actual_type} is correct"
            report.append((f"Type - {col}", status, details))
        else:
            report.append((f"Type - {col}", "Fail", "Missing column"))

    # Missing values
    for col, cnt in df.isnull().sum().items():
        status = "Pass" if cnt == 0 else "Fail"
        details = "No missing values" if status=="Pass" else f"{cnt} missing values"
        report.append((f"Missing - {col}", status, details))

    # Integrity checks
    if df["customerID"].isnull().any():
        report.append(("Integrity - Null ID", "Fail", "Null customerID found"))
    else:
        report.append(("Integrity - Null ID", "Pass", "No null customerIDs"))

    if df["customerID"].duplicated().any():
        report.append(("Integrity - Duplicates", "Fail", "Duplicate customerID found"))
    else:
        report.append(("Integrity - Duplicates", "Pass", "No duplicate customerIDs"))

    # Range checks
    for col, (low, high) in EXPECTED_RANGES.items():
        if col in df.columns:
            below = (df[col] < low).sum()
            above = (df[col] > high).sum()
            status = "Pass" if below==0 and above==0 else "Fail"
            details = f"{below} below {low}, {above} above {high}" if status=="Fail" else f"All values within {low}-{high}"
            report.append((f"Range - {col}", status, details))

    # Categorical domain checks
    for col, expected_vals in EXPECTED_CATEGORIES.items():
        if col in df.columns:
            invalid_vals = set(df[col].dropna().unique()) - set(expected_vals)
            status = "Fail" if invalid_vals else "Pass"
            details = f"Invalid values: {invalid_vals}" if status=="Fail" else "All values valid"
            report.append((f"Domain - {col}", status, details))
        else:
            report.append((f"Domain - {col}", "Fail", "Column missing"))

    return pd.DataFrame(report, columns=["Check", "Status", "Details"])


def get_latest_file(folder: str, pattern: str) -> str:
    files = glob.glob(os.path.join(folder, pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {folder} matching {pattern}")
    return max(files, key=os.path.getctime)


# ------------------- MAIN -------------------
def main():

    if not os.path.exists(MASTER_PATH):
        logging.info("[VALIDATION] Master raw data not found. Creating fresh master raw data...")

        static_file = get_latest_file(STATIC_RAW_PATH, "*static_data.csv")
        logging.info(f"[VALIDATION] Using latest static data: {static_file}")
        static_df = pd.read_csv(static_file)

        live_file = get_latest_file(LIVE_RAW_PATH, "*live_data.csv")
        logging.info(f"[VALIDATION] Using latest live data: {live_file}")
        live_df = pd.read_csv(live_file)

        combined_df = pd.concat([static_df, live_df])
        report_df = validate(combined_df)

        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_validation_report_combined.csv")
        report_df.to_csv(filename, index=False)
        logging.info(f"[VALIDATION] Report saved at: {filename}")

        if (report_df["Status"] == "Fail").any():
            logging.error("[VALIDATION] Validation failed. Master not created.")
        else:
            combined_df.to_csv(MASTER_PATH, index=False)
            logging.info(f"[VALIDATION] Master dataset saved at: {MASTER_PATH}")
            log_data_version(
                dataset_name="master_combined_raw_data.csv",
                file_path=MASTER_PATH,
                source="static data, live data",
                changelog="Created master data with static + live"
            )

    else:
        logging.info("[VALIDATION] Master data found. Appending new live data...")
        master_df = pd.read_csv(MASTER_PATH)

        live_file = get_latest_file(LIVE_RAW_PATH, "*live_data.csv")
        logging.info(f"[VALIDATION] Using latest live data: {live_file}")
        live_df = pd.read_csv(live_file)

        # Validate live data first
        live_report = validate(live_df)
        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_validation_report_live.csv")
        live_report.to_csv(filename, index=False)
        logging.info(f"[VALIDATION] Live data report saved at: {filename}")

        if (live_report["Status"] == "Fail").any():
            logging.error("[VALIDATION] Live data validation failed. Skipping update.")
            return

        new_master = pd.concat([master_df, live_df])
        combined_report = validate(new_master)
        filename = os.path.join(REPORT_PATH, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_validation_report_combined.csv")
        combined_report.to_csv(filename, index=False)
        logging.info(f"[VALIDATION] Updated master validation report saved at: {filename}")

        if (combined_report["Status"] == "Fail").any():
            logging.error("[VALIDATION] Combined data invalid. Master not updated.")
        else:
            new_master.to_csv(MASTER_PATH, index=False)
            logging.info(f"[VALIDATION] Master dataset updated. Rows: {len(new_master)}")
            log_data_version(
                dataset_name="master_combined_raw_data.csv",
                file_path=MASTER_PATH,
                source="static data, live data",
                changelog="Updated master with new live data"
            )


if __name__ == "__main__":
    main()
