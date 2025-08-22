import pandas as pd
import logging
import sqlite3
import numpy as np
import os
from sklearn.preprocessing import StandardScaler

import src.utils.log_config as log_config
from src.utils.data_versioning import log_data_version


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply feature engineering transformations on the dataset.
    """

    try:
        # Average monthly spend (avoid division by zero)
        df["AverageMonthlySpend"] = df.apply(
            lambda row: row["TotalCharges"] / row["tenure"] if row["tenure"] > 0 else 0,
            axis=1,
        )

        # Tenure groups
        df["TenureGroup"] = pd.cut(
            df["tenure"],
            bins=[0, 12, 24, 60, 120],
            labels=[1,2,5,10],
            right=False,
        )
        df['TenureGroup'] = df['TenureGroup'].astype(int)
        
        # Count of subscribed services
        service_cols = [
            "PhoneService",
            "MultipleLines",
            "OnlineSecurity",
            "OnlineBackup",
            "DeviceProtection",
            "TechSupport",
            "StreamingTV",
            "StreamingMovies",
        ]
        df["ServicesCount"] = df[service_cols].apply(lambda row: row.sum(), axis=1)

        # Partner or Dependents
        df["HasPartnerOrDependents"] = df[['Partner','Dependents']].apply(lambda row: row.sum(), axis=1)

        logging.info("[TRANSFORMATION] Feature engineering complete.")
        return df

    except Exception as e:
        logging.error(f"[TRANSFORMATION] Feature engineering failed: {e}", exc_info=True)
        raise

def scale_numeric(df):
    scaler = StandardScaler()

    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    binary_cols = [col for col in num_cols if set(df[col].dropna().unique()).issubset({0, 1, 2, 3})]
    scale_cols = [col for col in num_cols if col not in binary_cols]

    if scale_cols:
        df[scale_cols] = scaler.fit_transform(df[scale_cols])
        logging.info(f"[TRANSFORMATION] Scaled numeric features: {scale_cols}")
    else:
        logging.info("[TRANSFORMATION] No numeric features to scale")
    
    return df


def save_to_database(df: pd.DataFrame, db_path: str = "data/transformed/customer_churn.db"):
    """
    Save the transformed dataframe into an SQLite database.
    """
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path)
        df.to_sql("transformed_features", conn, if_exists="replace", index=False)
        conn.close()

        logging.info(
            f"[STORAGE] Transformed data saved successfully to {db_path} in table 'transformed_features'."
        )
        log_data_version(
            dataset_name="customer_churn.db",
            file_path=db_path,
            source="master raw data",
            changelog="applied transformations on master data."
        )
        

    except Exception as e:
        logging.error(f"[STORAGE] Failed to save to database: {e}", exc_info=True)
        raise


def main():
    try:
        # Load clean dataset (from preparation step)
        df = pd.read_csv("data/processed/cleaned_data.csv")
        original_cols = set(df.columns)

        # Apply transformations
        transformed_df = engineer_features(df)
        new_cols = set(transformed_df.columns) - original_cols
        logging.info(f"[TRANSFORMATION] New features created: {list(new_cols)}")

        transformed_df = scale_numeric(transformed_df)

        # Save back to CSV
        os.makedirs("data/transformed", exist_ok=True)
        transformed_df.to_csv("data/transformed/transformed_data.csv", index=False)
        log_data_version(
                dataset_name="transformed_data.csv",
                file_path="data/transformed/transformed_data.csv",
                source="master raw data",
                changelog="applied transformations on master data."
            )

        # Save to SQLite DB
        save_to_database(transformed_df)

        logging.info("[TRANSFORMATION] Transformation pipeline complete.")
        print("Transformation complete.")

    except Exception as e:
        logging.error(f"[TRANSFORMATION] Pipeline failed: {e}", exc_info=True)
        print(f"Error during transformation: {e}")


if __name__ == "__main__":
    main()
