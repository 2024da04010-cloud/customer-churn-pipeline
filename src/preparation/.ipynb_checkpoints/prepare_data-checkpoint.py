import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder

import src.utils.log_config as log_config

DATA_PATH = "data/raw/combined/master_combined_raw_data.csv"
OUTPUT_CLEAN_DATA = "data/processed/cleaned_data.csv"
EDA_OUTPUT_DIR = f"reports/eda/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"

os.makedirs(os.path.dirname(OUTPUT_CLEAN_DATA), exist_ok=True)
os.makedirs(EDA_OUTPUT_DIR, exist_ok=True)


def load_data(path):
    if not os.path.exists(path):
        logging.error(f"[PREPARATION] File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    logging.info(f"[PREPARATION] Loaded dataset. Shape: {df.shape}")
    return df


def handle_missing_values(df):
    missing_report = df.isnull().sum()
    logging.info(f"[PREPARATION] Missing values:\n{missing_report[missing_report > 0]}")

    # Drop rows with missing target
    if "Churn" in df.columns:
        df = df.dropna(subset=["Churn"])

    # Impute numeric with median, categorical with mode
    for col in df.columns:
        if df[col].dtype in [np.float64, np.int64]:
            df[col].fillna(df[col].median(), inplace=True)
        else:
            df[col].fillna(df[col].mode()[0], inplace=True)

    logging.info("[PREPARATION] Missing values handled.")
    return df


def encode_categoricals(df):
    label_enc = LabelEncoder()
    for col in df.select_dtypes(include="object").columns:
        if col == "customerID":
            continue
        df[col] = label_enc.fit_transform(df[col])
    logging.info("[PREPARATION] Categorical variables encoded")
    return df


def scale_numeric(df):
    scaler = StandardScaler()

    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    binary_cols = [col for col in num_cols if set(df[col].dropna().unique()).issubset({0, 1, 2, 3})]
    scale_cols = [col for col in num_cols if col not in binary_cols]

    if scale_cols:
        df[scale_cols] = scaler.fit_transform(df[scale_cols])
        logging.info(f"[PREPARATION] Scaled numeric features: {scale_cols}")
    else:
        logging.info("[PREPARATION] No numeric features to scale")

    if binary_cols:
        logging.info(f"[PREPARATION] Skipped binary columns: {binary_cols}")
    
    return df
    

def detect_outliers_iqr(df, column, multiplier=2.0):
    """
    Detect outliers using a relaxed IQR rule.
    multiplier=1.5 is stricter, 2.0 is more relaxed.
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    return outliers, lower_bound, upper_bound


def perform_eda(df):
    try:
        # Histograms
        for col in df.columns:
            plt.figure(figsize=(6, 4))
            sns.histplot(df[col], kde=True, bins=30)
            plt.title(f"Distribution of {col}")
            plt.savefig(os.path.join(EDA_OUTPUT_DIR, f"hist_{col}.png"))
            plt.close()

        # outliers check
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if "SeniorCitizen" in numeric_cols:
            numeric_cols.remove("SeniorCitizen")
        if "Churn" in numeric_cols:
            numeric_cols.remove("Churn")
        outlier_found = False  # track if any outliers exist
        for col in numeric_cols:
            outliers, lb, ub = detect_outliers_iqr(df, col, multiplier=2.0)
            if not outliers.empty:
                outlier_found = True
                logging.info(
                    f"[PREPARATION] Column: {col} | Outliers detected: {outliers.shape[0]} | "
                    f"Lower Bound: {lb:.2f}, Upper Bound: {ub:.2f}"
                )
                logging.debug(f"[PREPARATION] Sample outliers in {col}:\n{outliers.head(5)}")
        if not outlier_found:
            logging.info("[PREPARATION] No outliers detected in any numeric column.")

        # Boxplots
        for col in df.select_dtypes(include=[np.number]).columns:
            plt.figure(figsize=(6, 4))
            sns.boxplot(x=df[col])
            plt.title(f"Boxplot of {col}")
            plt.savefig(os.path.join(EDA_OUTPUT_DIR, f"box_{col}.png"))
            plt.close()

        # Correlation heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(df.corr(), annot=False, cmap="coolwarm")
        plt.title("Correlation Heatmap")
        plt.savefig(os.path.join(EDA_OUTPUT_DIR, "correlation_heatmap.png"))
        plt.close()

        logging.info(f"[PREPARATION] EDA visualizations saved in {EDA_OUTPUT_DIR}")

    except Exception as e:
        logging.error(f"[PREPARATION] EDA generation failed: {e}", exc_info=True)


# -------------------
# Main Pipeline
# -------------------
def main():
    try:
        logging.info("[PREPARATION] Starting data preparation pipeline")

        df = load_data(DATA_PATH)
        df = handle_missing_values(df)

        perform_eda(df)

        df = encode_categoricals(df)
        # df = scale_numeric(df)

        df.to_csv(OUTPUT_CLEAN_DATA, index=False)
        logging.info(f"[PREPARATION] Cleaned dataset saved to {OUTPUT_CLEAN_DATA}")

        print("Data preparation complete.")

    except Exception as e:
        logging.error(f"[PREPARATION] Pipeline failed: {e}", exc_info=True)
        print(f"Error during data preparation: {e}")


if __name__ == "__main__":
    main()
