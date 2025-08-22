import os
import yaml
import pickle
import logging
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from xgboost import XGBClassifier

from src.feature_store.feature_store import FeatureStore
import src.utils.log_config as log_config

MODEL_DIR = "models"
MODEL_LOG = "models/model_versions.yaml"
os.makedirs(MODEL_DIR, exist_ok=True)

def load_data():
    """Load features from feature store (DB)."""
    fs = FeatureStore()
    df = fs.get_features(table="transformed_features")
    fs.close()

    # Assuming target column is 'Churn'
    X = df.drop(columns=["Churn", "customerID"], errors="ignore")
    y = df["Churn"].astype(int) 
    return train_test_split(X, y, test_size=0.2, random_state=42)

def evaluate_model(model, X_test, y_test):
    """Evaluate model with standard metrics."""
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred)
    }
    metrics = {k: round(float(v),2) for k, v in metrics.items()}
    return metrics


def save_model(model, metrics, model_name):
    model_path = f"models/{model_name}.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Load or initialize versions file
    versions_file = "models/model_versions.yaml"
    if os.path.exists(versions_file):
        with open(versions_file, "r") as f:
            model_versions = yaml.safe_load(f) or {}  # fallback if empty
    else:
        model_versions = {}

    # Ensure top-level "models" key exists
    if "models" not in model_versions:
        model_versions["models"] = {}

    # Get versions for this model
    existing_versions = model_versions["models"].get(model_name, [])
    new_version = f"v{len(existing_versions) + 1}"

    # Ensure metrics are serializable (convert NumPy scalars to floats)
    metrics = {k: float(v) for k, v in metrics.items()}

    # Add new version entry
    existing_versions.append({
        new_version: {
            "file": model_path,
            "metrics": metrics,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    })

    model_versions["models"][model_name] = existing_versions

    # Save back
    with open(versions_file, "w") as f:
        yaml.safe_dump(model_versions, f)

def main():
    X_train, X_test, y_train, y_test = load_data()

    models = {
        "LogisticRegression": LogisticRegression(max_iter=1000),
        "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
        "SVM": SVC(probability=True, kernel="rbf", random_state=42),
        "XGBoost": XGBClassifier( random_state=42, eval_metric="logloss")
    }

    best_model, best_metrics, best_algo = None, None, None

    logging.info(f"[MODEL] Training models: {', '.join(models.keys())}")

    for algo_name, model in models.items():
        model.fit(X_train, y_train)

        metrics = evaluate_model(model, X_test, y_test)
        logging.info(f"[MODEL] {algo_name} metrics: {metrics}")

        save_model(model, metrics, algo_name)

        if best_model is None or metrics["f1"] > best_metrics["f1"]:
            best_model, best_metrics, best_algo = model, metrics, algo_name

    logging.info(f"[MODEL] Best model: {best_algo} with F1 = {best_metrics['f1']:.4f}")

if __name__ == "__main__":
    main()
