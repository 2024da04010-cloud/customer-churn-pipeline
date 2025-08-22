import sqlite3
import pandas as pd
import yaml
import logging

import src.utils.log_config as log_config

class FeatureStore:
    def __init__(self, db_path="data/transformed/customer_churn.db", metadata_path="feature_metadata.yaml"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        with open(metadata_path, "r") as f:
            self.metadata = yaml.safe_load(f)["features"]

    def list_features(self):
        """Return available features with metadata"""
        return self.metadata

    def get_features(self, entity_ids=None, feature_list=None, table="transformed_features"):
        features_to_select = "*" if feature_list is None else ",".join(feature_list)
        query = f"SELECT {features_to_select} FROM {table}"
        if entity_ids:
            ids = ",".join([f"'{eid}'" for eid in entity_ids])
            query += f" WHERE customerID IN ({ids})"

        logging.info(f"[FEATURE_STORE] Fetching features from feature store {table}")
        return pd.read_sql(query, self.conn)

    def close(self):
        self.conn.close()
