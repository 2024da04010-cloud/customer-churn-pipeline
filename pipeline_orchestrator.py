import logging
import os
import sys

# Import the actual run functions from each module
from src.ingestion.ingest_data import run as ingest_data_run
from src.preparation.prepare_data import run as prepare_data_run
from src.validation.validate_data import run as validate_data_run
from src.transformation_and_storage.transform_and_store_data import run as transform_and_store_data_run
from src.model_building.model_building import run as model_building_run

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define pipeline steps as (step_name, function)
STEPS = [
    ("INGESTION", ingest_data_run),
    ("VALIDATION", validate_data_run),
    ("PREPARATION", prepare_data_run),
    ("TRANSFORMATION_AND_STORAGE", transform_and_store_data_run),
    ("MODEL_BUILDING", model_building_run)
]

def run_step(func, step_name):
    logger.info(f"🔹 Starting step: {step_name}")
    try:
        func()
        logger.info(f"✅ Completed step: {step_name}")
    except Exception as e:
        logger.error(f"❌ Error in step {step_name}: {e}", exc_info=True)
        raise

def main():
    logger.info("🚀 Starting Customer Churn Pipeline Orchestration")
    for step_name, func in STEPS:
        run_step(func, step_name)
    logger.info("🎉 Pipeline execution completed successfully")

if __name__ == "__main__":
    main()
