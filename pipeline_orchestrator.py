import logging
import sys
import os

# --------------------------
# Set up Python path
# --------------------------
pipeline_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(pipeline_dir)

for p in [parent_dir, pipeline_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

# --------------------------
# Import pipeline modules
# --------------------------
try:
    from src.ingestion import ingest_data
    from src.preparation import prepare_data
    from src.validation import validate_data
    from src.transformation_and_storage import transform_and_store_data
    from src.model_building import model_building
    import src.utils.log_config as log_config
except ModuleNotFoundError as e:
    logging.error(f"Failed to import pipeline modules: {e}")
    raise

# --------------------------
# Pipeline steps
# --------------------------
STEPS = [
    ("INGESTION", ingest_data),
    ("VALIDATION", validate_data),
    ("PREPARATION", prepare_data),
    ("TRANSFORMATION_AND_STORAGE", transform_and_store_data),
    ("MODEL_BUILDING", model_building)
]

# --------------------------
# Run each step
# --------------------------
def run_step(func, name):
    try:
        logging.info(f"🚀 Running step: {name}")
        func()
        logging.info(f"✅ Step {name} completed successfully")
    except Exception as e:
        logging.error(f"❌ Step {name} failed: {e}")
        raise

# --------------------------
# Main pipeline orchestrator
# --------------------------
def main():
    logging.info('---------------------------- INITIATING PIPELINE ----------------------------')
    for step_name, func in STEPS:
        run_step(func, step_name)
    logging.info('-------------------------- PIPELINE RUN SUCCESSFUL --------------------------')

if __name__ == "__main__":
    main()
