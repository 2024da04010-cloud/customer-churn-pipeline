import subprocess
import logging
import sys
import os

# --------------------------
# Set up Python path
# --------------------------
# Absolute path to the pipeline folder (assumes pipeline_orchestrator.py is inside customer-churn-pipeline/)
pipeline_dir = os.path.dirname(os.path.realpath(__file__))  # pipeline folder
parent_dir = os.path.dirname(pipeline_dir)  # points to DAG folder

# Add pipeline_dir and parent_dir to sys.path so 'src' can be imported
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
    ("INGESTION", "src.ingestion.ingest_data"),
    ("VALIDATION", "src.validation.validate_data"),
    ("PREPARATION", "src.preparation.prepare_data"),
    ("TRANSFORMATION_AND_STORAGE", "src.transformation_and_storage.transform_and_store_data"),
    ("MODEL_BUILDING", "src.model_building.model_building")
]

# --------------------------
# Run each step as module
# --------------------------
def run_step(script):
    try:
        result = subprocess.run(
            ["python", "-m", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True
        )

        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.warning(result.stderr)

    except subprocess.CalledProcessError as e:
        logging.error(f"Step {script} failed with error: \n{e.stderr}")
        sys.exit(1)

# --------------------------
# Main pipeline orchestrator
# --------------------------
def main():
    logging.info('----------------------------INITIATING PIPELINE----------------------------')

    for step_no, (task, script) in enumerate(STEPS):
        logging.info(f"STEP {step_no+1}/{len(STEPS)} - {task} - {script}")
        run_step(script)

    logging.info('--------------------------PIPELINE RUN SUCCESSFUL--------------------------')


if __name__ == "__main__":
    main()
