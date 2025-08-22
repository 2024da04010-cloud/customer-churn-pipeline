import os
import yaml
from datetime import datetime

def log_data_version(dataset_name, file_path, source, changelog, version_log="data_version_log.yaml"):
    if os.path.exists(version_log):
        with open(version_log, "r") as f:
            log = yaml.safe_load(f) or {}
    else:
        log = {}

    if "versions" not in log:
        log["versions"] = {}

    if dataset_name not in log["versions"]:
        log["versions"][dataset_name] = []

    current_versions = log["versions"][dataset_name]
    version_number = len(current_versions) + 1

    new_entry = {
        f"v{version_number}": {
            "file": file_path,
            "source": source,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "changelog": changelog
        }
    }

    log["versions"][dataset_name].append(new_entry)

    with open(version_log, "w") as f:
        yaml.dump(log, f, sort_keys=False)