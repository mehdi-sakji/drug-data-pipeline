import os
import json
from prefect.testing.utilities import prefect_test_harness
from src.config.deploy_config import DeployConfig
from src.pipeline.dag import main_flow
from tests.data.pipeline.task import TEST_TASK_DATA_DIR

def test_dag():
    os.makedirs(os.path.join(TEST_TASK_DATA_DIR, "output"), exist_ok=True)

    d_config_dict = {
        "path_to_drugs": os.path.join(TEST_TASK_DATA_DIR, "input", "drugs.csv"),
        "path_to_pubmed_csv": os.path.join(TEST_TASK_DATA_DIR, "input", "pubmed.csv"),
        "path_to_pubmed_json": os.path.join(TEST_TASK_DATA_DIR, "input", "pubmed.json"),
        "path_to_clinical_trials": os.path.join(TEST_TASK_DATA_DIR, "input", "clinical_trials.csv"),
        "path_to_output_matching": os.path.join(TEST_TASK_DATA_DIR, "output", "aggregated_matches.json"),
    }

    test_config = DeployConfig(**d_config_dict)

    with prefect_test_harness():
        main_flow(test_config)

    with open(
            os.path.join(TEST_TASK_DATA_DIR, "expected", "aggregated_matches.json"), "r", encoding="utf-8") as f:
        matches_expected = json.load(f)

    with open(
            os.path.join(TEST_TASK_DATA_DIR, "output", "aggregated_matches.json"), "r", encoding="utf-8") as f:
        matches_results = json.load(f)

    assert matches_expected == matches_results
