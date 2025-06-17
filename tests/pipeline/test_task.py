import os
import json
import pandas as pd
import pandas.testing as pdt
import src.pipeline.task as tasks
from tests.data.pipeline.task.input import TEST_TASK_INPUT_DATA_DIR
from tests.data.pipeline.task.expected import TEST_TASK_EXPECTED_DATA_DIR


def test_task_clean_drugs():
    df_drugs = pd.read_csv(os.path.join(TEST_TASK_INPUT_DATA_DIR, "drugs.csv"))
    df_result = tasks.task_clean_drugs(df_drugs)
    df_expected = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drugs_clean.json"))
    pdt.assert_frame_equal(df_expected, df_result)


def test_task_clean_clinical_trials():
    df_c_trials = pd.read_csv(os.path.join(TEST_TASK_INPUT_DATA_DIR, "clinical_trials.csv"))
    df_result = tasks.task_clean_clinical(df_c_trials)
    df_expected = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "clinical_trials_clean.json"))
    df_expected['date'] = df_expected['date'].astype(str)
    pdt.assert_frame_equal(df_expected, df_result)


def test_task_clean_merge_pubmed():
    df_pubmed_csv = pd.read_csv(os.path.join(TEST_TASK_INPUT_DATA_DIR, "pubmed.csv"))
    df_pubmed_json = pd.read_json(os.path.join(TEST_TASK_INPUT_DATA_DIR, "pubmed.json"))
    df_result = tasks.task_clean_merge_pubmed(df_pubmed_json, df_pubmed_csv)
    df_expected = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "pubmed_clean_merged.json"))
    df_expected['date'] = df_expected['date'].astype(str)
    pdt.assert_frame_equal(df_expected, df_result)


def test_task_matching_drug_clinical():
    df_drugs = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drugs_clean.json"))
    df_clinical = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "clinical_trials_clean.json"))
    matches_result = tasks.task_matching_drug_clinical(df_drugs, df_clinical)

    with open(
            os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drug_clinical_matches.json"), "r", encoding="utf-8") as f:
        matches_expected = json.load(f)
    assert matches_expected == matches_result


def test_task_matching_drug_pubmed():
    df_drugs = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drugs_clean.json"))
    df_pubmed = pd.read_json(os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "pubmed_clean_merged.json"))
    matches_result = tasks.task_matching_drug_pubmed(df_drugs, df_pubmed)
    with open(
            os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drug_pubmed_matches.json"), "r", encoding="utf-8") as f:
        matches_expected = json.load(f)
    assert matches_expected == matches_result


def test_task_aggregating_matches():
    with open(
            os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drug_pubmed_matches.json"), "r", encoding="utf-8") as f:
        drug_pubmed_matches = json.load(f)
    with open(
            os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "drug_clinical_matches.json"), "r", encoding="utf-8") as f:
        drug_clinical_matches = json.load(f)

    aggregated_result = tasks.task_aggregating_matches(drug_clinical_matches, drug_pubmed_matches)
    with open(
            os.path.join(TEST_TASK_EXPECTED_DATA_DIR, "aggregated_matches.json"), "r", encoding="utf-8") as f:
        aggregated_expected = json.load(f)
    assert aggregated_expected == aggregated_result
