import os
import pandas as pd
import pytest
import pandas.testing as pdt
from src.pipeline.process.extract import load_json, load_csv
from tests.data.pipeline.process.extract import PROCESS_EXTRACT_DATA_TEST_DIR

# load_json tests

def test_load_json_success():
    expected_df = pd.DataFrame(
        [{"key1": "value11", "key2": "value12"}, {"key1": "value21", "key2": "value22"}])
    result_df = load_json(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "valid.json"))
    pdt.assert_frame_equal(expected_df, result_df)


def test_load_json_missing():
    with pytest.raises(Exception):
        load_json(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "non_existent_file.json"))


def test_load_json_malformed():
    with pytest.raises(Exception):
        load_json(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "malformed.json"))

# load_csv tests

def test_load_csv_success():
    expected_df = pd.DataFrame(
        [{"col1": "value11", "col2": "value12"}, {"col1": "value21", "col2": "value22"}])
    result_df = load_csv(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "valid.csv"))
    pdt.assert_frame_equal(expected_df, result_df)


def test_load_csv_missing():
    with pytest.raises(FileNotFoundError):
        load_csv(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "non_existent_file.csv"))


def test_load_csv_empty():
    with pytest.raises(pd.errors.EmptyDataError):
        load_csv(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "empty.csv"))


def test_load_csv_parse_error():
    with pytest.raises(pd.errors.ParserError):
        load_csv(os.path.join(PROCESS_EXTRACT_DATA_TEST_DIR, "malformed.csv"))
