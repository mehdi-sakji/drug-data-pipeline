import pandas as pd
from typing import Dict, List
from src.config.build_config import COLS_CLEAN_MAPPING, COLS_MATCH_MAPPING
from src.pipeline.process.extract import load_csv
from src.pipeline.process.extract import load_json
from src.pipeline.process.transform.utils import concatenate_dataframe_list
from src.pipeline.process.transform.cleaning import DataCleaner
from src.pipeline.process.transform.matching import DataMatcher
from src.pipeline.process.transform.aggregating import DataAggregator
from src.pipeline.process.load import save_json

def task_extract_drugs(path_to_drugs: str) -> pd.DataFrame:
    """
    Extract the drugs dataset from a CSV file.

    :param path_to_drugs: Path to the drugs CSV file.
    :type path_to_drugs: str
    :return: Loaded drugs DataFrame.
    :rtype: pd.DataFrame
    """

    df_drugs = load_csv(csv_path=path_to_drugs)
    return df_drugs


def task_extract_pubmed(path_to_pubmed_csv: str, path_to_pubmed_json: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract PubMed data from both CSV and JSON files.

    :param path_to_pubmed_csv: Path to the PubMed CSV file.
    :type path_to_pubmed_csv: str
    :param path_to_pubmed_json: Path to the PubMed JSON file.
    :type path_to_pubmed_json: str
    :return: Tuple containing DataFrames for PubMed JSON and CSV data.
    :rtype: tuple[pd.DataFrame, pd.DataFrame]
    """

    df_pubmed_json = load_json(json_path=path_to_pubmed_json)
    df_pubmed_csv = load_csv(csv_path=path_to_pubmed_csv)
    return df_pubmed_json, df_pubmed_csv


def task_extract_clinical_trials(path_to_clinical_trials: str) -> pd.DataFrame:
    """
    Extract the clinical trials dataset from a CSV file.

    :param path_to_clinical_trials: Path to the clinical trials CSV file.
    :type path_to_clinical_trials: str
    :return: Loaded clinical trials DataFrame.
    :rtype: pd.DataFrame
    """

    df_clinical_trials = load_csv(csv_path=path_to_clinical_trials)
    return df_clinical_trials


def task_clean_drugs(df_drugs: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the drugs DataFrame using the configuration specified in COLS_CLEAN_MAPPING.

    :param df_drugs: Raw drugs DataFrame.
    :type df_drugs: pd.DataFrame
    :return: Cleaned drugs DataFrame.
    :rtype: pd.DataFrame
    """

    data_cleaner = DataCleaner(**COLS_CLEAN_MAPPING["drugs"])
    df_drugs = data_cleaner(df=df_drugs)
    return df_drugs


def task_clean_merge_pubmed(df_pubmed_json: pd.DataFrame, df_pubmed_csv: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and merge PubMed data from JSON and CSV sources.

    :param df_pubmed_json: Raw PubMed data from JSON.
    :type df_pubmed_json: pd.DataFrame
    :param df_pubmed_csv: Raw PubMed data from CSV.
    :type df_pubmed_csv: pd.DataFrame
    :return: Cleaned and merged PubMed DataFrame.
    :rtype: pd.DataFrame
    """

    data_cleaner = DataCleaner(**COLS_CLEAN_MAPPING["pubmed"])
    df_pubmed_json = data_cleaner(df=df_pubmed_json)
    df_pubmed_csv = data_cleaner(df=df_pubmed_csv)
    df_pubmed = concatenate_dataframe_list(dfs=[df_pubmed_json, df_pubmed_csv])
    return df_pubmed


def task_clean_clinical(df_clinical_trials: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the clinical trials DataFrame using the configuration specified in COLS_CLEAN_MAPPING.

    :param df_clinical_trials: Raw clinical trials DataFrame.
    :type df_clinical_trials: pd.DataFrame
    :return: Cleaned clinical trials DataFrame.
    :rtype: pd.DataFrame
    """

    data_cleaner = DataCleaner(**COLS_CLEAN_MAPPING["clinical"])
    df_clinical_trials = data_cleaner(df=df_clinical_trials)
    return df_clinical_trials


def task_matching_drug_clinical(df_drugs: pd.DataFrame,  df_clinical_trials: pd.DataFrame) -> List[Dict[str, str]]:
    """
    Perform matching between drug names and clinical trial titles.

    :param df_drugs: Cleaned drugs DataFrame.
    :type df_drugs: pd.DataFrame
    :param df_clinical_trials: Cleaned clinical trials DataFrame.
    :type df_clinical_trials: pd.DataFrame
    :return: List of dictionaries with matched clinical trial entries.
    :rtype: List[Dict[str, str]]
    """

    data_matcher = DataMatcher(**COLS_MATCH_MAPPING["drugs_clinical"])
    drug_clinical_matches = data_matcher(df_drugs=df_drugs, df_publications=df_clinical_trials)
    return drug_clinical_matches


def task_matching_drug_pubmed(df_drugs: pd.DataFrame,  df_pubmed: pd.DataFrame) ->  List[Dict[str, str]]:
    """
    Perform matching between drug names and PubMed publication titles.

    :param df_drugs: Cleaned drugs DataFrame.
    :type df_drugs: pd.DataFrame
    :param df_pubmed: Cleaned and combined PubMed DataFrame (CSV + JSON).
    :type df_pubmed: pd.DataFrame
    :return: List of dictionaries with matched PubMed entries.
    :rtype: List[Dict[str, str]]
    """

    data_matcher = DataMatcher(**COLS_MATCH_MAPPING["drugs_pubmed"])
    drug_pubmed_matches = data_matcher(df_drugs=df_drugs, df_publications=df_pubmed)
    return drug_pubmed_matches


def task_aggregating_matches(
        drug_clinical_matches: List[Dict[str, str]],
        drug_pubmed_matches: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Aggregate matched results from clinical trials and PubMed publications.

    :param drug_clinical_matches: Matches between drugs and clinical trials.
    :type drug_clinical_matches: List[Dict[str, str]]
    :param drug_pubmed_matches: Matches between drugs and PubMed publications.
    :type drug_pubmed_matches: List[Dict[str, str]]
    :return: Aggregated list of all matches.
    :rtype: List[Dict[str, str]]
    """

    data_aggregator = DataAggregator()
    aggregated_matches = data_aggregator(data=[drug_clinical_matches, drug_pubmed_matches])
    return aggregated_matches


def task_load_matches(aggregated_matches: List[Dict[str, str]], file_output_path: str) -> None:
    """
    Save aggregated matching results to a JSON file.

    :param aggregated_matches: A list of dictionaries containing aggregated drug-publication matches.
    :type aggregated_matches: List[Dict[str, str]]
    :param file_output_path: The file path (including filename) where the JSON output will be saved.
    :type file_output_path: str
    :return: None
    """

    save_json(data=aggregated_matches, file_output_path=file_output_path)
