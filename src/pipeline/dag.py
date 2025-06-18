from prefect import flow, task
from src.pipeline.task import *
from src.config.deploy_config import DeployConfig

@flow(name='dag')
def main_flow(d_config: DeployConfig):
    """
    Prefect workflow to orchestrate the entire drug-publication matching pipeline.

    This flow coordinates the extraction, cleaning, matching, aggregation,
    and saving of drug-related clinical trial and publication data.

    :param d_config: Deployment configuration object containing paths to input data
                     and output locations.
    :type d_config: DeployConfig
    """

    run(d_config)


@task
def run(d_config):
    """
    Execute the main data processing pipeline steps as a Prefect task.

    Steps performed:
    1. Extract drug data from clinical trials source.
    2. Extract publication data from PubMed (both JSON and CSV).
    3. Extract clinical trial data from clinical trials source.
    4. Clean the drug data.
    5. Clean and merge PubMed data from JSON and CSV sources.
    6. Clean clinical trial data.
    7. Perform matching of drugs with clinical trial data.
    8. Perform matching of drugs with PubMed publication data.
    9. Aggregate matching results from clinical and publication sources.
    10. Save aggregated matching results to the configured output path.

    :param d_config: Deployment configuration object containing all necessary file paths.
    :type d_config: DeployConfig
    :return: None
    """

    df_drugs = task_extract_clinical_trials(
        path_to_clinical_trials=d_config.path_to_drugs
    )
    df_pubmed_json, df_pubmed_csv = task_extract_pubmed(
        path_to_pubmed_csv=d_config.path_to_pubmed_csv,
        path_to_pubmed_json=d_config.path_to_pubmed_json
    )
    df_clinical_trials = task_extract_clinical_trials(
        path_to_clinical_trials=d_config.path_to_clinical_trials
    )
    df_drugs = task_clean_drugs(df_drugs=df_drugs)
    df_pubmed = task_clean_merge_pubmed(
        df_pubmed_json=df_pubmed_json, df_pubmed_csv=df_pubmed_csv
    )
    df_clinical_trials = task_clean_clinical(df_clinical_trials=df_clinical_trials)
    drug_clinical_matches = task_matching_drug_clinical(
        df_drugs=df_drugs, df_clinical_trials=df_clinical_trials
    )
    drug_pubmed_matches = task_matching_drug_pubmed(
        df_drugs=df_drugs, df_pubmed=df_pubmed
    )
    aggregated_matches = task_aggregating_matches(
        drug_clinical_matches=drug_clinical_matches, drug_pubmed_matches=drug_pubmed_matches
    )
    task_load_matches(
        aggregated_matches=aggregated_matches, file_output_path=d_config.path_to_output_matching,
    )
