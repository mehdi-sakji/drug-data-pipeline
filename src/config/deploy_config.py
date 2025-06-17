"""
This module contains declarations of config variables used for deploying prefect's workflow in production.
(to be deferenciated from inner config variables instanciated when building the project."
"""

from pydantic import BaseModel


class DeployConfig(BaseModel):
    """
    Configuration model for deployment paths used by the workflow.

    :param path_to_drugs: Path to the file or directory containing drug data.
    :type path_to_drugs: str

    :param path_to_pubmed_csv: Path to the pubmed CSV file.
    :type path_to_pubmed_csv: str

    :param path_to_pubmed_json: Path to the pubmed JSON file.
    :type path_to_pubmed_json: str

    :param path_to_clinical_trials: Path to the clinical trials CSV file.
    :type path_to_clinical_trials: str

    :param path_to_output_matching: Path where output matching results will be saved under JSON format.
    :type path_to_output_matching: str
    """

    path_to_drugs : str
    path_to_pubmed_csv : str
    path_to_pubmed_json: str
    path_to_clinical_trials: str
    path_to_output_matching: str
