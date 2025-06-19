import json
import logging
from collections import defaultdict
from typing import Dict, Optional
import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_journal_with_most_drug_mentions(matches_path: str) -> Optional[Dict[str, object]]:
    """
    Extracts the journal that mentions the greatest number of unique drugs
    from a JSON file containing matching results.

    :param matches_path: Path to the JSON file containing matching results.
    :type matches_path: str

    :return: A dictionary with the journal name and the set of unique drugs it mentions,
             or None if no journal entries are found or an error occurs.
             Example: {"journal": "psychopharmacology", "mentions": {"tetracycline", "ethanol"}}
    :rtype: Optional[Dict[str, object]]

    :raises FileNotFoundError: If the input JSON file does not exist.
    :raises json.JSONDecodeError: If the JSON file is not properly formatted.
    :raises Exception: For any unexpected errors during processing.
    """

    try:
        with open(matches_path, "r", encoding="utf-8") as f:
            matches = json.load(f)

        journal_to_drugs = defaultdict(set)

        for entry in matches:
            if entry.get("ref_type") == "journal":
                journal = entry.get("title", "").strip().lower()
                drug = entry.get("drug", "").strip().lower()
                if journal and drug:
                    journal_to_drugs[journal].add(drug)

        if not journal_to_drugs:
            logging.warning("No journal mentionning drugs founds.")
            return None

        max_journal = max(journal_to_drugs.items(), key=lambda x: len(x[1]))
        max_journal_occurrences = journal_to_drugs[max_journal[0]]

        result = {"journal": max_journal[0], "mentions": max_journal_occurrences}

        logging.info(
            f"The journal mentioning the max number of drugs is "
            f"'{max_journal[0]}' with {len(max_journal[1])} drugs: "
            f"{max_journal_occurrences}."
        )

        return result
    except FileNotFoundError:
        logging.error(f"Error: The file {matches_path} does not exist.")
        raise FileNotFoundError(f"Error: The file {matches_path} does not exist.")
    except json.JSONDecodeError as e:
        logging.error(f"Error: Failed to parse JSON file. More details here: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred. More details here: {e}")

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("matches_path", type=str, help="Path to the matches file")
    args = parser.parse_args()
    get_journal_with_most_drug_mentions(args.matches_path)
