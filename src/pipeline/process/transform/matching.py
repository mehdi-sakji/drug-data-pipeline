import pandas as pd
import logging
from typing import Dict, List
import re
from pandas import Timestamp

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class DataMatcher:
    """
    A class used to find and format matches between drugs and publication titles then journals
    from provided data sources.
    :param drug_col_name: Column name containing drug names in the drug DataFrame.
    :type drug_col_name: str
    :param pub_title_col_name: Column name containing publication titles in the publication DataFrame.
    :type pub_title_col_name: str
    :param journal_col_name: Column name containing journal names in the publication DataFrame.
    :type journal_col_name: str
    :param date_col_name: Column name containing publication dates in the publication DataFrame.
    :type date_col_name: str
    :param data_source: Name of the data source (used in output formatting).
    :type data_source: str
    """

    def __init__(
            self,
            drug_col_name: str,
            pub_title_col_name: str,
            journal_col_name: str,
            date_col_name: str,
            data_source: str):
        self.drug_col_name = drug_col_name
        self.pub_title_col_name = pub_title_col_name
        self.journal_col_name = journal_col_name
        self.date_col_name = date_col_name
        self.data_source = data_source

    def find_drug_pub_matches(self, df_drugs: pd.DataFrame, df_publications: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Identify matches between drug names and publication titles.

        :param df_drugs: DataFrame containing drug names.
        :type df_drugs: pd.DataFrame
        :param df_publications: DataFrame containing publication data.
        :type df_publications: pd.DataFrame
        :return: A list of dictionaries representing matched drugs and publication metadata.
        :rtype: List[Dict[str, str]]
        """

        matches = []
        drug_patterns = {
            drug: re.compile(rf"\b{re.escape(drug)}\b") for drug in df_drugs[self.drug_col_name].dropna()
        }
        for drug, pattern in drug_patterns.items():
            df_matching = df_publications[df_publications[self.pub_title_col_name].str.contains(
                pattern, regex=True, na=False)]
            for _, row in df_matching.iterrows():
                matches.append(
                    {
                        "drug": drug,
                        "source": self.data_source,
                        "title": row[self.pub_title_col_name],
                        "journal": row.get(self.journal_col_name),
                        "date": row.get(self.date_col_name)
                    }
                )
        logging.info(f"Found {len(matches)} drug mentions in publications.")
        return matches

    def _format_drug_pub_matches(self, matches: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Format matches between drug names and publication titles.

        :param matches: A list of matched drug-publication dictionaries.
        :type matches: List[Dict[str, str]]
        :return: A list of formatted matches with standardized keys.
        :rtype: List[Dict[str, str]]
        """

        formatted_matches = []
        for match in matches:
            formatted_matches.append({
                "drug": match["drug"],
                "title": match["title"],
                "ref_type": "{}_publication".format(self.data_source),
                "date_mention": match["date"]
            })
        return formatted_matches

    def format_drug_journal_matches(self, matches: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Format and deduplicate matches between drug names and journal entries.

        :param matches: A list of matched drug-publication dictionaries.
        :type matches: List[Dict[str, str]]
        :return: A list of formatted matches, including both publication and journal references.
        :rtype: List[Dict[str, str]]
        """

        formatted_matches = self._format_drug_pub_matches(matches)
        n_matchings_drugs_pub = len(formatted_matches)

        if not matches:
            logging.warning(
                "No matches found."
            )
            return []

        seen_journals = set()
        for match in matches:
            journal_val = match.get(self.journal_col_name)
            if journal_val:
                journal_cleaned = bytes(match[self.journal_col_name], "utf-8").decode("utf-8", errors="ignore")
                key = (match["drug"], journal_cleaned.strip(), match["date"])
                if key not in seen_journals:
                    seen_journals.add(key)
                    formatted_matches.append({
                        "drug": match[self.drug_col_name],
                        "title": journal_cleaned.strip(),
                        "ref_type": "journal",
                        "date_mention": match[self.date_col_name]
                    })
        logging.info(f"Found {len(matches)} drug mentions in publications.")
        logging.info(f"Found {len(formatted_matches) - n_matchings_drugs_pub} drug mentions in journals.")
        return formatted_matches


    @staticmethod
    def _normalize_dates(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Normalize the 'date_mention' field in the matching list of dictionaries to string format 'YYYY-MM-DD'.

        :param data: List of dictionaries each potentially containing a 'date_mention' key
                     whose value may be a pandas Timestamp object.
        :type data: list of dict
        :return: A new list of dictionaries with 'date_mention' converted to string format if it was a Timestamp.
        :rtype: list of dict
        """

        normalized = []
        for d in data:
            new_d = d.copy()
            dt = new_d.get('date_mention')
            if isinstance(dt, Timestamp):
                new_d['date_mention'] = dt.strftime('%Y-%m-%d')
            normalized.append(new_d)
        return normalized

    def __call__(self,  df_drugs: pd.DataFrame, df_publications: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Execute the data matching process when the object is called like a function.

        :param df_drugs: DataFrame containing drug names.
        :type df_drugs: pd.DataFrame
        :param df_publications: DataFrame containing publication and journal data.
        :type df_publications: pd.DataFrame
        :return: A list of formatted matches including publication and journal references.
        :rtype: List[Dict[str, str]]
        """

        drug_pub_matches = self.find_drug_pub_matches(df_drugs, df_publications)
        all_formatted_matches = self.format_drug_journal_matches(drug_pub_matches)
        all_formatted_matches = self._normalize_dates(all_formatted_matches)
        return all_formatted_matches
