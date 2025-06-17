from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DataAggregator:
    """
    A class to aggregate a list of formatted matchings e.g. matching issued from pubmed + matching issued
    from clinical trials as a single JSON object.

    Provides functionality to:
    - Flatten nested lists of dictionaries into a single list.
    - Deduplicate entries based on dictionary content.
    """

    def __init__(self):
        self.aggregated_data: List[Dict[str, str]] = []

    def _flatten(self, data: List[List[Dict[str, str]]]) -> List[Dict[str, str]]:
        """
        Flatten a list of lists of dictionaries into a single list.

        :param data: A list of lists, where each inner list contains dictionaries with string keys and values.
        :type data: List[List[Dict[str, str]]]
        :return: A flattened list of dictionaries.
        :rtype: List[Dict[str, str]]
        :raises ValueError: If input is not a list of lists of dictionaries.
        """

        if not isinstance(data, list) or not all(isinstance(sub, list) for sub in data):
            raise ValueError("Input must be a list of lists.")

        flattened = [item for sublist in data for item in sublist]
        logging.info(f"Flattened data into {len(flattened)} total entries.")
        self.aggregated_data = flattened
        return flattened

    def _deduplicate(self) -> List[Dict[str, str]]:
        """
        Remove duplicate dictionaries from the aggregated data.

        :return: A deduplicated list of dictionaries.
        :rtype: List[Dict[str, str]]
        """

        seen = set()
        unique_data = []
        for entry in self.aggregated_data:
            frozen = frozenset(entry.items())
            if frozen not in seen:
                seen.add(frozen)
                unique_data.append(entry)
        logging.info(f"Reduced to {len(unique_data)} unique entries.")
        self.aggregated_data = unique_data
        return unique_data

    def __call__(self, data: List[List[Dict[str, str]]]) -> List[Dict[str, str]]:
        """
        Aggregate data by flattening and deduplicating it.

        :param data: A list of lists of dictionaries to aggregate.
        :type data: List[List[Dict[str, str]]]
        :return: The aggregated (flattened and deduplicated) list of dictionaries.
        :rtype: List[Dict[str, str]]
        """

        self._flatten(data=data)
        return self._deduplicate()
