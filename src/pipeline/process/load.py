import json
import os
import logging
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def save_json(data: List[Dict[str, str]], file_output_path: str) -> None:
    """
    Saves a list of dictionaries e.g. drug publication matching results to a JSON file.

    :param data: The data to save; must be serializable to JSON.
    :type data: List[Dict[str, str]]
    :param file_output_path: The path (including filename) to save the JSON output.
    :type file_output_path: str
    :raises ValueError: If the data is not serializable to JSON.
    :raises IOError: If there is an issue writing the file.
    :return: None
    :rtype: None
    """

    try:
        json.dumps(data, ensure_ascii=False, indent=4)

        dir_name = os.path.dirname(file_output_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(file_output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

        logging.info(f"JSON file successfully saved at: {file_output_path}")

    except TypeError as e:
        logging.error(f"Data is not serializable to JSON: {e}")
        raise ValueError(f"Data provided is not serializable to JSON: {e}")

    except OSError as e:
        logging.error(f"Failed to write JSON file at {file_output_path}: {e}")
        raise OSError(f"Failed to write JSON file at {file_output_path}: {e}")
