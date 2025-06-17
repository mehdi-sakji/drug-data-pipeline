import pandas as pd
import logging
from json import JSONDecodeError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

ENCODING = "utf-8"

def load_json(json_path: str) -> pd.DataFrame:
    """
    Load a JSON file into a Pandas DataFrame.

    :param json_path: Path to the input JSON file.
    :type json_path: str
    :return: The loaded Pandas DataFrame, or None if an exception occurred.
    :rtype: pd.DataFrame or None
    :raises Exception: If the file does not exist at the specified path.
    :raises ValueError: If the file is not valid JSON.
    :raises Exception: For any unexpected error during loading.
    """

    try:
        df = pd.read_json(json_path, encoding=ENCODING)
        logging.info(f"Successfully loaded JSON: {json_path}")
        return df

    except FileNotFoundError:
        logging.error(f"JSON file not found at: {json_path}")
        raise Exception(f"JSON file not found at: {json_path}")
    except ValueError:
        logging.error(f"Failed to parse file to json object: {json_path}")
        raise Exception(f"Failed to parse file to json object: {json_path}")
    except Exception as e:
        logging.error(
            f"Exception occured when loading file to json object: {json_path}\n"
            f"More details here : {e}"
        )
        raise Exception(
            f"Exception occured when loading file to json object: {json_path}\n"
            f"More details here : {e}"
        )

def load_csv(csv_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a Pandas DataFrame.

    :param csv_path: Path to the input CSV file.
    :type csv_path: str
    :return: The loaded Pandas DataFrame, or None if an exception occurred.
    :rtype: pd.DataFrame or None
    :raises FileNotFoundError: If the file does not exist at the specified path.
    :raises pd.errors.EmptyDataError: If the CSV file is empty.
    :raises pd.errors.ParserError: If the CSV cannot be parsed properly.
    :raises Exception: For any other unexpected error during loading.
    """

    try:
        df = pd.read_csv(csv_path, encoding=ENCODING)
        logging.info(f"Successfully loaded CSV: {csv_path}")
        return df

    except FileNotFoundError:
        logging.error(f"CSV File not found at: {csv_path}")
        raise FileNotFoundError(f"CSV File not found at: {csv_path}")
    except pd.errors.EmptyDataError:
        logging.warning(f"CSV file is empty: {csv_path}")
        raise pd.errors.EmptyDataError(f"CSV file is empty: {csv_path}")
    except pd.errors.ParserError:
        logging.error(f"Failed to parse CSV file: {csv_path}")
        raise pd.errors.ParserError(f"Failed to parse CSV file: {csv_path}")
    except Exception as e:
        logging.error(
            f"Exception occured when loading CSV file: {csv_path}\n"
            f"More details here : {e}"
        )
        raise Exception(
            f"Exception occured when loading CSV file: {csv_path}\n"
            f"More details here : {e}"
        )
