import pandas as pd
from typing import List
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DataCleaner:
    """
    A utility class for cleaning and standardizing data in a pandas DataFrame.

    The class provides methods to:
    - Clean ID columns
    - Standardize date values to a specific format
    - Remove rows with missing values in specific columns
    - Remove special characters from specific text columns
    - Normalize text formatting (lowercase, whitespace trimming) for specific columns

    :param date_columns: List of column names containing date values to standardize.
    :type date_columns: list
    :param drop_na_columns: List of column names to drop rows if they contain missing values.
    :type drop_na_columns: list
    :param text_search_columns: List of text columns to clean and standardize.
    :type text_search_columns: list
    :param id_column: Name of the ID column to clean.
    :type id_column: str
    :param id_prefix: Prefix to add to each ID value for uniqueness.
    :type id_prefix: str
    """

    def __init__(
            self, date_columns: list, drop_na_columns: list, text_search_columns: list, id_column: str, id_prefix: str):
        self.standard_date_format = "%Y-%m-%d"
        self.date_columns = date_columns
        self.drop_na_columns = drop_na_columns
        self.text_search_columns = text_search_columns
        self.id_prefix = id_prefix
        self.id_column = id_column

    def clean_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean ID by homogenizing type to str and adding a prefix to distinguish identical IDs from different
        sources of input data.

        :param df: Dataframe.
        :type df: pd.DataFrame
        :return: Dataframe with cleaned ID.
        :rtype: pd.DataFrame
        :raises ValueError: If the specified column is not found in the dataframe.
        :raises Exception: For any other errors encountered during processing.
        """

        try:
            df[self.id_column] = df[self.id_column].astype(str)
            df[self.id_column] = df[self.id_column].apply(
                lambda x: "{}_{}".format(self.id_prefix, x)
            )
            df = df.drop_duplicates(subset=[self.id_column]).reset_index(drop=True)
            logging.info(f"Cleaned ID '{self.id_column}'.")
            return df
        except KeyError:
            logging.error(f"Column '{self.id_column}' not found in the dataframe.")
            raise ValueError(f"Column '{self.id_column}' not found in the dataframe.")
        except Exception as e:
            logging.error(f"Error converting ID '{self.id_column}' column to string. More details here: {e}")
            raise Exception(f"Error converting ID '{self.id_column}' column to string. More details here: {e}")

    def standardize_date_format(self, df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        """
        Standardize all date values to a specific format for a given column.

        :param df: Dataframe.
        :type df: pd.DataFrame
        :param date_column: Date column name.
        :type date_column: str.
        :return: Dataframe with column standardized date.
        :rtype: pd.DataFrame
        :raises ValueError: If the specified column is not found in the dataframe.
        :raises Exception: For any other errors encountered during processing.
        """

        try:
            df[date_column] = df[date_column].astype(str).str.replace('/', '-', regex=False)
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce").dt.strftime(
                self.standard_date_format
            )
            logging.info(f"Standardized date column '{date_column}' to '{self.standard_date_format}' format.")
            return df
        except KeyError:
            logging.error(f"Column '{date_column}' not found in the dataframe.")
            raise ValueError(f"Column '{date_column}' not found in the dataframe.")
        except Exception as e:
            logging.error(f"Error standardizing date values. More details here: {e}")
            raise Exception(f"Error standardizing date values. More details here: {e}")

    @staticmethod
    def remove_rows_missing_column_value(df: pd.DataFrame, column_to_drop: str) -> pd.DataFrame:
        """
        Remove rows where a given column NaN.

        :param df: Dataframe.
        :type df: pd.DataFrame
        :param column_to_drop: Name of column to drop row containing NaN value on it.
        :type column_to_drop: str
        :return: The dataframe with rows with NaN values for given columns removed.
        :rtype: pd.DataFrame
        :raises ValueError: If the specified column is not found in the dataframe.
        :raises Exception: For any other errors encountered during processing.
        """

        try:
            logging.info(f"Dropped rows where '{column_to_drop}' is NaN.")
            return df.dropna(subset=[column_to_drop])
        except KeyError:
            logging.error(f"Columns '{column_to_drop}' not found in the dataframe.")
            raise ValueError(
                f"Columns '{column_to_drop}' not found in the dataframe."
            )
        except Exception as e:
            logging.error(f"Error removing rows with empty titles or journals. More details here: {e}")
            raise Exception(f"Error removing rows with empty titles or journals. More details here: {e}")

    @staticmethod
    def remove_special_characters(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        Remove special characters from a specified text column in a dataframe.
        Only ASCII characters, word characters, whitespace, and hyphens are preserved.

        :param df: Dataframe.
        :type df: pd.DataFrame
        :param column_name: Name of column from which to remove special characters.
        :type column_name: str
        :return: Dataframe with specified column cleaned of special characters.
        :rtype: pd.DataFrame
        :raises ValueError: If the specified column is not found in the dataframe.
        :raises Exception: For any other errors encountered during processing.
        """

        try:
            df[column_name] = (
                df[column_name].str.encode("ascii", "ignore").str.decode("utf-8")
            )
            df[column_name] = df[column_name].str.replace(r"[^\w\s-]", "", regex=True)
            logging.info(f"Cleaned special characters from column '{column_name}'.")
            return df
        except KeyError:
            logging.error(f"Column '{column_name}' not found in the dataframe.")
            raise ValueError(f"Column '{column_name}' not found in the dataframe.")
        except Exception as e:
            logging.error(f"Error cleaning special characters. More details here: {e}")
            raise Exception(f"Error cleaning special characters. More details here: {e}")

    @staticmethod
    def standardize_text(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        Standardize text in a specified column by applying lower casing, trimming whitespace,
        and replacing multiple spaces with a single space.

        :param df: Dataframe.
        :type df: pd.DataFrame
        :param column_name: Name of the column to standardize.
        :type column_name: str
        :return: Dataframe with the standardized text column.
        :rtype: pd.DataFrame
        :raises ValueError: If the specified column is not found in the dataframe.
        :raises Exception: For any other errors encountered during processing.
        """

        try:
            df[column_name] = (
                df[column_name]
                .str.lower()
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )
            logging.info(f"Standardized text within column '{column_name}'.")
            return df
        except KeyError:
            logging.error(f"Column '{column_name}' not found in the dataframe.")
            raise ValueError(f"Column '{column_name}' not found in the dataframe.")
        except Exception as e:
            logging.error(f"Error standardizing text. More details here: {e}")
            raise Exception(f"Error standardizing text. More details here: {e}")

    def __call__(self, df):
        """
        Clean the given DataFrame by applying a pipeline of transformations:
        - Standardizes date formats for all specified date columns.
        - Removes rows with missing values in specified columns.
        - Removes special characters from specified text columns.
        - Standardizes text formatting (lowercasing, whitespace normalization) for text columns.

        :param df: Input DataFrame to be cleaned.
        :type df: pd.DataFrame
        :return: The cleaned and transformed DataFrame.
        :rtype: pd.DataFrame
        """

        df = self.clean_id(df)
        for col_name in self.date_columns:
            df = self.standardize_date_format(
                df=df, date_column=col_name)
        for col_name in self.drop_na_columns:
            df = self.remove_rows_missing_column_value(
                df=df, column_to_drop=col_name)
        for col_name in self.text_search_columns:
            df = self.remove_special_characters(
                df=df, column_name=col_name)
        for col_name in self.text_search_columns:
            df = self.standardize_text(
                df=df, column_name=col_name)
        return df.reset_index(drop=True)
