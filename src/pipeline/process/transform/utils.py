import pandas as pd
import logging
from typing import List

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def concatenate_dataframe_list(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate a list of Pandas DataFrames row-wise (i.e., vertically).

    :param dfs: List of DataFrames to concatenate.
    :type dfs: List[pd.DataFrame]
    :return: A single DataFrame resulting from row-wise concatenation of all valid input DataFrames.
    :rtype: pd.DataFrame
    :raises ValueError: If no valid DataFrames are provided.
    :raises Exception: If concatenation fails for any reason.
    """

    valid_dfs = []
    for i, df in enumerate(dfs):
        if isinstance(df, pd.DataFrame):
            valid_dfs.append(df)
        else:
            logging.warning(f"Item at index {i} is not a DataFrame and will be skipped: {type(df)}")

    if not valid_dfs:
        logging.error("No valid DataFrames to concatenate.")
        raise ValueError("No valid DataFrames to concatenate.")

    try:
        concat_df = pd.concat(valid_dfs, axis=0, ignore_index=True)
        logging.info(f"Concatenated {len(valid_dfs)} DataFrames into one.")
        return concat_df
    except Exception as e:
        logging.error(f"Failed to concatenate DataFrames: {e}", exc_info=True)
        raise
