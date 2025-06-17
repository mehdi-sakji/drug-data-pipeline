import pytest
import pandas as pd
from src.pipeline.process.transform.utils import concatenate_dataframe_list

@pytest.fixture
def sample_dfs():
    df1 = pd.DataFrame({'A': [1, 2], 'B': ['x', 'y']})
    df2 = pd.DataFrame({'A': [3], 'B': ['z']})
    return df1, df2

def test_concatenate_valid_dataframes(sample_dfs):
    df1, df2 = sample_dfs
    result = concatenate_dataframe_list([df1, df2])
    expected = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
    pd.testing.assert_frame_equal(result, expected)

def test_concatenate_empty_list():
    with pytest.raises(ValueError, match="No valid DataFrames"):
        concatenate_dataframe_list([])

def test_concatenate_invalid_dfs():
    with pytest.raises(ValueError, match="No valid DataFrames"):
        concatenate_dataframe_list([123, "abc", None])
