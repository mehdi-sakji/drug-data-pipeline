import pytest
import pandas as pd
from src.pipeline.process.transform.cleaning import DataCleaner

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'id': [1, 2, 3],
        'date': ['2023-01-01', '01/02/2023', '27 April 2020'],
        'text_col': ['Hello, World!', 'Test@123', 'Foo    Bar'],
        'drop_col': ['keep', None, 'keep']
    })

@pytest.fixture
def cleaner():
    return DataCleaner(
        date_columns=['date'],
        drop_na_columns=['drop_col'],
        text_search_columns=['text_col'],
        id_column='id',
        id_prefix='prefix'
    )

def test_clean_id(cleaner, sample_df):
    df = cleaner.clean_id(sample_df.copy())
    assert df['id'].iloc[0] == 'prefix_1'
    assert df['id'].iloc[1] == 'prefix_2'
    assert df['id'].iloc[2] == 'prefix_3'

def test_standardize_date_format(cleaner, sample_df):
    df = cleaner.standardize_date_format(sample_df.copy(), 'date')
    assert df['date'].iloc[0] == '2023-01-01'
    assert df['date'].iloc[1] == '2023-02-01'
    assert df['date'].iloc[2] == '2020-04-27'

def test_remove_rows_missing_column_value(cleaner, sample_df):
    df = cleaner.remove_rows_missing_column_value(sample_df.copy(), 'drop_col')
    assert len(df) == 2
    assert df['drop_col'].isnull().sum() == 0

def test_remove_special_characters(cleaner, sample_df):
    df = cleaner.remove_special_characters(sample_df.copy(), 'text_col')
    assert df['text_col'].iloc[0] == 'Hello World'
    assert df['text_col'].iloc[1] == 'Test123'

def test_standardize_text(cleaner, sample_df):
    df = cleaner.standardize_text(sample_df.copy(), 'text_col')
    assert df['text_col'].iloc[2] == 'foo bar'

def test_pipeline_call(cleaner, sample_df):
    df = cleaner(sample_df.copy())
    assert df.shape[0] == 2  # row with None in drop_col should be removed
    assert df['text_col'].iloc[0] == 'hello world'
    assert df['text_col'].iloc[1] == 'foo bar'
    assert df['id'].str.startswith('prefix_').all()
