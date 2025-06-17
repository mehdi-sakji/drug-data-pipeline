import pytest
import pandas as pd
from pipeline.process.transform.matching import DataMatcher

@pytest.fixture
def df_drugs():
    return pd.DataFrame({'drug': ['Aspirin', 'Ibuprofen', 'Paracetamol']})

@pytest.fixture
def df_publications():
    return pd.DataFrame({
        'title': [
            'Aspirin reduces fever',
            'Paracetamol and its effects',
            'Unrelated article',
            'Ibuprofen in treatment',
            'Ibuprofen and Aspirin combo'
        ],
        'journal': [
            'Journal of Medicine',
            'Health Weekly',
            'Science Daily',
            'Medical Reports',
            'Pharma Journal'
        ],
        'date': [
            '2020-01-01',
            '2019-12-15',
            '2021-06-30',
            '2022-03-10',
            '2023-04-22'
        ]
    })

@pytest.fixture
def matcher():
    return DataMatcher(
        drug_col_name='drug',
        pub_title_col_name='title',
        journal_col_name='journal',
        date_col_name='date',
        data_source='test_source'
    )


# find_drug_pub_matches test

def test_find_drug_pub_matches_valid(matcher, df_drugs, df_publications):
    matches = matcher.find_drug_pub_matches(df_drugs, df_publications)
    drugs_found = {m['drug'] for m in matches}
    assert 'Aspirin' in drugs_found
    assert 'Ibuprofen' in drugs_found
    assert 'Paracetamol' in drugs_found
    assert len(matches) == 5

# _format_drug_pub_matches test

def test__format_drug_pub_matches_valid(matcher):
    sample_matches = [
        {
            'drug': 'Aspirin',
            'title': 'Aspirin reduces fever',
            'journal': 'Journal A',
            'date': '2020-01-01'
        }
    ]
    formatted = matcher._format_drug_pub_matches(sample_matches)
    assert formatted[0]['drug'] == 'Aspirin'
    assert formatted[0]['title'] == 'Aspirin reduces fever'
    assert formatted[0]['ref_type'] == 'test_source_publication'
    assert formatted[0]['date_mention'] == '2020-01-01'

# format_drug_journal_matches test

def test_format_drug_journal_matches_valid(matcher):
    sample_matches = [
        {
            'drug': 'Aspirin',
            'title': 'Aspirin reduces fever',
            'journal': 'Journal A',
            'date': '2020-01-01'
        },
        {
            'drug': 'Aspirin',
            'title': 'Aspirin reduces fever',
            'journal': 'Journal A',
            'date': '2020-01-01'
        },  # duplicate journal, should only add once
        {
            'drug': 'Ibuprofen',
            'title': 'Ibuprofen study',
            'journal': 'Journal B',
            'date': '2021-01-01'
        }
    ]
    formatted = matcher.format_drug_journal_matches(sample_matches)
    # Should contain 3 entries total: 2 publications + 1 journal (since one journal duplicate)
    # The format_drug_journal_matches appends journal entries to formatted_matches after pub matches
    # The 2 publications + 2 journals (only one journal duplicate removed) total 4
    # But one of the inputs is duplicate, so journal duplicates are 1 not 2
    # Let's check distinct ref_types and counts:
    pub_count = sum(1 for m in formatted if m['ref_type'].endswith('publication'))
    journal_count = sum(1 for m in formatted if m['ref_type'] == 'journal')
    assert pub_count == 3
    assert journal_count == 2


def test___call___(matcher, df_drugs, df_publications):
    results = matcher(df_drugs, df_publications)
    assert isinstance(results, list)
    assert all('drug' in d for d in results)
    assert all('title' in d for d in results)
    assert all('ref_type' in d for d in results)
    assert all('date_mention' in d for d in results)
    # Check some known drug names
    drugs = {d['drug'] for d in results}
    assert 'Aspirin' in drugs
    assert 'Ibuprofen' in drugs
    assert 'Paracetamol' in drugs
