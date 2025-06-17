import os
import pytest
from src.adhoc.main import get_journal_with_most_drug_mentions
from tests.data.adhoc import TEST_ADHOC_DATA_DIR

def test_get_journal_with_most_drug_mentions_valid():
    result = get_journal_with_most_drug_mentions(os.path.join(TEST_ADHOC_DATA_DIR, "matches.json"))
    expected = {'journal': 'psychopharmacology', 'mentions': {'ethanol', 'tetracycline'}}
    assert(expected == result)


def test_get_journal_with_most_drug_mentions_missing():
    with pytest.raises(FileNotFoundError):
        get_journal_with_most_drug_mentions(os.path.join(TEST_ADHOC_DATA_DIR, "missing_matches.json"))
