import pytest
from src.pipeline.process.transform.aggregating import DataAggregator

@pytest.fixture
def nested_dict_data():
    return [
        [{'id': '1', 'value': 'A'}, {'id': '2', 'value': 'B'}],
        [{'id': '2', 'value': 'B'}, {'id': '3', 'value': 'C'}]
    ]

@pytest.fixture
def expected_flattened():
    return [
        {'id': '1', 'value': 'A'},
        {'id': '2', 'value': 'B'},
        {'id': '2', 'value': 'B'},
        {'id': '3', 'value': 'C'}
    ]

@pytest.fixture
def expected_deduplicated():
    return [
        {'id': '1', 'value': 'A'},
        {'id': '2', 'value': 'B'},
        {'id': '3', 'value': 'C'}
    ]

# test _flatten

def test_flatten_valid(nested_dict_data, expected_flattened):
    aggregator = DataAggregator()
    result = aggregator._flatten(nested_dict_data)
    assert result == expected_flattened

def test_flatten_invalid_input_not_list():
    aggregator = DataAggregator()
    with pytest.raises(ValueError, match="Input must be a list of lists."):
        aggregator._flatten("not a list")

def test_flatten_invalid_structure():
    aggregator = DataAggregator()
    invalid_data = [{'id': '1'}]
    with pytest.raises(ValueError):
        aggregator._flatten(invalid_data)

# test _deduplicate

def test_deduplicate(expected_flattened, expected_deduplicated):
    aggregator = DataAggregator()
    aggregator.aggregated_data = expected_flattened
    result = aggregator._deduplicate()
    assert result == expected_deduplicated

# test __call__

def test_call_method(nested_dict_data, expected_deduplicated):
    aggregator = DataAggregator()
    result = aggregator(nested_dict_data)
    assert result == expected_deduplicated

def test_call_with_empty_list():
    aggregator = DataAggregator()
    result = aggregator([])
    assert result == []
