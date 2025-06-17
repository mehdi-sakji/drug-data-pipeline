import os
import json
import pytest
from tempfile import NamedTemporaryFile
from src.pipeline.process.load import save_json

def test_save_json_success():
    data = [{"key": "value"}, {"key2": "value2"}]

    with NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        tmp_path = tmp.name
    save_json(data, tmp_path)
    with open(tmp_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    os.unlink(tmp_path)
    assert loaded == data

def test_save_json_unserializable():
    data = [{"a": {1, 2, 3}}]
    with NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        tmp_path = tmp.name
    with pytest.raises(ValueError):
        save_json(data, tmp_path)
    os.unlink(tmp_path)
