import os
import json
from datetime import datetime, timedelta

from src.script1 import (
    get_new_files,
    read_local_json,
    write_local_json,
    list_local_files
)


# =====================================
# TEST 1: get_new_files (core logic)
# =====================================
def test_get_new_files():
    files = [
        {"path": "file1", "last_modified": "2024-01-01T10:00:00"},
        {"path": "file2", "last_modified": "2024-01-02T10:00:00"},
    ]

    last_processed = "2024-01-01T12:00:00"
    job_start_time = "2024-01-03T00:00:00"

    result = get_new_files(files, last_processed, job_start_time)

    assert result == ["file2"]


# =====================================
# TEST 2: get_new_files edge case
# =====================================
def test_get_new_files_empty():
    result = get_new_files([], "2024-01-01T00:00:00", "2024-01-02T00:00:00")
    assert result == []


# =====================================
# TEST 3: read_local_json (file exists)
# =====================================
def test_read_local_json(tmp_path):
    file = tmp_path / "test.json"
    data = {"a": 1}

    with open(file, "w") as f:
        json.dump(data, f)

    result = read_local_json(str(file))

    assert result == data


# =====================================
# TEST 4: read_local_json (file missing)
# =====================================
def test_read_local_json_missing(tmp_path):
    file = tmp_path / "missing.json"

    result = read_local_json(str(file))

    assert result is None


# =====================================
# TEST 5: write_local_json
# =====================================
def test_write_local_json(tmp_path):
    file = tmp_path / "out" / "test.json"
    data = {"x": 10}

    write_local_json(str(file), data)

    assert os.path.exists(file)

    with open(file, "r") as f:
        saved = json.load(f)

    assert saved == data


# =====================================
# TEST 6: list_local_files
# =====================================
def test_list_local_files(tmp_path):
    file = tmp_path / "file1.txt"
    file.write_text("data")

    files = list_local_files(str(tmp_path))

    assert len(files) == 1
    assert files[0]["path"].endswith("file1.txt")