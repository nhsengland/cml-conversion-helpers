import datetime
import re
import json
import hashlib

import pandas as pd
import numpy as np
import pytest

from cml_conversion_helpers.pandas_functions import processing


def test_rename_cols():
    df = pd.DataFrame([("1", "2", "3", "4", "5")] * 4, columns=["1", "2", "3", "4", "5"])

    col_name_mappings = {"1": "1_new", "2": "2", "3": "3_new", "6": "6_new"}
    actual = processing.rename_cols(df, col_name_mappings)

    assert list(actual.columns) == ["1_new", "2", "3_new", "4", "5"]


def test_replace_col_values():
    df = pd.DataFrame({"col_1": ["1", "1", "2", "3"], "col_2": ["2", "2", "2", "2"]})
    expected = pd.DataFrame({"col_1": ["1_new", "1_new", "2_new", "3"], "col_2": ["2", "2", "2", "2"]})

    actual = processing.replace_col_values(df, {"1": "1_new", "2": "2_new"}, "col_1")

    pd.testing.assert_frame_equal(
        actual.sort_values(actual.columns.tolist()).reset_index(drop=True),
        expected.sort_values(expected.columns.tolist()).reset_index(drop=True),
    )


@pytest.mark.parametrize("test_data, mappings, col_name, expected_data", [
    ([("1", "A")],            {"1": "1_new"},   "col_1", [("1_new", "A")]),
    ([(10, "E")],             {10: 100},         "col_1", [(100, "E")]),
    ([("3", "C"), ("4", "C")], {"3": None},      "col_1", [(None, "C"), ("4", "C")]),
    ([("2", "B")],            {"1": "1_new"},   "col_1", [("2", "B")]),
    ([("1", "D")],            {},                "col_1", [("1", "D")]),
])
def test_replace_col_values_parametrized(test_data, mappings, col_name, expected_data):
    col_names = ["col_1", "col_2"]
    df = pd.DataFrame(test_data, columns=col_names)
    expected = pd.DataFrame(expected_data, columns=col_names)
    actual = processing.replace_col_values(df, mappings, col_name)

    # fillna with a sentinel so that None/NaN/pd.NA all compare equal across dtypes
    _NULL = "__NULL__"
    pd.testing.assert_frame_equal(
        actual.sort_values(col_names, na_position="first").reset_index(drop=True).fillna(_NULL),
        expected.sort_values(col_names, na_position="first").reset_index(drop=True).fillna(_NULL),
        check_dtype=False,
    )


def test_concat_cols():
    df = pd.DataFrame(
        [("1", "2", "3", "4", "5"), ("1", " ", "3", "4", "5"), ("1", "2", None, "4", "5")],
        columns=["1", "2", "3", "4", "5"],
    )
    expected_concat = ["1|2|3|4|5", "1| |3|4|5", "1|2|4|5"]

    actual = processing.concat_cols(df, "6", ["1", "2", "3", "4", "5"], "", "|")

    assert list(actual["6"]) == expected_concat


def test_concat_cols_with_prefix():
    df = pd.DataFrame(
        [("1", "2", "3", "4", "5"), ("1", " ", "3", "4", "5"), ("1", "2", None, "4", "5")],
        columns=["all_1", "all_2", "all_3", "all_4", "all_5"],
    )
    expected_concat = ["1|2|3|4|5", "1| |3|4|5", "1|2|4|5"]

    actual = processing.concat_cols(df, "6", ["1", "2", "3", "4", "5"], "all_", "|")

    assert list(actual["6"]) == expected_concat


def test_concat_cols_with_suffix():
    df = pd.DataFrame(
        [("1", "2", "3", "4", "5"), ("1", " ", "3", "4", "5"), ("1", "2", None, "4", "5")],
        columns=["1_sfx", "2_sfx", "3_sfx", "4_sfx", "5_sfx"],
    )
    expected_concat = ["1|2|3|4|5", "1| |3|4|5", "1|2|4|5"]

    actual = processing.concat_cols(df, "6", ["1", "2", "3", "4", "5"], "", "|", "_sfx")

    assert list(actual["6"]) == expected_concat


def test_concat_cols_with_prefix_and_suffix():
    df = pd.DataFrame(
        [("1", "2", "3", "4", "5"), ("1", " ", "3", "4", "5"), ("1", "2", None, "4", "5")],
        columns=["pre_1_sfx", "pre_2_sfx", "pre_3_sfx", "pre_4_sfx", "pre_5_sfx"],
    )
    expected_concat = ["1|2|3|4|5", "1| |3|4|5", "1|2|4|5"]

    actual = processing.concat_cols(df, "6", ["1", "2", "3", "4", "5"], "pre_", "|", "_sfx")

    assert list(actual["6"]) == expected_concat


def test_create_uuid_col():
    df = pd.DataFrame({"existing_col": ["a", "b", "c", "d", "e"]})
    uuid_length = 12

    actual = processing.create_uuid_col(df, "row_id", uuid_length)

    assert "row_id" in actual.columns

    ids = list(actual["row_id"])
    assert all(len(id_) == uuid_length for id_ in ids)
    assert all(re.fullmatch(r"[0-9a-f]+", id_) for id_ in ids)
    assert len(set(ids)) == len(ids)


def test_cast_date_col_to_timestamp():
    df = pd.DataFrame({"event_date": ["15/01/2024", "01/06/2000", None]})

    actual = processing.cast_date_col_to_timestamp(df, "event_date")

    assert pd.api.types.is_datetime64_any_dtype(actual["event_date"])
    assert actual.loc[0, "event_date"] == datetime.datetime(2024, 1, 15)
    assert actual.loc[1, "event_date"] == datetime.datetime(2000, 6, 1)
    assert pd.isna(actual.loc[2, "event_date"])


def test_cast_date_col_to_timestamp_custom_format():
    df = pd.DataFrame({"event_date": ["2024-01-15", "2000-06-01"]})

    actual = processing.cast_date_col_to_timestamp(df, "event_date", format="%Y-%m-%d")

    assert actual.loc[0, "event_date"] == datetime.datetime(2024, 1, 15)
    assert actual.loc[1, "event_date"] == datetime.datetime(2000, 6, 1)


def test_drop_cols():
    df = pd.DataFrame([("a", "b", "c")], columns=["col_1", "col_2", "col_3"])

    actual = processing.drop_cols(df, ["col_1", "col_3"])

    assert list(actual.columns) == ["col_2"]
    assert len(actual) == 1


def test_drop_cols_nonexistent():
    df = pd.DataFrame([("a", "b")], columns=["col_1", "col_2"])

    actual = processing.drop_cols(df, ["col_1", "col_99"])

    assert list(actual.columns) == ["col_2"]


def test_add_lit_col():
    df = pd.DataFrame([("a",), ("b",), ("c",)], columns=["existing_col"])

    actual = processing.add_lit_col(df, "publication_date", "01/01/2001")

    assert "publication_date" in actual.columns
    assert list(actual["publication_date"]) == ["01/01/2001", "01/01/2001", "01/01/2001"]


def test_add_lit_col_does_not_affect_other_columns():
    df = pd.DataFrame([("x", "y")], columns=["col_1", "col_2"])

    actual = processing.add_lit_col(df, "new_col", "val")

    assert actual.loc[0, "col_1"] == "x"
    assert actual.loc[0, "col_2"] == "y"

def test_add_json_key_with_null():
    result = processing.add_json_key(None, "location_id", "booking_site")
    assert json.loads(result) == {"location_id": "booking_site"}


def test_add_json_key_with_nan():
    result = processing.add_json_key(np.nan, "location_id", "booking_site")
    assert json.loads(result) == {"location_id": "booking_site"}


def test_add_json_key_with_empty_string():
    result = processing.add_json_key("", "location_id", "booking_site")
    assert json.loads(result) == {"location_id": "booking_site"}


def test_add_json_key_with_existing_json():
    cell = '{"data_source":"official_stats"}'
    result = processing.add_json_key(cell, "location_id", "booking_site")
    assert json.loads(result) == {
        "data_source": "official_stats",
        "location_id": "booking_site",
    }


def test_add_json_key_overwrites_existing_key():
    cell = '{"location_id":"old_value"}'
    result = processing.add_json_key(cell, "location_id", "booking_site")
    assert json.loads(result) == {"location_id": "booking_site"}


def test_add_json_key_with_invalid_json():
    result = processing.add_json_key("not valid json", "location_id", "booking_site")
    assert json.loads(result) == {"location_id": "booking_site"}


def test_add_dict_to_json_col_with_null():
    new_values = {
        "location_id": "booking_site",
        "another_thing": "another_value",
    }
    result = processing.add_dict_to_json_col(None, new_values)
    assert json.loads(result) == new_values


def test_add_dict_to_json_col_with_empty_string():
    new_values = {
        "location_id": "booking_site",
        "another_thing": "another_value",
    }
    result = processing.add_dict_to_json_col("", new_values)
    assert json.loads(result) == new_values


def test_add_dict_to_json_col_with_existing_json():
    cell = '{"data_source":"official_stats"}'
    new_values = {
        "location_id": "booking_site",
        "another_thing": "another_value",
    }
    result = processing.add_dict_to_json_col(cell, new_values)
    assert json.loads(result) == {
        "data_source": "official_stats",
        "location_id": "booking_site",
        "another_thing": "another_value",
    }


def test_add_dict_to_json_col_overwrites_existing_keys():
    cell = '{"location_id":"old_value","data_source":"official_stats"}'
    new_values = {
        "location_id": "booking_site",
        "another_thing": "another_value",
    }
    result = processing.add_dict_to_json_col(cell, new_values)
    assert json.loads(result) == {
        "location_id": "booking_site",
        "data_source": "official_stats",
        "another_thing": "another_value",
    }


def test_add_dict_to_json_col_with_invalid_json():
    new_values = {"location_id": "booking_site"}
    result = processing.add_dict_to_json_col("not valid json", new_values)
    assert json.loads(result) == {"location_id": "booking_site"}


def test_create_md5_hash_col():
    df = pd.DataFrame({
        "name": ["alice", "bob", "alice"],
        "age":  ["42",    "99",  "42"],
    })

    result = processing.create_md5_hash_col(df, ["name", "age"], "row_hash")

    assert "row_hash" in result.columns

    def expected_md5(name, age):
        return hashlib.md5(f"{name}|{age}".encode()).hexdigest()

    result = result.sort_values(["name", "age"]).reset_index(drop=True)

    assert result.loc[0, "row_hash"] == expected_md5("alice", "42")
    assert result.loc[1, "row_hash"] == expected_md5("alice", "42")
    assert result.loc[2, "row_hash"] == expected_md5("bob", "99")

    assert result.loc[0, "row_hash"] == result.loc[1, "row_hash"]


def test_hash_basic_stable_output():
    df = pd.DataFrame(
        {
            "age": ["20-24"],
            "ethnicity": ["white"],
            "smoking_status": ["non-smoker"],
        }
    )

    out = processing.create_md5_hash_col_with_exceptions(df, ["age", "ethnicity", "smoking_status"], "id")

    expected_input = "age=20-24|ethnicity=white|smoking_status=non-smoker"
    expected_hash = hashlib.md5(expected_input.encode()).hexdigest()

    assert out.loc[0, "id"] == expected_hash


def test_ignore_prefixes_removes_values():
    df = pd.DataFrame(
        {
            "age": ["20-24"],
            "ethnicity": ["all_ethnicity"],
            "smoking_status": ["all_smoking_status"],
        }
    )

    out = processing.create_md5_hash_col_with_exceptions(
        df,
        ["age", "ethnicity", "smoking_status"],
        "id",
        ignore_prefixes=["all_"],
    )

    expected_input = "age=20-24"
    expected_hash = hashlib.md5(expected_input.encode()).hexdigest()

    assert out.loc[0, "id"] == expected_hash


def test_all_values_ignored_returns_all_placeholder():
    df = pd.DataFrame(
        {
            "age": ["all_age"],
            "ethnicity": ["all_ethnicity"],
        }
    )

    out = processing.create_md5_hash_col_with_exceptions(
        df,
        ["age", "ethnicity"],
        "id",
        ignore_prefixes=["all_"],
    )

    expected_hash = hashlib.md5("__ALL__".encode()).hexdigest()

    assert out.loc[0, "id"] == expected_hash


def test_column_name_included_in_hash_input():
    df = pd.DataFrame(
        {
            "age": ["20-24"],
            "ethnicity": ["white"],
        }
    )

    out = processing.create_md5_hash_col_with_exceptions(df, ["age", "ethnicity"], "id")

    wrong_input = "20-24|white"
    wrong_hash = hashlib.md5(wrong_input.encode()).hexdigest()

    assert out.loc[0, "id"] != wrong_hash


def test_consistency_across_multiple_rows():
    df = pd.DataFrame(
        {
            "age": ["20-24", "20-24"],
            "ethnicity": ["white", "white"],
        }
    )

    out = processing.create_md5_hash_col_with_exceptions(df, ["age", "ethnicity"], "id")

    assert out.loc[0, "id"] == out.loc[1, "id"]