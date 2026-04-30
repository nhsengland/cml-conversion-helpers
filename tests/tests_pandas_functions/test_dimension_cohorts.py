import hashlib

import pandas as pd

from cml_conversion_helpers.pandas_functions import dimension_cohorts


def test_get_dimension_list_from_col():
    df = pd.DataFrame({"existing_dim": ["1", "2", "2", "A", "A", "A", "A"]})

    expected = ["1", "2", "A"]
    actual = dimension_cohorts.get_dimension_list_from_col(df, "existing_dim")

    assert sorted(expected) == sorted(actual)


def test_create_dimension_count_col():
    df = pd.DataFrame({
        "Ethnicity": ["all_Ethnicity", "no_Ethnicity_filter", "all_Ethnicity", "Asian"],
        "Age_band":  ["all_Age_band",  "all_Age_band",        "15-19",          "25-29"],
    })

    result = dimension_cohorts.create_dimension_count_col(df, ["Ethnicity", "Age_band"], "dimension_count")

    assert "dimension_count" in result.columns

    rows = {(r["Ethnicity"], r["Age_band"]): r["dimension_count"] for _, r in result.iterrows()}

    assert rows[("all_Ethnicity",       "all_Age_band")] == 0
    assert rows[("no_Ethnicity_filter", "all_Age_band")] == 0
    assert rows[("all_Ethnicity",       "15-19")]        == 1
    assert rows[("Asian",               "25-29")]        == 2


def test_create_dimension_type_col():
    df = pd.DataFrame({
        "Ethnicity": ["all_Ethnicity", "no_Ethnicity_filter", "all_Ethnicity", "Asian",  "Asian",       "no_Ethnicity_filter"],
        "Age_band":  ["all_Age_band",  "all_Age_band",        "15-19",          "25-29", "all_Age_band", "15-19"],
    })

    result = dimension_cohorts.create_dimension_type_col(df, ["Ethnicity", "Age_band"], "dimension_type")

    assert "dimension_type" in result.columns

    rows = {(r["Ethnicity"], r["Age_band"]): r["dimension_type"] for _, r in result.iterrows()}

    assert rows[("all_Ethnicity",       "all_Age_band")] == "total"
    assert rows[("no_Ethnicity_filter", "all_Age_band")] == "total"
    assert rows[("all_Ethnicity",       "15-19")]         == "Age_band"
    assert rows[("Asian",               "25-29")]         == "Ethnicity&Age_band"
    assert rows[("Asian",               "all_Age_band")]  == "Ethnicity"
    assert rows[("no_Ethnicity_filter", "15-19")]         == "Age_band"


def test_create_md5_hash_col():
    df = pd.DataFrame({
        "name": ["alice", "bob", "alice"],
        "age":  ["42",    "99",  "42"],
    })

    result = dimension_cohorts.create_md5_hash_col(df, ["name", "age"], "row_hash")

    assert "row_hash" in result.columns

    def expected_md5(name, age):
        return hashlib.md5(f"{name}|{age}".encode()).hexdigest()

    result = result.sort_values(["name", "age"]).reset_index(drop=True)

    assert result.loc[0, "row_hash"] == expected_md5("alice", "42")
    assert result.loc[1, "row_hash"] == expected_md5("alice", "42")
    assert result.loc[2, "row_hash"] == expected_md5("bob", "99")

    assert result.loc[0, "row_hash"] == result.loc[1, "row_hash"]
