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


def test_move_attributes_to_new_dimension():
    df = pd.DataFrame({"existing_dim": ["1", "2", "3", "a", "b", "c"]})

    expected = pd.DataFrame({
        "existing_dim": ["1", "2", "3", "all_numbers", "all_numbers", "all_numbers"],
        "new_dim":      ["all_letters", "all_letters", "all_letters", "a", "b", "c"],
    })

    actual = dimension_cohorts.move_attributes_to_new_dimension(
        df, "existing_dim", "all_numbers", "new_dim", "all_letters", ["a", "b", "c"]
    )

    pd.testing.assert_frame_equal(
        actual.sort_values(actual.columns.tolist()).reset_index(drop=True),
        expected.sort_values(expected.columns.tolist()).reset_index(drop=True),
    )