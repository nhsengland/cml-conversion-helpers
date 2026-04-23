import pytest
import hashlib

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from cml_conversion_helpers.processing import dimension_cohorts
    

def test_get_dimension_list_from_col(spark):
    """
    Tests get_dimension_list_from_col
    """

    test_data = [
        ('1',),
        ('2',),
        ('2',),
        ('A',),
        ('A',),
        ('A',),
        ('A',),
    ]
    test_cols = ['existing_dim']
    df_test = spark.createDataFrame(test_data, test_cols)

    expected = ['1', '2', 'A']

    actual = dimension_cohorts.get_dimension_list_from_col(
        df_test,
        'existing_dim',
    )
    expected.sort()
    actual.sort()
    assert expected == actual


def test_create_dimension_count_col(spark):
    """
    Tests create_dimension_count_col counts the number of non-"all_" dimension values per row.
    """
    test_data = [
        ('all_Ethnicity', 'all_Age_band'),  # both all → 0
        ('all_Ethnicity', '15-19'),          # one non-all → 1
        ('Asian', '25-29'),                  # both non-all → 2
    ]
    test_cols = ['Ethnicity', 'Age_band']
    df_test = spark.createDataFrame(test_data, test_cols)

    result = dimension_cohorts.create_dimension_count_col(df_test, ['Ethnicity', 'Age_band'], 'dimension_count')

    assert 'dimension_count' in result.columns

    rows = {(r['Ethnicity'], r['Age_band']): r['dimension_count'] for r in result.collect()}

    assert rows[('all_Ethnicity', 'all_Age_band')] == 0
    assert rows[('all_Ethnicity', '15-19')] == 1
    assert rows[('Asian', '25-29')] == 2


def test_create_dimension_type_col(spark):
    test_data = [
        ('all_Ethnicity', 'all_Age_band'),
        ('all_Ethnicity', '15-19'),
        ('Asian', '25-29'),
        ('Asian', 'all_Age_band'),
    ]
    test_cols = ['Ethnicity', 'Age_band']
    df_test = spark.createDataFrame(test_data, test_cols)

    result = dimension_cohorts.create_dimension_type_col(df_test, ['Ethnicity', 'Age_band'], 'dimension_type')

    assert 'dimension_type' in result.columns

    rows = {(r['Ethnicity'], r['Age_band']): r['dimension_type'] for r in result.collect()}

    assert rows[('all_Ethnicity', 'all_Age_band')] == 'total'
    assert rows[('all_Ethnicity', '15-19')] == 'Age_band'
    assert rows[('Asian', '25-29')] == 'Ethnicity&Age_band'
    assert rows[('Asian', 'all_Age_band')] == 'Ethnicity'


def test_create_md5_hash_col(spark):
    """
    Tests create_md5_hash_col produces a consistent MD5 hash from multiple columns.
    """
    

    test_data = [
        ('alice', '42'),
        ('bob', '99'),
        ('alice', '42'),
    ]
    test_cols = ['name', 'age']
    df_test = spark.createDataFrame(test_data, test_cols)

    result = dimension_cohorts.create_md5_hash_col(df_test, ['name', 'age'], 'row_hash')

    assert 'row_hash' in result.columns

    rows = result.orderBy('name', 'age').collect()

    # Verify the hash matches what Python's hashlib produces for the same concat_ws logic
    def expected_md5(name, age):
        return hashlib.md5(f"{name}|{age}".encode()).hexdigest()

    assert rows[0]['row_hash'] == expected_md5('alice', '42')
    assert rows[1]['row_hash'] == expected_md5('alice', '42')
    assert rows[2]['row_hash'] == expected_md5('bob', '99')

    # Same inputs should produce the same hash
    assert rows[0]['row_hash'] == rows[1]['row_hash']
