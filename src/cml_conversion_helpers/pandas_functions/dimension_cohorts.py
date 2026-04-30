import hashlib

import pandas as pd


def create_dimension_columns(
    df,
    dimension_col_name,
    attribute_col_name,
    dimensions,
    dimensions_to_exclude
):
    """Creates a new column for each dimension, populated with the attribute value or a default "all_<dimension>" string.

    For each dimension not in `dimensions_to_exclude`, a new column is added. If a row's
    `dimension_col_name` matches the dimension, the column is set to the `attribute_col_name`
    value (or "all_<dimension>" if null). Otherwise, it defaults to "all_<dimension>".

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    dimension_col_name : str
        The column containing the dimension identifier for each row.
    attribute_col_name : str
        The column containing the attribute value for each row.
    dimensions : list
        The full list of dimension names to create columns for.
    dimensions_to_exclude : list
        Dimensions to skip — no column will be created for these.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with new dimension columns appended.
    """
    df = df.copy()
    for dimension in dimensions:
        if dimension in dimensions_to_exclude:
            continue
        matched = df[dimension_col_name] == dimension
        df[dimension] = f"all_{dimension}"
        values = df.loc[matched, attribute_col_name]
        df.loc[matched, dimension] = values.where(values.notna(), other=f"all_{dimension}")
    return df


def create_dimension_cohort_id_col(df, dimension_cols):
    """Creates a `dimension_cohort_id` column by concatenating all dimension column values with "|".

    Null values are skipped, matching the behaviour of Spark's concat_ws.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame, expected to already contain the dimension columns.
    dimension_cols : list
        The list of dimension column names to concatenate.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new `dimension_cohort_id` column added.
    """
    df = df.copy()
    df["dimension_cohort_id"] = df[dimension_cols].apply(
        lambda row: "|".join(str(v) for v in row if pd.notna(v)),
        axis=1,
    )
    return df


def create_dimension_table(df, dimension_cols, dimensions_to_exclude, dimension_col_name="Dimension", attribute_col_name="Measure"):
    """Builds a dimension table by creating per-dimension columns and a cohort ID column.

    Combines `create_dimension_columns` and `create_dimension_cohort_id_col` into a single step.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    dimension_cols : list
        The list of dimension names to create columns for and include in the cohort ID.
    dimensions_to_exclude : list
        Dimensions to skip when creating dimension columns.
    dimension_col_name : str, optional
        The column in `df` containing the dimension identifier. Defaults to "Dimension".
    attribute_col_name : str, optional
        The column in `df` containing the attribute value. Defaults to "Measure".

    Returns
    -------
    pandas.DataFrame
        The DataFrame with dimension columns and a `dimension_cohort_id` column added.
    """
    df = create_dimension_columns(
        df,
        dimension_col_name,
        attribute_col_name,
        dimension_cols,
        dimensions_to_exclude,
    )
    df = create_dimension_cohort_id_col(df, dimension_cols)
    return df


def get_dimension_list_from_col(df, dimension_col_name):
    """Retrieves the list of distinct values from a dimension column.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    dimension_col_name : str
        The name of the column from which to extract unique dimension values.

    Returns
    -------
    list
        A list of distinct values found in `dimension_col_name`.
    """
    return df[dimension_col_name].unique().tolist()


def _is_sentinel(series, col_name):
    """Returns a boolean Series that is True where the column holds a sentinel value.

    Sentinel values indicate "no filter applied": ``all_<col_name>`` or ``no_<col_name>_filter``.
    """
    return (series == f"all_{col_name}") | (series == f"no_{col_name}_filter")


def create_dimension_count_col(df, dimension_cols, new_col_name):
    """Creates a new integer column counting how many dimension columns are not set to a sentinel value.

    Sentinel values are ``all_<column_name>`` and ``no_<column_name>_filter``.
    Any other value contributes 1 to the count.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame, expected to contain all columns named in `dimension_cols`.
    dimension_cols : list
        The dimension column names to inspect.
    new_col_name : str
        The name of the new column to create.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with a new integer column added.
    """
    df = df.copy()
    count = pd.Series(0, index=df.index)
    for dim in dimension_cols:
        count = count + (~_is_sentinel(df[dim], dim)).astype(int)
    df[new_col_name] = count
    return df


def create_dimension_type_col(df, dimension_cols, new_col_name):
    """Creates a new column identifying which dimensions have specific (non-sentinel) values.

    For each row, inspects each column in `dimension_cols`. Columns whose value is not a
    sentinel (``all_<column_name>`` or ``no_<column_name>_filter``) have their name included
    in the result, concatenated with ``&``. If all columns carry a sentinel value, the result
    is ``"total"``.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame, expected to contain all columns named in `dimension_cols`.
    dimension_cols : list
        The dimension column names to inspect.
    new_col_name : str
        The name of the new column to create.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new dimension type column added.
    """
    df = df.copy()

    def _row_type(row):
        active = [
            dim for dim in dimension_cols
            if row[dim] != f"all_{dim}" and row[dim] != f"no_{dim}_filter"
        ]
        return "&".join(active) if active else "total"

    df[new_col_name] = df.apply(_row_type, axis=1)
    return df


def create_md5_hash_col(df, cols, new_col_name):
    """Creates a new column containing the MD5 hash of the concatenation of specified columns.

    Columns are joined with "|" before hashing.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    cols : list
        The list of column names whose values will be concatenated and hashed.
    new_col_name : str
        The name of the new MD5 hash column.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new MD5 hash column added.
    """
    df = df.copy()
    df[new_col_name] = df[cols].apply(
        lambda row: hashlib.md5("|".join(str(v) for v in row).encode()).hexdigest(),
        axis=1,
    )
    return df
