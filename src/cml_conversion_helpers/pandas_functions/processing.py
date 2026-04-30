import uuid

import pandas as pd

PROCESSING_FUNC_REGISTRY = {}


def register(func):
    """A decorator used to register a processing function."""
    PROCESSING_FUNC_REGISTRY[func.__name__] = func
    return func


@register
def move_attributes_to_new_dimension(
    df,
    source_col_name,
    source_col_fill_value,
    new_col_name,
    new_col_fill_value,
    attributes_to_move,
):
    """Moves specified attribute values from one column into a new dimension column.

    Rows whose source column value is in `attributes_to_move` are updated: their
    source column is replaced with `source_col_fill_value` and the matched value is
    placed into `new_col_name`. All other rows get `new_col_fill_value` in `new_col_name`.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    source_col_name : str
        The name of the column from which attributes will be moved.
    source_col_fill_value : str
        The value to fill in `source_col_name` for rows whose attribute was moved.
    new_col_name : str
        The name of the new column to create for the moved attributes.
    new_col_fill_value : str
        The fill value for `new_col_name` in rows whose attribute was not moved.
    attributes_to_move : list
        The list of attribute values to move from `source_col_name` to `new_col_name`.

    Returns
    -------
    pandas.DataFrame
        The updated DataFrame with `new_col_name` added and the specified attributes moved.
    """
    df = df.copy()
    mask = df[source_col_name].isin(attributes_to_move)
    df[new_col_name] = new_col_fill_value
    df.loc[mask, new_col_name] = df.loc[mask, source_col_name]
    df.loc[mask, source_col_name] = source_col_fill_value
    return df


@register
def rename_cols(df, col_name_mappings):
    """Renames columns in a DataFrame according to a mapping dictionary.

    Columns not present in the mapping are kept with their original names.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    col_name_mappings : dict
        A dictionary mapping existing column names to their new names.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with columns renamed as specified.
    """
    return df.rename(columns=col_name_mappings)


@register
def replace_col_values(df, value_mappings, col_name):
    """Replaces values in a specified column using a mapping dictionary.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    value_mappings : dict
        A dictionary mapping old values to their replacements.
    col_name : str
        The name of the column in which values will be replaced.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the specified values replaced in `col_name`.
    """
    df = df.copy()
    df[col_name] = df[col_name].replace(value_mappings)
    return df


@register
def concat_cols(df, new_col_name, cols_to_concat, prefix="", sep="|"):
    """Concatenates multiple columns into a new column, with an optional prefix and separator.

    Null values are skipped during concatenation.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    new_col_name : str
        The name of the new concatenated column.
    cols_to_concat : list
        The list of column names to concatenate.
    prefix : str, optional
        A prefix to prepend to each column name before concatenating. Defaults to "".
    sep : str, optional
        The separator to use between values. Defaults to "|".

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new concatenated column added.
    """
    df = df.copy()
    actual_cols = [prefix + col for col in cols_to_concat]
    df[new_col_name] = df[actual_cols].apply(
        lambda row: sep.join(str(v) for v in row if pd.notna(v)),
        axis=1,
    )
    return df


@register
def create_uuid_col(df, col_name, length):
    """Adds a new column containing a truncated UUID string.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    col_name : str
        The name of the new UUID column.
    length : int
        The number of characters to keep from the generated UUID (hyphens removed).

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new UUID column added.
    """
    df = df.copy()
    df[col_name] = [uuid.uuid4().hex[:length] for _ in range(len(df))]
    return df


@register
def cast_date_col_to_timestamp(df, col_name, format="%d/%m/%Y"):
    """Casts a date string column to a datetime column using the specified format.

    Note: format uses Python's strftime conventions (e.g. ``%d/%m/%Y``), unlike the
    Spark equivalent which uses Java-style patterns (e.g. ``dd/MM/yyyy``).

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    col_name : str
        The name of the column to cast.
    format : str, optional
        The strftime format string. Defaults to "%d/%m/%Y".

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the specified column cast to datetime.
    """
    df = df.copy()
    df[col_name] = pd.to_datetime(df[col_name], format=format)
    return df


@register
def drop_cols(df, cols):
    """Drops the specified columns from a DataFrame, silently ignoring any that don't exist.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    cols : list
        The list of column names to drop.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the specified columns removed.
    """
    return df.drop(columns=[c for c in cols if c in df.columns])


@register
def add_lit_col(df, col_name, col_value):
    """Adds a new column with a constant literal value to a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    col_name : str
        The name of the new column.
    col_value : any
        The literal value to populate the new column with.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new literal column added.
    """
    df = df.copy()
    df[col_name] = col_value
    return df
