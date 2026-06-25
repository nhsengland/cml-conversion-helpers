import uuid
import json
import hashlib

import pandas as pd

PROCESSING_FUNC_REGISTRY = {}


def register(func):
    """A decorator used to register a processing function."""
    PROCESSING_FUNC_REGISTRY[func.__name__] = func
    return func


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
def concat_cols(df, new_col_name, cols_to_concat, prefix="", sep="|", suffix="", value_suffix=""):
    """Concatenates multiple columns into a new column, with an optional prefix, suffix, and separator.

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
    suffix : str, optional
        A suffix to append to each column name before concatenating. Defaults to "".
    value_suffix : str, optional
        A value appended (with sep) to the concatenated result. Defaults to "".

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new concatenated column added.
    """
    df = df.copy()
    actual_cols = [prefix + col + suffix for col in cols_to_concat]

    def _concat_row(row):
        values = [str(v) for v in row if pd.notna(v)]
        if value_suffix:
            values.append(value_suffix)
        return sep.join(values)

    df[new_col_name] = df[actual_cols].apply(_concat_row, axis=1)
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


@register
def add_dict_to_json_col(cell, new_values):
    """
    Add or update multiple key-value pairs in a JSON object stored as a string.

    If `cell` is null, empty, or contains invalid JSON, it is treated as an
    empty JSON object (`{}`). The function then merges `new_values` into the
    parsed JSON object and returns the result as a compact JSON string.

    Parameters
    ----------
    cell : str, None, or NaN
        A string containing a JSON object, or a null/empty value.
    new_values : dict
        A dictionary of key-value pairs to add or update in the JSON object.

    Returns
    -------
    str
        A compact JSON string with the merged key-value pairs.

    Examples
    --------
    >>> new_values = {"location_id": "booking_site", "another_thing": "another_value"}
    >>> add_dict_to_json_col(None, new_values)
    '{"location_id":"booking_site","another_thing":"another_value"}'

    >>> add_dict_to_json_col('{"data_source": "official_stats"}', new_values)
    '{"data_source":"official_stats","location_id":"booking_site","another_thing":"another_value"}'
    """
    if pd.isna(cell) or str(cell).strip() == "":
        data = {}
    else:
        try:
            data = json.loads(cell)
        except (json.JSONDecodeError, TypeError):
            data = {}

    data.update(new_values)
    return json.dumps(data, separators=(",", ":"))


@register
def add_json_key(cell, key, value):
    """
    Add or update a key-value pair in a JSON object stored as a string.

    If `cell` is null, empty, or contains invalid JSON, it is treated as an
    empty JSON object (`{}`). The function then adds or updates `key` with
    `value` and returns the result as a compact JSON string.

    Parameters
    ----------
    cell : str, None, or NaN
        A string containing a JSON object, or a null/empty value.
    key : str
        The key to add or update in the JSON object.
    value : Any
        The value to assign to `key`.

    Returns
    -------
    str
        A compact JSON string with the added or updated key-value pair.

    Examples
    --------
    >>> add_json_key(None, "location_id", "booking_site")
    '{"location_id":"booking_site"}'

    >>> add_json_key('{"data_source": "official_stats"}', "location_id", "booking_site")
    '{"data_source":"official_stats","location_id":"booking_site"}'
    """
    if pd.isna(cell) or str(cell).strip() == "":
        data = {}
    else:
        try:
            data = json.loads(cell)
        except (json.JSONDecodeError, TypeError):
            data = {}

    data[key] = value
    return json.dumps(data, separators=(",", ":"))


@register
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


@register
def create_md5_hash_col_with_exceptions(
    df,
    cols,
    new_col_name,
    ignore_prefixes=None,
):
    """Creates a new column containing the MD5 hash of the
    concatenation of specified columns.

    Values whose string representation starts with any of the
    prefixes in ignore_prefixes are excluded from the hash.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    cols : list
        The list of column names to use.
    new_col_name : str
        The name of the new MD5 hash column.
    ignore_prefixes : list[str], optional
        Value prefixes to ignore when generating the hash.
        Example: ["all_", "no_"]

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the new hash column added.
    """
    ignore_prefixes = tuple(ignore_prefixes or [])

    def _build_hash(row):
        parts = []

        for col in cols:
            value = str(row[col])

            if value.startswith(ignore_prefixes):
                continue

            parts.append(f"{col}={value}")

        hash_input = "|".join(sorted(parts)) or "__ALL__"

        return hashlib.md5(hash_input.encode()).hexdigest()

    df = df.copy()
    df[new_col_name] = df.apply(_build_hash, axis=1)

    return df
