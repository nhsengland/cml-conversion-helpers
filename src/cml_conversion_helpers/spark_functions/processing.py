from pyspark.sql import functions as F
from pyspark import sql as pyspark

PROCESSING_FUNC_REGISTRY = {}

def register(func):
    """
    A decorator used to register a processing function.
    """
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

    Rows whose source column value is in `attributes_to_move` are split off: their
    source column is replaced with `source_col_fill_value` and the matched value is
    placed into `new_col_name`. All other rows get `new_col_fill_value` in `new_col_name`.
    The two sets are then unioned back together.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
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
    pyspark.sql.DataFrame
        The updated DataFrame with `new_col_name` added and the specified attributes moved.
    """
    columns = df.columns + [new_col_name]

    df_attributes_to_keep = df.filter(~F.col(source_col_name).isin(attributes_to_move))
    df_attributes_to_keep = (df_attributes_to_keep
        .withColumn(new_col_name, F.lit(new_col_fill_value))
        .select(*columns)
    )

    df_attributes_to_move = df.filter(F.col(source_col_name).isin(attributes_to_move))
    df_attributes_to_move = (df_attributes_to_move
        .withColumn(new_col_name, F.col(source_col_name))
        .withColumn(source_col_name, F.lit(source_col_fill_value))
        .select(*columns)
    )

    df_updated = df_attributes_to_keep.union(df_attributes_to_move)

    return df_updated


@register
def rename_cols(df, col_name_mappings):
    """Renames columns in a DataFrame according to a mapping dictionary.

    Columns not present in the mapping are kept with their original names.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    col_name_mappings : dict
        A dictionary mapping existing column names to their new names.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with columns renamed as specified.
    """
    current_cols = df.columns
    new_cols = []
    for current_col_name in current_cols:
        if current_col_name not in col_name_mappings:
            new_cols.append(current_col_name)
        else:
            new_cols.append(
                F.col(current_col_name).alias(col_name_mappings[current_col_name])
            )

    return df.select(*new_cols)


@register
def replace_col_values(df, value_mappings, col_name):
    """Replaces values in a specified column using a mapping dictionary.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    value_mappings : dict
        A dictionary mapping old values to their replacements.
    col_name : str
        The name of the column in which values will be replaced.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the specified values replaced in `col_name`.
    """
    df = df.replace(value_mappings, subset=[col_name])

    return df


@register
def concat_cols(df, new_col_name, cols_to_concat, prefix="", sep="|"):
    """Concatenates multiple columns into a new column, with an optional prefix and separator.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
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
    pyspark.sql.DataFrame
        The DataFrame with the new concatenated column added.
    """
    cols_to_concat = [prefix+col for col in cols_to_concat]

    df = df.withColumn(new_col_name, 
        F.concat_ws(sep, *cols_to_concat)
    )

    return df


@register
def create_uuid_col(df, col_name, length):
    """Adds a new column containing a truncated UUID string.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    col_name : str
        The name of the new UUID column.
    length : int
        The number of characters to keep from the generated UUID (hyphens removed).

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the new UUID column added.
    """
    df = df.withColumn(
        col_name,
        F.substring(F.regexp_replace(F.expr("uuid()"), "-", ""), 1, length)
    )
    return df


@register
def cast_date_col_to_timestamp(df, col_name, format="dd/MM/yyyy"):
    """Casts a date string column to a timestamp column using the specified format.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    col_name : str
        The name of the column to cast.
    format : str, optional
        The date format string. Defaults to "dd/MM/yyyy".

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the specified column cast to timestamp.
    """
    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), format))
    return df


@register
def drop_cols(df, cols):
    """Drops the specified columns from a DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    cols : list
        The list of column names to drop.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the specified columns removed.
    """
    return df.drop(*cols)


@register
def add_lit_col(df, col_name, col_value):
    """Adds a new column with a constant literal value to a DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    col_name : str
        The name of the new column.
    col_value : any
        The literal value to populate the new column with.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the new literal column added.
    """
    return df.withColumn(col_name, F.lit(col_value))
