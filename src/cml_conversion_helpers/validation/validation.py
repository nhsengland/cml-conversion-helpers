from pyspark.sql import DataFrame


def select_from_schema(df: DataFrame, schema) -> DataFrame:
    """Selects only the columns defined in the given schema from the DataFrame.

    Parameters
    ----------
    df : DataFrame
        The input Spark DataFrame.
    schema : pyspark.sql.types.StructType
        The schema whose field names will be used to select columns.

    Returns
    -------
    DataFrame
        A DataFrame containing only the columns present in the schema.
    """
    return df.select(*[field.name for field in schema.fields])


def validate_schema(df: DataFrame, schema) -> None:
    """Validates that a DataFrame matches the expected schema.

    Checks that all fields defined in the schema exist in the DataFrame
    and have the correct data types. Raises a TypeError if any columns
    are missing or have mismatched types.

    Parameters
    ----------
    df : DataFrame
        The Spark DataFrame to validate.
    schema : pyspark.sql.types.StructType
        The expected schema to validate against.

    Raises
    ------
    TypeError
        If any columns are missing or have unexpected data types.
    """
    df_types = dict(df.dtypes)
    errors = []

    for field in schema.fields:
        col_name = field.name

        if col_name not in df_types:
            errors.append(f"  - '{col_name}': column is missing")
            continue

        actual = df_types[col_name]
        expected = field.dataType.simpleString()
        if actual != expected:
            errors.append(f"  - '{col_name}': expected {expected}, got {actual}")

    if errors:
        raise TypeError("Schema validation failed:\n" + "\n".join(errors))
