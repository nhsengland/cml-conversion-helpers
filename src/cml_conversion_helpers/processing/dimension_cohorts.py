from pyspark.sql import functions as F


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
    df : pyspark.sql.DataFrame
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
    pyspark.sql.DataFrame
        The DataFrame with new dimension columns appended.
    """
    def _create_new_dim_col_expr(dimension):
        return (   
            F.when(
                (F.col(dimension_col_name) == dimension)
                & 
                (F.col(attribute_col_name).isNull()), 
                F.concat(F.lit("all_"), F.lit(dimension))
             )
             .when(
                F.col(dimension_col_name) == dimension, 
                F.col(attribute_col_name)
             )
             .otherwise(F.concat(F.lit("all_"), F.lit(dimension) ))
             .alias(dimension)
        )

    new_dimension_cols = []
    for dimension in dimensions:
        if dimension in dimensions_to_exclude:
            continue
        new_dimension_cols.append(
            _create_new_dim_col_expr(dimension)
        )

    df = df.select("*", *new_dimension_cols)

    return df


def create_dimension_cohort_id_col(df, dimension_cols):
    """Creates a `dimension_cohort_id` column by concatenating all dimension column values with "|".

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame, expected to already contain the dimension columns.
    dimension_cols : list
        The list of dimension column names to concatenate.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the new `dimension_cohort_id` column added.
    """
    df = df.withColumn(
        "dimension_cohort_id",
        F.concat_ws(
            "|",
            *[F.col(col) for col in dimension_cols]
        )
    )

    return df


def create_dimension_table(df, dimension_cols, dimensions_to_exclude, dimension_col_name="Dimension", attribute_col_name="Measure"):
    """Builds a dimension table by creating per-dimension columns and a cohort ID column.

    Combines `create_dimension_columns` and `create_dimension_cohort_id_col` into a single step.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    dimension_cols : list
        The list of dimension names to create columns for and include in the cohort ID.
    dimensions_to_exclude : list
        Dimensions to skip when creating dimension columns. Typically this will be because the dimension column was created
        by another function.
    dimension_col_name : str, optional
        The column in `df` containing the dimension identifier. Defaults to "Dimension".
    attribute_col_name : str, optional
        The column in `df` containing the attribute value. Defaults to "Measure".

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with dimension columns and a `dimension_cohort_id` column added.
    """
    df = create_dimension_columns(
        df,
        dimension_col_name,
        attribute_col_name,
        dimension_cols,
        dimensions_to_exclude
    )

    df = create_dimension_cohort_id_col(
        df,
        dimension_cols
    )

    return df


def get_dimension_list_from_col(df, dimension_col_name):
    """Retrieves the list of distinct values from a dimension column.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    dimension_col_name : str
        The name of the column from which to extract unique dimension values.

    Returns
    -------
    list
        A list of distinct values found in `dimension_col_name`.
    """
    df_unique_dimensions = (df
        .select(dimension_col_name)
        .distinct()
    )

    dimension_cols = [
        row[dimension_col_name] 
        for row in df_unique_dimensions.select(dimension_col_name).collect()
    ]

    return dimension_cols


def add_dimension_count_col(df, dimension_cols):
    """Adds a `dimension_count` column counting how many dimension columns are not set to their "all_" default.

    For each column in `dimension_cols`, a value of ``all_<column_name>`` is treated as the
    "all" sentinel. Any other value contributes 1 to the count.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame, expected to contain all columns named in `dimension_cols`.
    dimension_cols : list
        The dimension column names to inspect.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with a new integer ``dimension_count`` column added.
    """
    count_expr = F.lit(0)
    for dim in dimension_cols:
        count_expr = count_expr + F.when(F.col(dim) != F.lit(f"all_{dim}"), 1).otherwise(0)

    return df.withColumn("dimension_count", count_expr)


def create_dimension_type_col(df, dimension_cols, new_col_name):
    """Creates a new column identifying which dimensions have specific (non-"all_") values.

    For each row, inspects each column in `dimension_cols`. Columns whose value is not
    ``all_<column_name>`` have their name included in the result, concatenated with ``&``.
    If all columns carry their ``all_*`` sentinel value, the result is ``"total"``.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame, expected to contain all columns named in `dimension_cols`.
    dimension_cols : list
        The dimension column names to inspect.
    new_col_name : str
        The name of the new column to create.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the new dimension type column added.
    """
    parts = [
        F.when(F.col(dim) != F.lit(f"all_{dim}"), F.lit(dim))
        for dim in dimension_cols
    ]

    concatenated = F.concat_ws("&", *parts)

    return df.withColumn(
        new_col_name,
        F.when(concatenated.isNull() | (concatenated == ""), F.lit("total")).otherwise(concatenated)
    )


def create_md5_hash_col(df, cols, new_col_name):
    """Creates a new column containing the MD5 hash of the concatenation of specified columns.

    Columns are joined with "|" before hashing.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame.
    cols : list
        The list of column names whose values will be concatenated and hashed.
    new_col_name : str
        The name of the new MD5 hash column.

    Returns
    -------
    pyspark.sql.DataFrame
        The DataFrame with the new MD5 hash column added.
    """
    df = df.withColumn(
        new_col_name,
        F.md5(F.concat_ws("|", *[F.col(c) for c in cols]))
    )
    return df
