# API Reference

---

## `cml_conversion_helpers.processing.processing`

All functions in this module are decorated with `@register` and are available via `PROCESSING_FUNC_REGISTRY` for config-driven pipelines.

### `move_attributes_to_new_dimension(df, source_col_name, source_col_fill_value, new_col_name, new_col_fill_value, attributes_to_move)`

Moves specified values from one column into a new dimension column. Rows whose `source_col_name` value is in `attributes_to_move` have that value placed into `new_col_name`, and `source_col_name` is replaced with `source_col_fill_value`. All other rows get `new_col_fill_value` in `new_col_name`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `source_col_name` | `str` | Column to move values from |
| `source_col_fill_value` | `str` | Replacement value for `source_col_name` in moved rows |
| `new_col_name` | `str` | Name of the new column |
| `new_col_fill_value` | `str` | Default value for `new_col_name` in non-moved rows |
| `attributes_to_move` | `list` | Values to move |

**Returns:** `DataFrame`

```python
df = processing.move_attributes_to_new_dimension(
    df,
    source_col_name="Org_Code",
    source_col_fill_value="england",
    new_col_name="mbrrace_grouping",
    new_col_fill_value="no_mbrrace_grouping_filter",
    attributes_to_move=["Group 1. Level 3 NICU & NS", "Group 2. Level 3 NICU"]
)
```

---

### `rename_cols(df, col_name_mappings)`

Renames columns according to a mapping. Unmapped columns are left unchanged.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `col_name_mappings` | `dict` | `{old_name: new_name}` mapping |

**Returns:** `DataFrame`

```python
df = processing.rename_cols(df, {"Org_Code": "location_id", "Final_value": "metric_value"})
```

---

### `replace_col_values(df, value_mappings, col_name)`

Replaces values in a column using a mapping dictionary.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `value_mappings` | `dict` | `{old_value: new_value}` mapping |
| `col_name` | `str` | Column to apply replacements to |

**Returns:** `DataFrame`

```python
df = processing.replace_col_values(df, {"ALL": "england"}, "Org_Code")
```

---

### `concat_cols(df, new_col_name, cols_to_concat, prefix="", sep="|")`

Concatenates multiple columns into a new column.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Input DataFrame |
| `new_col_name` | `str` | — | Name of the new column |
| `cols_to_concat` | `list` | — | Columns to concatenate |
| `prefix` | `str` | `""` | Optional prefix prepended to each column name before lookup |
| `sep` | `str` | `"\|"` | Separator between values |

**Returns:** `DataFrame`

```python
df = processing.concat_cols(df, "metric_id", ["Dimension", "Count_Of"], sep="_")
```

---

### `create_uuid_col(df, col_name, length)`

Adds a column containing a truncated UUID string (hyphens removed).

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `col_name` | `str` | Name of the new UUID column |
| `length` | `int` | Number of characters to keep from the UUID |

**Returns:** `DataFrame`

```python
df = processing.create_uuid_col(df, "datapoint_id", length=32)
```

---

### `cast_date_col_to_timestamp(df, col_name, format="dd/MM/yyyy")`

Casts a string date column to a timestamp.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Input DataFrame |
| `col_name` | `str` | — | Column to cast |
| `format` | `str` | `"dd/MM/yyyy"` | Date format string |

**Returns:** `DataFrame`

```python
df = processing.cast_date_col_to_timestamp(df, "reporting_period_start_datetime")
```

---

### `drop_cols(df, cols)`

Drops specified columns from a DataFrame.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `cols` | `list` | Column names to drop |

**Returns:** `DataFrame`

```python
df = processing.drop_cols(df, ["unwanted_col_a", "unwanted_col_b"])
```

---

### `add_lit_col(df, col_name, col_value)`

Adds a new column populated with a constant value.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `col_name` | `str` | Name of the new column |
| `col_value` | `any` | Literal value (use `null` in YAML / `None` in Python for null) |

**Returns:** `DataFrame`

```python
df = processing.add_lit_col(df, "publication_date", "01/12/2026")
df = processing.add_lit_col(df, "additional_metric_values", None)
```

---

## `cml_conversion_helpers.processing.dimension_cohorts`

### `create_dimension_table(df, dimension_cols, dimensions_to_exclude, dimension_col_name="Dimension", attribute_col_name="Measure")`

Convenience function that calls `create_dimension_columns` and `create_dimension_cohort_id_col` in sequence. This is the main entry point for building the dimensions table.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Input DataFrame |
| `dimension_cols` | `list` | — | All dimension names to create columns for |
| `dimensions_to_exclude` | `list` | — | Dimensions to skip (no column created, not included in cohort ID) |
| `dimension_col_name` | `str` | `"Dimension"` | Source column holding the dimension identifier per row |
| `attribute_col_name` | `str` | `"Measure"` | Source column holding the attribute value per row |

**Returns:** `DataFrame` with one new column per dimension (minus exclusions) and a `dimension_cohort_id` column.

```python
from cml_conversion_helpers.processing import dimension_cohorts

df = dimension_cohorts.create_dimension_table(
    df,
    dimension_cols=config["dimensions"],
    dimensions_to_exclude=["mbrrace_grouping"]
)
```

---

### `create_dimension_columns(df, dimension_col_name, attribute_col_name, dimensions, dimensions_to_exclude)`

Creates one new column per dimension. Each column is set to the attribute value for rows belonging to that dimension, and `all_<dimension>` for all other rows.

Prefer `create_dimension_table` unless you need the intermediate step.

---

### `create_dimension_cohort_id_col(df, dimension_cols)`

Creates a `dimension_cohort_id` column by joining all dimension column values with `|`.

---

### `get_dimension_list_from_col(df, dimension_col_name)`

Extracts the list of distinct values from a dimension column. Useful when you want to derive the dimension list from the data rather than hard-coding it in config.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `dimension_col_name` | `str` | Column containing dimension identifiers |

**Returns:** `list`

```python
dimensions = dimension_cohorts.get_dimension_list_from_col(df, "Dimension")
```

---

### `create_md5_hash_col(df, cols, new_col_name)`

Creates a column containing the MD5 hash of the `|`-joined values of the specified columns.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `cols` | `list` | Columns whose values are concatenated and hashed |
| `new_col_name` | `str` | Name of the new hash column |

**Returns:** `DataFrame`

```python
df = dimension_cohorts.create_md5_hash_col(df, ["location_id", "metric_id"], "row_hash")
```

