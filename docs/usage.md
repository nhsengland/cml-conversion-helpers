# Usage Guide

> **Scope:** this library provides DataFrame transformation functions only. Loading data, saving outputs, creating a SparkSession, managing config, and logging are all the responsibility of the parent project.

There are three ways to use this library — pick whichever suits your project:

1. **Config-driven** — define your whole pipeline in `config.yaml`; your script is minimal boilerplate.
2. **Direct scripting** — import and call functions directly in Python with no config file.
3. **Hybrid** — load config for parameters but call functions explicitly for clarity or custom logic.

---

## 1. Config-driven approach

This approach is best when you want to keep your transformation logic separate from your code and make it easy to adjust parameters without editing Python files.

### config.yaml

```yaml
publication_date: &publication_date "01/12/2026"
last_ingest_timestamp: &last_ingest_timestamp "15/12/2026"

# List every dimension that exists in the Dimension column of your source data.
# These become the dimension columns in the output dimensions table.
dimensions: &dimensions
  - EthnicCategoryMotherGroup
  - AgeAtBookingMotherGroup
  - SmokingStatusGroupBooking
  # ... add all dimensions here

# A sequence of processing functions to run in order.
# Each entry must match a name in PROCESSING_FUNC_REGISTRY.
processing_funcs:
  - name: move_attributes_to_new_dimension
    params:
      source_col_name: "Org_Code"
      source_col_fill_value: "england"
      new_col_name: "mbrrace_grouping"
      new_col_fill_value: "no_mbrrace_grouping_filter"
      attributes_to_move:
        - "Group 1. Level 3 NICU & NS"
        - "Group 2. Level 3 NICU"

  - name: replace_col_values
    params:
      col_name: "Org_Code"
      value_mappings:
        ALL: "england"

  - name: rename_cols
    params:
      col_name_mappings:
        Org_Code: "location_id"
        Org_Level: "location_type"
        Final_value: "metric_value"
        ReportingPeriodStartDate: "reporting_period_start_datetime"
        ReportingPeriodEndDate: "last_record_timestamp"

  - name: cast_date_col_to_timestamp
    params:
      col_name: reporting_period_start_datetime

  - name: cast_date_col_to_timestamp
    params:
      col_name: last_record_timestamp

  - name: create_uuid_col
    params:
      col_name: "datapoint_id"
      length: 32

  - name: concat_cols
    params:
      new_col_name: "metric_id"
      cols_to_concat: ["Dimension", "Count_Of"]
      prefix: ""
      sep: "_"

  - name: add_lit_col
    params:
      col_name: "publication_date"
      col_value: *publication_date

  - name: cast_date_col_to_timestamp
    params:
      col_name: publication_date

  - name: add_lit_col
    params:
      col_name: "last_ingest_timestamp"
      col_value: *last_ingest_timestamp

  - name: cast_date_col_to_timestamp
    params:
      col_name: last_ingest_timestamp

  - name: add_lit_col
    params:
      col_name: "additional_metric_values"
      col_value: null
```

### Python script

```python
from cml_conversion_helpers.processing import processing, dimension_cohorts

# spark, df, and config are provided by the parent project
for func_config in config["processing_funcs"]:
    func = processing.PROCESSING_FUNC_REGISTRY[func_config["name"]]
    df = func(df, **func_config["params"])

# Build dimension columns and cohort ID
df = dimension_cohorts.create_dimension_table(
    df,
    config["dimensions"],
    dimensions_to_exclude=["custom_dimension"]  # exclude if needed
)

# Create the combined metric+dimension ID
df = processing.concat_cols(df, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")
```

### How the registry works

`PROCESSING_FUNC_REGISTRY` is a plain dictionary that maps function name strings to the actual functions. Any function decorated with `@register` in `processing.py` is automatically added to it:

```python
from cml_conversion_helpers.processing.processing import PROCESSING_FUNC_REGISTRY

print(list(PROCESSING_FUNC_REGISTRY.keys()))
# ['move_attributes_to_new_dimension', 'rename_cols', 'replace_col_values',
#  'concat_cols', 'create_uuid_col', 'cast_date_col_to_timestamp',
#  'drop_cols', 'add_lit_col']
```

You can extend it with your own functions by applying the same decorator to any function in your own code:

```python
from cml_conversion_helpers.processing.processing import register, PROCESSING_FUNC_REGISTRY

@register
def my_custom_transform(df, some_param):
    # your logic here
    return df
```

---

## 2. Direct scripting approach

If you prefer to keep everything in Python without a config file, import and call functions directly:

```python
from cml_conversion_helpers.processing import processing, dimension_cohorts

# df is provided by the parent project
df = processing.move_attributes_to_new_dimension(
    df,
    source_col_name="Org_Code",
    source_col_fill_value="england",
    new_col_name="mbrrace_grouping",
    new_col_fill_value="no_mbrrace_grouping_filter",
    attributes_to_move=["Group 1. Level 3 NICU & NS", "Group 2. Level 3 NICU"]
)
df = processing.replace_col_values(df, {"ALL": "england"}, "Org_Code")
df = processing.rename_cols(df, {
    "Org_Code": "location_id",
    "Final_value": "metric_value",
    "ReportingPeriodStartDate": "reporting_period_start_datetime",
})
df = processing.cast_date_col_to_timestamp(df, "reporting_period_start_datetime")
df = processing.create_uuid_col(df, "datapoint_id", length=32)
df = processing.concat_cols(df, "metric_id", ["Dimension", "Count_Of"], sep="_")
df = processing.add_lit_col(df, "publication_date", "01/12/2026")
df = processing.cast_date_col_to_timestamp(df, "publication_date")

dimensions = ["EthnicCategoryMotherGroup", "AgeAtBookingMotherGroup"]
df = dimension_cohorts.create_dimension_table(df, dimensions, dimensions_to_exclude=[])
df = processing.concat_cols(df, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")
```

---

## 3. Hybrid approach

A common pattern is to load config for parameters (dates, dimension lists) but call the transformation functions explicitly — useful when you need conditional logic or want to mix in your own custom functions alongside the library ones:

```python
from cml_conversion_helpers.processing import processing, dimension_cohorts

# spark, df, and config are provided by the parent project

# Use the registry for the bulk of standard transforms...
for func_config in config["processing_funcs"]:
    func = processing.PROCESSING_FUNC_REGISTRY[func_config["name"]]
    df = func(df, **func_config["params"])

# ...then call custom logic directly
df = my_own_special_transform(df)

df = dimension_cohorts.create_dimension_table(df, config["dimensions"], dimensions_to_exclude=[])
df = processing.concat_cols(df, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")
```
