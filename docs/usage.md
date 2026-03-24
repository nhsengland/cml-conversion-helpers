# Usage Guide

There are three ways to use this library — pick whichever suits your project:

1. **Config-driven** — define your whole pipeline in `config.yaml`; your script is minimal boilerplate.
2. **Direct scripting** — import and call functions directly in Python with no config file.
3. **Hybrid** — load config for parameters but call functions explicitly for clarity or custom logic.

---

## 1. Config-driven approach

This approach is best when you want to keep your transformation logic separate from your code and make it easy to adjust parameters without editing Python files.

### config.yaml

```yaml
project_name: "my_project"
publication_date: &publication_date "01/12/2026"
last_ingest_timestamp: &last_ingest_timestamp "15/12/2026"
path_to_input_data: "data_in/source.csv"
output_dir: ""
log_dir: ""

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
import logging
from cml_conversion_helpers.utils import file_paths, logging_config, spark as spark_utils
from cml_conversion_helpers.data_ingestion import get_data, reading_data
from cml_conversion_helpers.processing import processing, dimension_cohorts
from cml_conversion_helpers.data_exports import write_csv
from cml_schemas import spark_schemas

logger = logging.getLogger(__name__)

def main():
    config = file_paths.get_config("config.yaml")
    spark = spark_utils.create_spark_session(config["project_name"])

    df = reading_data.load_csv_into_spark_data_frame(spark, config["path_to_maternity_data"])

    # Run every processing function listed in config in order
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

    # Split into metric and dimensions tables using cml_schemas
    dimensions_schema = spark_schemas.create_dimensions_schema(config["dimensions"])
    df_dimensions = spark_schemas.select_from_schema(df, dimensions_schema)
    df_metric = spark_schemas.select_from_schema(df, spark_schemas.METRIC_SCHEMA)

    write_csv.save_df_as_named_csv(df_metric, "metric")
    write_csv.save_df_as_named_csv(df_dimensions, "dimensions")

    spark.stop()
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
from cml_conversion_helpers.utils import spark as spark_utils
from cml_conversion_helpers.data_ingestion import reading_data
from cml_conversion_helpers.processing import processing, dimension_cohorts
from cml_conversion_helpers.data_exports import write_csv
from cml_schemas import spark_schemas

spark = spark_utils.create_spark_session("my_project")
df = reading_data.load_csv_into_spark_data_frame(spark, "data_in/source.csv")

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

df_metric = spark_schemas.select_from_schema(df, spark_schemas.METRIC_SCHEMA)
df_dimensions = spark_schemas.select_from_schema(df, spark_schemas.create_dimensions_schema(dimensions))

write_csv.save_df_as_named_csv(df_metric, "metric")
write_csv.save_df_as_named_csv(df_dimensions, "dimensions")

spark.stop()
```

---

## 3. Hybrid approach

A common pattern is to load config for parameters (dates, paths, dimension lists) but call the transformation functions explicitly — useful when you need conditional logic or want to mix in your own custom functions alongside the library ones:

```python
from cml_conversion_helpers.utils import file_paths, spark as spark_utils
from cml_conversion_helpers.data_ingestion import reading_data
from cml_conversion_helpers.processing import processing, dimension_cohorts
from cml_conversion_helpers.data_exports import write_csv
from cml_schemas import spark_schemas

config = file_paths.get_config("config.yaml")
spark = spark_utils.create_spark_session(config["project_name"])
df = reading_data.load_csv_into_spark_data_frame(spark, config["path_to_maternity_data"])

# Use the registry for the bulk of standard transforms...
for func_config in config["processing_funcs"]:
    func = processing.PROCESSING_FUNC_REGISTRY[func_config["name"]]
    df = func(df, **func_config["params"])

# ...then call custom logic directly
df = my_own_special_transform(df)

df = dimension_cohorts.create_dimension_table(df, config["dimensions"], dimensions_to_exclude=[])
df = processing.concat_cols(df, "metric_dimension_id", ["metric_id", "dimension_cohort_id"], sep="_")

df_metric = spark_schemas.select_from_schema(df, spark_schemas.METRIC_SCHEMA)
df_dimensions = spark_schemas.select_from_schema(df, spark_schemas.create_dimensions_schema(config["dimensions"]))

write_csv.save_df_as_named_csv(df_metric, "metric")
write_csv.save_df_as_named_csv(df_dimensions, "dimensions")
spark.stop()
```

---

## Downloading source data

If your source data is distributed as a zip file at a URL, use `get_data`:

```python
from cml_conversion_helpers.data_ingestion import get_data

path = get_data.download_zip_from_url(
    zip_file_url="https://example.com/data.zip",
    overwrite=False,       # set True to replace an existing download
    output_path="data_in"  # defaults to "data_in/<filename>" if omitted
)
```

## Schema validation

Before final export you can validate that your DataFrame matches the expected CML schema:

```python
from cml_conversion_helpers.validation import validation
from cml_schemas import spark_schemas

validation.validate_schema(df_metric, spark_schemas.METRIC_SCHEMA)
# raises TypeError with details if any columns are missing or have wrong types
```
