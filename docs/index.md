# cml-conversion-helpers Documentation

Helper functions for converting tidy-format datasets into the NHS CML schema (metric and dimension tables).

## Contents

| Doc | Description |
|-----|-------------|
| [Getting Started](getting-started.md) | Installation, concepts, and input data requirements |
| [Usage Guide](usage.md) | How to use the library — config-driven, scripting, or both |
| [API Reference](api-reference.md) | Full function reference grouped by module |

## What this library does

CML expects data in two tables:

- **Metric table** — one row per data point, with fields such as `metric_id`, `metric_value`, `location_id`, `datapoint_id`, `reporting_period_start_datetime`, etc.
- **Dimensions table** — one row per data point, with a column per dimension (e.g. age group, ethnicity) and a `dimension_cohort_id` that links back to the metric table.

This library takes a **tidy-format** source dataset (where dimensions are stored as rows, not columns) and provides utilities to transform it into those two tables.
