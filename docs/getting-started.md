# Getting Started

## Installation

```bash
pip install cml-conversion-helpers
```

---

## Understanding the expected input format

The library is designed around converting a **tidy (long) data format** into the CML format.

| Org_Code | Org_Level | Dimension              | Attribute       | Final_value | ReportingPeriodStartDate | ReportingPeriodEndDate |
|----------|-----------|------------------------|----------------|-------------|--------------------------|------------------------|
| RXX      | Trust     | EthnicCategoryMotherGroup | EthnicWhite | 82        | 01/04/2026               | 30/06/2026             |
| RXX      | Trust     | AgeAtBookingMotherGroup   | Age25to29   | 54        | 01/04/2026               | 30/06/2026             |
| ALL      | England   | EthnicCategoryMotherGroup | EthnicWhite | 79        | 01/04/2026               | 30/06/2026             |

In this example the `Dimension` column identifies *which* dimension the row belongs to, and the attribute value (e.g. `EthnicWhite`) sits in the `Count_Of` column.

---

## Understanding the output format

After transformation you will have two PySpark DataFrames ready to export:

### Metric table

One row per data point, containing the numeric value and metadata:

| datapoint_id | metric_id | metric_dimension_id | location_id | location_type | metric_value | reporting_period_start_datetime | last_record_timestamp | publication_date | last_ingest_timestamp | additional_metric_values |
|---|---|---|---|---|---|---|---|---|---|---|

### Dimensions table

One row per data point, one column per dimension. All dimension columns default to `all_<dimension>` unless the data point belongs to that dimension:

| datapoint_id | metric_dimension_id | dimension_cohort_id | EthnicCategoryMotherGroup | AgeAtBookingMotherGroup | ... |
|---|---|---|---|---|---|

The `dimension_cohort_id` is a `|`-separated concatenation of all dimension column values and links the two tables together.

---

## Project layout (recommended)

```
my_project/
├── config.yaml           # pipeline configuration
├── create_cml_tables.py  # main script
├── data_in/              # source CSVs
└── data_out/             # output CSVs written here
```
