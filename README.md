# cml-conversion-helpers

Helper functions for converting data to CML schema format.

## Installation

```bash
pip install cml-conversion-helpers
```

## Modules

- `cml_conversion_helpers.data_ingestion` — download and load source data
- `cml_conversion_helpers.data_exports` — save Spark DataFrames as CSV
- `cml_conversion_helpers.processing` — DataFrame transformation functions
- `cml_conversion_helpers.validation` — schema validation utilities
- `cml_conversion_helpers.utils` — Spark session, logging, and config helpers

## Development

```bash
poetry install
poetry run pytest
```
