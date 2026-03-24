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

### Codespaces

If you're using GitHub Codespaces, the container will set itself up automatically — just wait a few minutes for it to finish. Once it's ready, activate the Poetry environment in the terminal:

```bash
eval $(poetry env activate)
```

### Local

```bash
poetry install
poetry run pytest
```
