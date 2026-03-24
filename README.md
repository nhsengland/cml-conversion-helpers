# cml-conversion-helpers

Helper functions for converting tidy-format datasets into the NHS CML schema (metric and dimension tables).

## What this library does

CML expects data in two tables:

- **Metric table** — one row per data point, with fields such as `metric_id`, `metric_value`, `location_id`, `reporting_period_start_datetime`, etc.
- **Dimensions table** — one row per data point, one column per dimension (e.g. age group, ethnicity), linked to the metric table via `dimension_cohort_id`.

This library takes a **tidy-format** source dataset (where dimensions are stored as rows, not columns) and provides utilities to transform it into those two tables.

## Installation

```bash
pip install cml-conversion-helpers
```

## Modules

- `cml_conversion_helpers.processing` — DataFrame transformation functions

## Documentation

- [Getting Started](docs/getting-started.md) — installation, concepts, and input data requirements
- [Usage Guide](docs/usage.md) — config-driven, scripting, or hybrid approaches with full examples
- [API Reference](docs/api-reference.md) — full function reference grouped by module

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

## Acknowledgements

Some code written by [Claude](https://www.anthropic.com/claude) (Anthropic).
