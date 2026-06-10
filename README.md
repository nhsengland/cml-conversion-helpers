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

Before you build and publish, make sure you:

* Update the README.md if new instructions are needed
* Update the CHANGELOG.md (paste the git diff since the last version into your favourite bot and ask it to make you a changelog entry)
* Bump the version number (use SemVer) in pyproject.toml

When you are ready:

```bash
python -m venv venv

source venv/bin/activate # if on Linux or...
source venv/Scripts/activate # if on Windows

pip install build twine
python -m build
python -m twine upload dist/* # for PyPi or...
python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/* # for Test PyPi
```

You'll be prompted for your API token, paste it in and press enter. 

## Acknowledgements

Some code written by [Claude](https://www.anthropic.com/claude) (Anthropic).
