# Changelog

## [0.3.0] - 2026-05-07

### Added
- Pandas equivalents of all processing and dimension cohort functions.
- `create_dimension_count_col` and `create_dimension_type_col` functions for Spark.
- Support for `no_{col}_filter` as a sentinel value alongside `all_{col}` in dimension cohort functions.

### Changed
- Renamed `processing/` module to `spark_functions/` to distinguish from the new `pandas_functions/` module.
- Renamed `add_dimension_count_col` to `create_dimension_count_col` with added `new_col_name` parameter.

### Dependencies
- Bumped `requests` from 2.32.4 to 2.33.0.
- Bumped `pygments` from 2.19.2 to 2.20.0.
- Bumped `pytest` from 9.0.2 to 9.0.3.
- Added `pandas` as a dependency.

## [0.2.0] - 2026-03-24

Initial published release with Spark processing and dimension cohort helper functions.
