# Changelog

## [0.5.0] - 2026-06-28

### Added
- `create_md5_hash_col_with_exceptions` in `pandas_functions.processing`: creates an MD5 hash column like `create_md5_hash_col`, but excludes values whose string representation starts with any prefix in a configurable `ignore_prefixes` list. Useful for generating stable cohort IDs that are unaffected by sentinel/filter values (e.g. `"all_"`, `"no_"`).
- `value_suffix` parameter to `concat_cols` in both `spark_functions.processing` and `pandas_functions.processing`: appends a fixed string (with the separator) to the end of the concatenated result.

### Changed
- `move_attributes_to_new_dimension` in the pandas module has been moved from `pandas_functions.processing` to `pandas_functions.dimension_cohorts` to match its Spark counterpart.

### Fixed
- `suffix` parameter to `concat_cols` (both Spark and pandas) was broken in a previous release; the implementation is now correct.


## [0.4.0] - 2026-06-10

### Added
- `add_json_key` for adding or updating a single key-value pair in a JSON string column in `pandas_functions.processing`.
- `add_dict_to_json_col` for merging multiple key-value pairs into a JSON string column in `pandas_functions.processing`.
- Test coverage for JSON string handling in pandas processing functions, including null, `NaN`, empty string, invalid JSON, existing JSON, and overwriting existing keys.

### Changed
- Migrated project packaging metadata from Poetry to PEP 621 `project` configuration in `pyproject.toml`.
- Switched the build backend from `poetry-core` to `setuptools.build_meta`.
- Moved development dependencies into `project.optional-dependencies.dev`.
- Added setuptools package discovery configuration for the `src/` layout.
- Updated author metadata formatting in `pyproject.toml`.

### Dependencies
- Removed Poetry-based dependency management (`poetry.lock` and Poetry configuration).
- Added explicit runtime dependencies under `project.dependencies` for `pyspark>=3.5,<4.0` and `pandas>=1.5`.
- Added development extras for `pytest>=8.0` and `pytest-html>3.1.1`.


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
