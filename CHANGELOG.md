# Changelog

## 0.2.4

- **NOTE:** The `patch` version bump is reflective of the underlying dependency bump from
  [Target-Postgres](https://github.com/datamill-co/target-postgres) signal the
- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.2.1` to `0.2.4`.
    - Performance improvements, bug fixes, and feature support for JSONSchema `anyOf` and `allOf`
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

## 0.2.0

- **NOTE:** The `minor` version bump is not expected to have much effect on folks. This was done to signal the
  output change from the below dependency bump. It is our impression not many are using this feature yet anyways. Since
  this was _not_ a `patch` change, we decided to make this a `minor` instead of `major` change to raise _less_
  concern. Thank you for your patience! Please see the below dependency change for more information.
- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.11` to `0.2.0`.
    - Change of `STATE` message support to be better aligned with the official `singer-target-template`
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.10

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.10` to `0.1.11`.
    - No functional changes impacting `target-redshift`
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.9

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.9` to `0.1.10`.
    - Canonicalization of the `root_table` name to allow for _any_ `stream` name to be passed in
    - Additional testing to ensure safe backwards compatibility
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.8

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.8` to `0.1.9`.
    - Addressed a bug with `ACTIVATE_VERSION` not clearing internal buffers. No _known_ issues arose from this, purely a performance issue
    - Added emitting `STATE` messages when all associated `RECORD` messages have been persisted
    - Added SSL support
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.7

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.7` to `0.1.8`.
    - See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.6

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.6` to `0.1.7`.
    - Fixes a bug with denesting. See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.5

- **DEPENDENCIES:**
  - [Singer Target Postgres](https://pypi.org/project/singer-target-postgres/)
    from `0.1.4` to `0.1.6`.
    - Fixes a number of bugs. See [CHANGELOG](https://github.com/datamill-co/target-postgres/blob/master/CHANGELOG.md)
      for more details.

# 0.0.4

- **FEATURES:**
  - [Added the `persist_empty_tables`](https://github.com/datamill-co/target-postgres) config option detailed in Target-Postgres.

# 0.0.3

- **FEATURES:**
  - [Added the `default_column_length`](https://github.com/datamill-co/target-redshift/pull/9) config option which allows users to declare a default column width for `string` types
