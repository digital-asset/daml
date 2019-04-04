# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

tl;dr is that for every big change, a developer add a corresponding entry to `UNRELEASED.md`.
Once a release is ready, the maintainer needs to move entries from `UNRELEASED.md` into
the section with the corresponding version number and release date in this file (`CHANGELOG.md`).

Please add unreleased changes to `UNRELEASED.md`.

## 0.3.0
### Changed
- Hide the multi-table format
- Encode `Empty` and `Unit` values as empty records in JSON

## 0.2.0
### Added
- Add option to merge identical templates from separate packages into
  the same table
- Option to strip prefix from table names
- Unify character types in PostgreSQL to `TEXT`
- Add `archived_by_transaction_id` field for contracts, drop `is_archived`

### Changed
- Transparently handle different offset formats
- Use singular table names

## 0.1.1
### Changed
- Fixed handling decimal types
- Changed `json` types to `jsonb` in PostgreSQL format

## 0.1.0
### Added
- PostgreSQL format can resume after stopping
- Respect `--keep-archived` parameter
- Added single table data shaper strategy

## 0.0.3
### Changed
- Increase maximum gRPC message size to 50MiB
- Increase maximum gRPC recursion limit to 1000

## 0.0.2
### Added
- Added dymamic package handling
- Map and validate API Client types into own types
- Rough first version of extraction to PostgreSQL
- Add a CLI argument for using secure ledger API connections

### Changed
- Insert ledger transactions as atomic SQL transactions

## 0.0.1
### Added
- Initial version with basic bare/pretty text output support
 