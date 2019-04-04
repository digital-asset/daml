# Unreleased changes

All notable unreleased changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

tl;dr is that for every big change, a developer add a corresponding entry into this
file (`UNRELEASED.md`). Once a release is ready, the maintainer needs to move entries
from this file into the section with the corresponding version number and release date
in `CHANGELOG.md`.

## Unreleased

### Added
- Add runner scripts to the packaged bin folder.
- Add support for DAML-LF `Optional` type

### Changed
- Change default ledger port to 6865.
- Use `TransactionTrees` instead of transactions, and thus store exercise events as well.
- Removed `--keep-archived` options.

### Fixed

### Removed
