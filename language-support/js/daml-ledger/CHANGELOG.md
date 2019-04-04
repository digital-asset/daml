# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

Summarizing, for every big change the contributor adds a corresponding entry to [`UNRELEASED.md`](UNRELEASED.md).
Once a release is ready, the maintainer needs to move entries from [`UNRELEASED.md`](UNRELEASED.md) into
the section with the corresponding version number and release date in this file (`CHANGELOG.md`).

Please add unreleased changes to `UNRELEASED.md`.

## 0.4.0

### Changed

- support Ledger API 1.4.0

## 0.3.0

### Added

- support for non-verbose mode
- perform client-side validation on requests

## 0.2.0

### Added

- DamlLedgerClient support for secure connections over SSL

## 0.1.1

### Fixed

- Package publishing to include relevant files

## 0.1.0

### Added

- LedgerClient interface and implementation
- First CommandClient implementation
- First CommandCompletionClient implementation
- First LedgerConfigurationClient implementation
- First TimeClient Implementation
- Protobuf/JSON mapping and interfaces for all services

### Changed

- TransactionClient now uses plain objects

### Fixed

- ClientReadableObjectStream behaves consistently
- ClientReadableObjectStream doesn't stall on the first item

## 0.0.3

### Added

- LedgerClient interface and implementation
- First CommandClient implementation
- First CommandCompletionClient implementation
- First LedgerConfigurationClient implementation
- First TimeClient Implementation
- Protobuf/JSON mapping and interfaces for all services

### Changed

- TransactionClient now uses plain objects

### Fixed

- ClientReadableObjectStream behaves consistently


## 0.0.2

### Added

- implement Active Contracts Client
- implement Package Client
- implement Ledger Identity Client
