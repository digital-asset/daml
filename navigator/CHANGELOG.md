CHANGELOG
=========

## Unreleased
- Refactor Navigator type and value system to be closer aligned with the ledger API and DAML-LF.
- Improve performance when loading big DAML-LF models.
- Add support for recursive types.
- BREAKING CHANGE: Change how types and values are represented in the GraphQL API.
- Store time as ISO8601 TEXT in the datastore.

## 1.1.1
- Fix display of Date values

## 1.1.0
- First official release of Navigator Console.
- Expose SQL interface in console mode.
- Use SQLite for all contract/event/transaction/command read/write operations if the `useDatabase` flag is turned on
- BREAKING CHANGE: Logging now uses logback, and goes to `./navigator.log` by default.
- BREAKING CHANGE: CLI for Navigator Console has changed from `server --console` to simply `console`.

## 1.0.14
- Add experimental SQLite database.

## 1.0.13
- Add experimental console.

## 1.0.12
- Increase maximum gRPC message size to 50MiB. This fixes issues with large
  DAR packages.

## 1.0.11
- Fix loading of more contracts when the user scrolls to the bottom of the
  contracts table.

## 1.0.10
- Improve the performance of DAML-LF processing.
- Merge `ui-backend` and `ui-core` projects into Navigator. These libraries
  are no longer published individually.
- Use cache control headers. This fixes caching issues where the browser
  displays an old, cached version of Navigator.
- Add a CLI argument for using secure ledger API connections.

## 1.0.9
- Minor documentation changes.

## 1.0.8
- Uses version 1.0.9 of the `ui-backend` library. This fixes a bug where
  Navigator does not work if the ledger runs in realtime mode.

## 1.0.7
- Uses version 1.0.8 of the `ui-backend` library. This fixes a bug where
  Navigator does not work if the ledger uses more than one package.

## 1.0.6
- Fix caching issues, where the browser displays an old, cached version
  of Navigator.
- Uses version 1.0.7 of the `ui-backend` library. This fixes issues with
  waiting for the ledger to start.

## 1.0.5
- Uses version 1.0.6 of the `ui-backend` library. This fixes issues with
  non-numeric transaction offsets.

## 1.0.4
- Uses version 1.0.5 of the `ui-backend` library. This adds support for
  printing usage text with `--help`.

## 1.0.3
- Uses version 1.0.4 of the `ui-backend` library. This fixes issues with
  non-serializable DAML-LF types.

## 1.0.2
- Uses version 1.0.3 of the `ui-backend` library, which was updated after a
  breaking ledger API change.

## 1.0.1
- Uses version 1.0.2 of the `ui-backend` library, which was updated after a
  breaking ledger API change.

## 1.0.0

- Uses version 1.0.0 of the `ui-backend` library, which uses ledger API version 1.
- Uses version 0.22.0 of the `ui-core` library. This is required for compatibility with
  the new `ui-backend` library.
- Navigator is now incompatible with the old ledger API.
- URLEncode contract IDs in links, fixing issues if the contract IDs
  contain invalid characters.

## 0.1.16

- No changes, re-releasing as last version using the old ledger API.

## 0.1.15

- Uses version 0.21.2 of the `ui-core` library. This fixes issues
  with time inputs.

## 0.1.14

- Add support for DAML's new builtin Unit type
- Add support for different time modes using the `--time` command line flag.
- Uses version 0.21.0 of the `ui-core` library and version 0.6.5 of the
  `ui-backend` library.

## 0.1.13

- Uses version 0.20.0 of the `ui-core` library. This fixes an issue with
  flickering tables in Firefox, and tweaks the visual style of Navigator.

## 0.1.12

- Add self-test on startup, warning the user if his browser is incompatible.

## 0.1.11

- Update documentation.

## 0.1.10

- Update documentation references.

## 0.1.9

- Update documentation of customizable table views.

## 0.1.8

- Add an `about` page, displaying the Navigator version and legal notice.
- Uses version 0.6.2 of the `ui-backend` library. This adds support for
  sandbox serving DAML-LF on the `/package` endpoint.

## 0.1.7

- Uses version 0.6.2 of the `ui-backend` library. This fixes an issue where
  Navigator does not display any data if the DAML code contains `case of`
  expressions.

## 0.1.6

- Add IE11 compatibility.
- Update licensing documentation.
- Uses version 0.6.1 of the `ui-backend` library.

## 0.1.5

- Add support for complex data types.
- Fix choice button not navigating to choice.
- Uses version 0.5.0 of the `ui-backend` library.
- Uses version 0.18.0 of the `ui-core` library.

## 0.1.4

- Add configurable table views.
- Uses version 0.17.2 of the `ui-core` library.
- Uses version 0.4.0 of the `ui-backend` library.

## 0.1.3

- Uses version 0.17.0 of the `ui-core` library.
  This includes minor visual changes.

## 0.1.2

- Fixed broken ledger watcher icon.

## 0.1.1

- Restyle using corporate design.

## 0.1.0

- Re-written from scratch using ui-core components and UI framework conventions.

## 0.0.10

- BREAKING CHANGE: Change default config file name back to `navigator.conf`.

## 0.0.9

- Uses version 0.2.1 of the UI backend which changes how business intents are
  generated.

## 0.0.8

- Bundled frontend uses a dark font for transparent buttons instead of white.

## 0.0.7

- Improved time entry and display fields. This fixes issues with Time parameters.

## 0.0.6

- Parameter UI elements are now imported from the `ui-core` library. This fixes issues with Decimal parameters.

## 0.0.5

- BREAKING CHANGE: The backend now uses the UI backend library which expects the
  config file to be called `ui-backend.conf` instead of `navigator.conf`.
