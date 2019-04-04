CHANGELOG
=========

## 1.0.9
- Fix a bug where the backend does not work against a ledger in realtime mode.

## 1.0.8
- Fix a bug where the backend did not work with multiple DAML-LF packages.

## 1.0.7
- Fix a bug in retrying to connect to the ledger if it is initially not available.

## 1.0.6
- Do not assume ledger offsets are numbers. This fixes an incompatiblity with ledger server.
- Update to ledger-api 1.0.2 and ledger-api-client 0.25.0.

## 1.0.5
- Print usage text with --help.

## 1.0.4
- Ignore non-serializable types in DAML-LF.

## 1.0.3
- Update to ledger-api 1.0.0 and DAML-LF 13.0.0

## 1.0.2
- Update after the ledger API removed the DAML-LF dependency

## 1.0.1

- Breaking: Switched to ledger API version 1. The backend is now incompatible with the
  old ledger API.
- Improve the GraphQL API. Deprecate Blocks, improve support for Events, add
  support for listing submitted commands.
- Numbers are now encoded as strings in the argument JSON.
- Stop printing passwords in the log.
- Add startup banner.
- Default time mode is now `auto`, which automatically chooses between realtime
  and static time.

## 0.6.6

- No changes, re-releasing as last version using the old ledger API

## 0.6.5

- Add support for different time modes using the `--time` command line flag.
- Add support for DAML's new builtin Unit type
- Fix application of type abstractions to types. Fixes DEL-2339

## 0.6.4

- Update scala-daml-core to 12.0.0 and daml-lf-archive to 7.0.0.

## 0.6.3

- Add support for consuming DAML-LF from the `/packages` endpoint.
- Update scala-daml-core to 6.1.0.

## 0.6.2

- Update scala-daml-core to 2.3.1. Fixes an issue where the backend does not
  serve any data.

## 0.6.1

- Update scala-daml-core to 2.3.0. Fixes an issue where the backend does not
  serve any data.

## 0.6.0

- The search criterion for template declarations was renamed from `declaration`
  to `topLevelDecl`, to be consistent with the corresponding GraphQL property.
- Add a new `template` GraphQL endpoint, returning a template by its top
  level declaration.
- Custom GraphQL endpoints can access the list of templates.

## 0.5.0

- The backend now uses the `/packages` endpoint to load template metadata.
- Added support for variant and list parameters.

## 0.4.0

- Allow adding custom routes by overriding `customRoutes` in the `UIBackend` subclass.

## 0.3.0

- Allow customising config file name by specifying `defaultConfigFile` in the
  `UIBackend` subclass.

## 0.2.1

- Updated how business intents are generated to adhere to new requirement of 32
  hexadecimal chars.

## 0.2.0

- Decimals and numbers are now encoded as numbers in the argument JSON.