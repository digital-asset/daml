# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

tl;dr is that for every big change, a developer add a corresponding entry to `UNRELEASED.md`.
Once a release is ready, the maintainer needs to move entries from `UNRELEASED.md` into
the section with the corresponding version number and release date in this file (`CHANGELOG.md`).

Please add unreleased changes to `UNRELEASED.md`.

## 1.5.0
### Added
-`Optional` type.

## 1.4.0
### Added
- gRPC definitions are now distributed on NPM too

### Changed
- removed various API limitations, we have full support

## 1.3.0
### Added
- Added fields `module_name` and `entity_name` and deprecated field `name` in message `Identifier`.
With the new fields, applications can now discern the module part of the identifier from the entity part.
This wasn't possible before. 

## 1.2.0
### Changed
- [leder-api] changed TransactionTree to have a flat representation.

## 1.1.0

### Fixed
 - setting correct state in DownstreamEventBuffer to avoid repeating buffered cancellations.

### Added
 - Added reset service (see DEL-5418)

### Removed
 - Deleted rs-grpc-bridge-scala and all related code.

## 1.0.3
### Fixed
- [ledger-api] fixed documententation on transaction offsets stating they are integers
- [ledger-api] added documentation for unsupported features
- [rs-grpc-bridge] fixed ClientPublisher to handle demand of Long.MAX_VALUE, as required by the reactive streams specification

### Removed
- [ledger-api] removed unimplemented OrderOffsetRequest and Response messages

## 1.0.2
### Added
- Support for multiple Subscribers in ClientPublisher. Each subscription will result in an independent RPC call.

### Changed
- [reactive-layer-java, data-layer-java] move under bindings
- [reactive-layer-java, data-layer-java] rename package base to com.daml.ledger.javaapi
- [reactive-layer-java, data-layer-java] set organization to com.daml.ledger

### Fixed
- Updated to daml-lf 13.1.0 for the java examples
- grpc-definitions doesn't use the scala version suffix anymore
- bindings-java and bindings-scala don't have a dependency on grpc-definitions anymore.
- [reactive-layer-java] publish javadocs
- Updated Python Binding to support changes in grpc definition of ArchivePayload; fixed commands submission for Party fields.   


## 1.0.0
#### Changed
- Streamlined `GetActiveContractsResponse`
- inlined `WitnessedCreatedEvents`
- inlined witness_parties in Events

## 0.0.57
### Changed
- reverted change allowing multiple materialisations of an Akka Source created from a ClientPublisher

## 0.0.56
### Changed
- Simplified Transaction related messages
- Inlined Archive fields in GetPackageResponse

### Fixed
- Publish bindings-java without the scala version suffix in the artifact name.
- Retain the directory structure for the protobuf reference documentation.

## 0.0.55
### Changed
- Remove scala version suffix from bindings_java
- publish rs-grpc-bridge-scala

## 0.0.54
### Changed
- Replaced ClientPublisher and ServerSubscriber in the gRPC bridge
- rs-grpc-bridge became Java-only, Scala extensions are in rs-grpc-bridge-scala, and rs-grpc-akka depends on rs-grpc-bridge-scala.

## 0.0.53
### Changed
- Active Contract Service returns only Created events
- Removing DAML-LF dependency in packages service

## 0.0.52
### Changed
- Simpler implementation for ServerPublisher in gRPC bridge
- Cleaned up TransactionFilter and nested messages. Use empty FilterMessage for parties that would receive all events, or spell out the template Ids in FiltersInclusive to filter by template.

## 0.0.51
### Fixed
- Fix deadlock in rs grpc bridge

## 0.0.50
### Fixed
- Fix error handling in rs grpc bridge to be consistent

## 0.0.49
### Added
- Add conversions between Instant and microseconds, since Ledger API timestamps are now microsecond-based.
### Changed
- Bump version of daml-lf-archive dependency to 10.1.0

## 0.0.48
### Changed
- Bumped DAML-LF Archive version
- simplified SuiteResourceManagedment traits

## 0.0.47
### Changed
- Revise primitive types according to DAML-LF spec, and recent changes to the DAML-LF proto definition

## 0.0.46
### Changed
- Use daml lf archive 7.2.1

## 0.0.45
### Added
- Release .zip file with .proto definitions, for release in the SDK

## 0.0.44
### Changed
- CommandSubmissionService.submit returns Empty

## 0.0.43
### Removed
- client and server modules

## 0.0.42
### Removed
- prototype-server

## 0.0.41
### Changed
- TraceContext.parent_span_id is now optional

## 0.0.40
### Changed
- Dispatcher is now async and storage-agnostic

## 0.0.39
### Added
- Command Client documentation
### Changed
- added parties to CompletionStreamRequest

## 0.0.38
### Changed
- changed SynchronousFailingCommandsIT to work with Sandbox as well

## 0.0.37
### Added
- changed the error semantics of SubmitAndWait

## 0.0.36
### Added
- proto files in the grpc-definitions module are now packaged in the published jar file

## 0.0.35
### Changed
- Fix test, add backpressure to ReferenceCommandService

## 0.0.34
### Changed
- ReferenceCommandService supports in-memory communication

## 0.0.33
### Changed
- Inject Channel into ReferenceCommandService.

## 0.0.32
### Changed
- Relaxed type contraints in PerformanceTest trait to allow memory footprint testing

## 0.0.31
### Fixed
- fixed a bug in rs-grpc-bridge when sending one extra element in case of server streaming

## 0.0.30
### Changed
- ExercisedEvent reflects that choices will be exercisable by multiple parties
  together from DAML code in the future.

## 0.0.29
### Changed
- [prototype-client]: StaticTime implements TimeProvider

## 0.0.28
### Changed
- refactored test utils around SuiteResourceManagement

## 0.0.27
### Changed
- turned choice argument into a Value rather than a Record in Events as well

## 0.0.26
### Removed
- Execute Commands
### Changed
- Command Tracking outputs Completions

## 0.0.25
### Changed
- turned choice argument into a Value rather than a Record

## 0.0.24
### Changed
- ActiveContractsService uses String offsets
- Dispatcher returns the new Offset when publishing
- minor doc fixes

### Removed
- removed ordering functionality from TransactionService

## 0.0.23
### Changed
- refactored transaction responses to share messages with active contracts service
- added offsets to transaction responses

## 0.0.22
### Added
- OrderOffsets RPC in Transaction Service
### Changed
- Use strings for variant constructors, to make interop with old legacy core possible and to ease debugging.

## 0.0.21
### Changed
- Added required requesting_parties field to transaction requests.

## 0.0.20
### Added
- Implement ledger-end in transaction client

### Fixed
- Added the `perf-testing` dependency in `rs-grpc-akka` to the test scope, so that `perf-testing` (along with transitive dependencies) doesn't show up in  the compile scope of projects depending on `rs-grpc-akka` (either directly or transitively)

## 0.0.19
### Changed
- Add unit and fromPair to Ctx

## 0.0.17
### Added
### Changed
- renamed mapping to selectors in transaction filters
- bumped DAML-LF Archive version to 7.0.0

## 0.0.16
### Added
- reusable time service tests
### Changed
- package service now serves daml-lf payloads

## 0.0.15
### Added
- reference time service

## 0.0.14
### Added
- Return NOT_FOUND error on ledger ID mismatch instead of INVALID_ARGUMENT
- Reusable command integration tests

## 0.0.13
### Added
- Reusable integration tests for Ledger Identity and Ledger Configuration services
- Simpler initialization method for prototype-client with host and port

## 0.0.9
### Added
- Add `testing-utils` package.

## 0.0.8
### Changed
- Changed Disptacher to return materialized value when subscribing

## 0.0.7
### Changed
- Upgraded ScalaPB

## 0.0.6
### Added
- Introduced the Dispatcher to serve as a basis for in memory ledger

## 0.0.4
### Changed
 - Moved proto messages from `common` to `v1` package.
 - Renamed `TransactionService` rpcs back to `GetTransactions` and `GetTransactionTrees`

## 0.0.3

### Added
* Added Package, Ledger, Status, Transaction services for v1 API.
* Initial RS-gRPC bridge
* Added trace logging to RS-gRPC bridge
* Added DAML LF Serializable types
* Added simple command submission payloads
* Introducing BatchConfiguration where applicable
* Added scratch app to demonstrate SSL usage and certificate access with gRPC
* Initial example client

### Changed
* Updated v1 spec based on meetings of 2018-02-20
* Split up build and made compilation rules stricter.
* Updated v1 spec based on meetings of 2018-02-21
* Using Google's suggested proto styling
* Changed arguments to have a more compact format.
* Moved workflow_id to batch level from composite command.
### Fixed
### Removed
