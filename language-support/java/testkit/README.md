# Testkit

This is a testkit that can test all the bindings. This md file contains the test
cases the bindings have to fullfil. This can later be translated to e.g. Cucumber,
and implement the steps in different languages, e.g. Scala for the JVM lanugages,
and Python for the Python binding. The feature files could be reused for all the
bindings we will have, with possibly a new implementation of the steps.

The test cases are constructed in a way, that there is no need to run a Sandbox
for the bindings. The tests can control what is sent to the bindings from the test
itself, and from the ledger as well. This results in faster testing, and easier
debugging. However it is still beneficial to have a basic scenario running with
Sandbox, as an integration test.

## Test cases

### 1. Active Contracts Service

```proto
service ActiveContractsService {
  rpc GetActiveContracts (GetActiveContractsRequest) returns (stream GetActiveContractsResponse);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 1.1  | All      | When ACS is requested, then all the events provided by the ledger is returned | &check; |
| 1.2  | All      | Transaction filter and verbose flag are passed to the ledger | &check; |
| 1.3  | All      | ACS is requested with the correct ledger ID                | &check; |
| 1.4  | Reactive | The returned stream handles backpressure                   |         |

### 2. Command Service

```proto
service CommandService {
  rpc SubmitAndWait (SubmitAndWaitRequest) returns (google.protobuf.Empty);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 2.1  | All      | When a command is sent to the bindings, it is sent to the ledger | &check; |
| 2.2  | All      | Command is sent to the correct ledger ID                   | &check; |

### 3. Command Submission Service

```proto
service CommandSubmissionService {
  rpc Submit (SubmitRequest) returns (google.protobuf.Empty);
}
```

| ID   | Scope    | Rule                                                       | Java   |
|------|----------|------------------------------------------------------------|---------|
| 3.1  | All      | When a command is sent to the binding, it is sent to the ledger | &check; |
| 3.2  | All      | Command is sent to the correct ledger ID                   |         |

### 4. Command Completion Service

```proto
service CommandCompletionService {
  rpc CompletionStream (CompletionStreamRequest) returns (stream CompletionStreamResponse);
  rpc CompletionEnd    (CompletionEndRequest)    returns (CompletionEndResponse);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 4.1  | All      | When the completion end is requested, it is provided from the ledger | &check; |
| 4.2  | All      | The completion end is requested with the correct ledger ID | &check; |
| 4.3  | All      | The completion stream response arrives as new command completes | &check; |
| 4.4  | All      | The completion stream is requested with the correct ledger ID, start offset, application ID and requested parties are provided with the request | &check; |
| 4.5  | Reactive | The completion stream handles backpressure                 |         |

### 5. Ledger Configuration Service

```proto
service LedgerConfigurationService {
  rpc GetLedgerConfiguration (GetLedgerConfigurationRequest) returns (stream GetLedgerConfigurationResponse);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 5.1  | All      | When the configuration is requested, then the configuration is returned from the ledger | &check; |
| 5.2  | All      | The configuration is requested with the correct ledger ID  | &check; |

### 6. Ledger Identity Service

```proto
service LedgerIdentityService {
  rpc GetLedgerIdentity (GetLedgerIdentityRequest) returns (GetLedgerIdentityResponse);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 6.1  | All      | When the ledger ID is requested, it is returned from the ledger | &check; |

### 7. Package Service

```proto
service PackageService {
  rpc ListPackages     (ListPackagesRequest)     returns (ListPackagesResponse);
  rpc GetPackage       (GetPackageRequest)       returns (GetPackageResponse);
  rpc GetPackageStatus (GetPackageStatusRequest) returns (GetPackageStatusResponse);
}
``` 

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 7.1  | All      | When supported packages are requested, it returns them from the ledger | &check; |
| 7.2  | All      | The supported packages are requested with the correct ledger ID | &check; |
| 7.3  | All      | When a package is requested, it is returned from the ledger | &check; |
| 7.4  | All      | The package is requested with the correct ledger ID        | &check; |
| 7.5  | All      | When the status of a package is requested, it is returned from the ledger | &check; |
| 7.6  | All      | The status is requested with the correct ledger ID         | &check; |

### 8. Transaction Service

```proto
service TransactionService {
  rpc GetTransactions         (GetTransactionsRequest)         returns (stream GetTransactionsResponse);
  rpc GetTransactionTrees     (GetTransactionsRequest)         returns (stream GetTransactionTreesResponse);
  rpc GetTransactionByEventId (GetTransactionByEventIdRequest) returns (GetTransactionResponse);
  rpc GetTransactionById      (GetTransactionByIdRequest)      returns (GetTransactionResponse);
  rpc GetLedgerEnd            (GetLedgerEndRequest)            returns (GetLedgerEndResponse);
}
```

| ID   | Scope    | Rule                                                       |  Java   |
|------|----------|------------------------------------------------------------|---------|
| 8.1  | All      | When transactions are requested, transactions are returned from the ledger | &check; |
| 8.2  | All      | Start offset, end offset transaction filter and verbose flag are passed with the transactions request to the ledger | &check; |
| 8.3  | All      | Transaction stream is requested with the correct ledger ID | &check; |
| 8.4  | Reactive | Transaction stream handles backpressure                    |         |
| 8.5  | All      | When transaction trees are requested, transaction trees are returned from the ledger | &check; |
| 8.6  | All      | Start offset, end offset transaction filter and verbose flag are passed with the transaction trees request to the ledger | &check; |
| 8.7  | All      | Transaction tree stream is requested with the correct ledger ID | &check; |
| 8.8  | Reactive | Transaction tree stream handles backpressure               |         |
| 8.9  | All      | Transactions can be looked up by a contained event ID      | &check; |
| 8.10 | All      | The requesting parties are passed with the transaction lookup by event ID | &check; |
| 8.11 | All      | The transaction lookup by event ID happens with the correct ledger ID | &check; |
| 8.12 | All      | Transactions can be looked up by transaction ID            | &check; |
| 8.13 | All      | The requesting parties are passed with the transaction lookup by transaction ID | &check; |
| 8.14 | All      | The transaction lookup by transaction ID happens with the correct ledger ID | &check; |
| 8.15 | All      | When the ledger end is requested, it is provided from the ledger | &check; |
| 8.16 | All      | The ledger end is requested with the correct ledger ID     | &check; |

### 9. Time Service

```
service TimeService {
  rpc GetTime (GetTimeRequest) returns (stream GetTimeResponse);
  rpc SetTime (SetTimeRequest) returns (google.protobuf.Empty);
}
```

| ID   | Scope | Rule                                                       |  Java   |
|------|-------|------------------------------------------------------------|---------|
| 9.1  | All   | The correct ledger ID, current and new time of the set time request are passed to the ledger | &check; |
| 9.2  | All   | When ledger times are requested, ledger times are returned from the ledger  | &check; |
| 9.3  | All   | Ledger times stream is requested with the correct ledger ID | &check; |
| 9.4  | All   | When ledger time is set, an error is returned if current time >= new time | &check; |