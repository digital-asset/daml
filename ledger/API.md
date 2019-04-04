# Platform API

## Introduction

The ledger API is defined as a set of gRPC services which can be found [here](https://github.com/DACH-NY/da/tree/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1).

There are two new ledger service processes which are introduced (referred to as the application services layer).

* Active Contract Service - Ledger Active Contract Service
* Command Service - Provides added functionality by combining the submission and completion.

These new services will be running as independent applications and will be packaged separately as well.

The other gRPC services are implemented in place with the existing participant application.

## Service Details

## Application Service Layer

### Active Contracts Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/active_contracts_service.proto)

```com.digitalasset.ledger.api.v1.ActiveContractsService```
```javascript
{
  "name": "ActiveContractsService",
  "method": [
    {
      "name": "GetActiveContracts",
      "inputType": ".com.digitalasset.ledger.api.v1.GetActiveContractsRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetActiveContractsResponse",
      "serverStreaming": true
    }
  ]
}
```

#### Artifact

This is published as a docker image  ``` digitalasset/platform-acs ``` from the following
```yaml
groupId: com.digitalasset.ledger
artifactId: ledger-acs
ext: tgz
```

### Command Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/commands.proto)

```com.digitalasset.ledger.api.v1.CommandService```
```javascript
{
  "name": "CommandService",
  "method": [
    {
      "name": "SubmitAndWait",
      "inputType": ".com.digitalasset.ledger.api.v1.SubmitAndWaitRequest",
      "outputType": ".google.protobuf.Empty"
    }
  ]
}
```

This is published as a docker image  ``` digitalasset/platform-commands ``` from the following
```yaml
groupId: com.digitalasset.ledger
artifactId: ledger-commands
ext: tgz
```

### Configuration

The services in the application services layer have the following common configuration

> Http Port: ```8081```

> Grpc Port: ```28182```

> Health status endpoint: ```/admin/status```

> Readiness status endpoint: ```/admin/ready```


## Ledger Service Layer

The services in this layer are currently running in the participant application in various location/roles

This is published as a docker image  ``` digitalasset/platform-participant ``` from the following
```yaml
groupId: com.digitalasset.ledger
artifactId: participant
ext: jar
```

#### Configuration

The ledger services  has the following common configuration

> Http Port: ```8081```

> Grpc Port: ```28182```

> Health status endpoint: ```/admin/status```

### Command Completion Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/command_completion_service.proto)

```com.digitalasset.ledger.api.v1.CommandCompletionService```
> Participant Modules: ```participant.pipeline```
```javascript
{
  "name": "CommandCompletionService",
  "method": [
    {
      "name": "CompletionStream",
      "inputType": ".com.digitalasset.ledger.api.v1.CompletionStreamRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.CompletionStreamResponse",
      "serverStreaming": true
    },
    {
      "name": "CompletionEnd",
      "inputType": ".com.digitalasset.ledger.api.v1.CompletionEndRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.CompletionEndResponse"
    }
  ]
}
```

### Command Submission Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/command_submission_service.proto)

```com.digitalasset.ledger.api.v1.CommandSubmissionService```
> Participant Modules: ```participant.writer```
```javascript
{
  "name": "CommandSubmissionService",
  "method": [
    {
      "name": "Submit",
      "inputType": ".com.digitalasset.ledger.api.v1.SubmitRequest",
      "outputType": ".google.protobuf.Empty"
    }
  ]
}
```

### Ledger Configuration Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/ledger_configuration_service.proto)

```com.digitalasset.ledger.api.v1.LedgerConfigurationService```
> Participant Modules: ```participant.pipeline```
```javascript
{
  "name": "LedgerConfigurationService",
  "method": [
    {
      "name": "GetLedgerConfiguration",
      "inputType": ".com.digitalasset.ledger.api.v1.GetLedgerConfigurationRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetLedgerConfigurationResponse",
      "serverStreaming": true
    }
  ]
}
```

### Ledger Identity Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/ledger_identity_service.proto)

```com.digitalasset.ledger.api.v1.LedgerIdentityService```
> Participant Modules: ```participant.pipeline```
```javascript
{
  "name": "LedgerIdentityService",
  "method": [
    {
      "name": "GetLedgerIdentity",
      "inputType": ".com.digitalasset.ledger.api.v1.GetLedgerIdentityRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetLedgerIdentityResponse"
    }
  ]
}
```

### Package Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/package_service.proto)

```com.digitalasset.ledger.api.v1.PackageService```
> Participant Modules: ```participant.writer```
```javascript
{
  "name": "PackageService",
  "method": [
    {
      "name": "ListPackages",
      "inputType": ".com.digitalasset.ledger.api.v1.ListPackagesRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.ListPackagesResponse"
    },
    {
      "name": "GetPackage",
      "inputType": ".com.digitalasset.ledger.api.v1.GetPackageRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetPackageResponse"
    },
    {
      "name": "GetPackageStatus",
      "inputType": ".com.digitalasset.ledger.api.v1.GetPackageStatusRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetPackageStatusResponse"
    }
  ]
}
```

### Transaction Service

[Proto Definition](https://github.com/DACH-NY/da/blob/master/ledger-api/grpc-definitions/src/main/protobuf/com/digitalasset/ledger/api/v1/transaction_service.proto)

```com.digitalasset.ledger.api.v1.TransactionService```
> Participant Modules: ```participant.pipeline```
```javascript
{
  "name": "TransactionService",
  "method": [
    {
      "name": "GetTransactions",
      "inputType": ".com.digitalasset.ledger.api.v1.GetTransactionsRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetTransactionsResponse",
      "serverStreaming": true
    },
    {
      "name": "GetTransactionTrees",
      "inputType": ".com.digitalasset.ledger.api.v1.GetTransactionsRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetTransactionTreesResponse",
      "serverStreaming": true
    },
    {
      "name": "GetTransactionByEventId",
      "inputType": ".com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetTransactionResponse"
    },
    {
      "name": "GetTransactionById",
      "inputType": ".com.digitalasset.ledger.api.v1.GetTransactionByIdRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetTransactionResponse"
    },
    {
      "name": "GetLedgerEnd",
      "inputType": ".com.digitalasset.ledger.api.v1.GetLedgerEndRequest",
      "outputType": ".com.digitalasset.ledger.api.v1.GetLedgerEndResponse"
    }
  ]
}
```
