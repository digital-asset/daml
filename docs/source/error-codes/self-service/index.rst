.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Error Codes
###########

.. toctree::
   :hidden:

Overview
*********


.. _gRPC status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _gRPC status code: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _StatusRuntimeException: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
.. _rich gRPC error model: https://cloud.google.com/apis/design/errors#error_details
.. _standard gRPC description: https://grpc.github.io/grpc-java/javadoc/io/grpc/Status.html#getDescription--


The majority of the errors are a result of some request processing.
They are logged and returned to the user as a failed gRPC request
using the standard StatusRuntimeException_.
As such this approach remains unchanged in principle while self-service error codes aim at
enhancing it by providing:

- improved consistency of the returned errors across API endpoints,

- richer error payload format with clearly distinguished machine readable parts to facilitate
  automated error handling strategies,

- complete inventory of all error codes with an explanation, suggested resolution and
  other useful information.


The goal is to enable users, developers and operators to act on the encountered
errors in a self-service manner, either in an automated-way or manually.

Configuration
-------------

The new error code formats and adapted gRPC response statuses are returned by default starting with the Daml 1.18 SDK release.
For backwards-compatibility, the legacy behavior, while deprecated, can be enabled by specifying ``--use-legacy-grpc-error-codes``
from command line.


Glossary
---------------------------

Error
        Represents an occurrence of a failure.
        Consists of:

        - an `error code id`,

        - a `gRPC status code`_ (determined by its error category),

        - an `error category`,

        - a `correlation id`,

        - a human readable message,

        - and optional additional metadata.

        You can think of it as an
        instantiation of an error code.

Error code
             Represents a class of failures.
             Identified by its error code id (we may use `error code` and `error code id` interchangeably in this document).
             Belongs to a single error category.

Error category
                 A broad categorization of error codes that you can base your error handling strategies on.
                 Map to exactly one `gRPC status code`_.
                 We recommended to deal with errors based on their error category.
                 However, if error category itself is too generic
                 you can act on particular error codes.

Correlation id
                  A value whose purpose is to allow a user to clearly identify the request,
                  such that the operator can lookup any log information associated with this error.
                  We use request's submission id for correlation id.


Error Categories
---------------------------

The error categories allow to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.

TransientServerFailure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 1

    **gRPC status code**: UNAVAILABLE

    **Default log level**: INFO

    **Description**: One of the services required to process the request was not available.

    **Resolution**: Expectation: transient failure that should be handled by retrying the request with appropriate backoff.

    **Retry strategy**: Retry quickly in load balancer.


ContentionOnSharedResources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 2

    **gRPC status code**: ABORTED

    **Default log level**: INFO

    **Description**: The request could not be processed due to shared processing resources (e.g. locks or rate limits that replenish quickly) being occupied. If the resource is known (i.e. locked contract), it will be included as a resource info. (Not known resource contentions are e.g. overloaded networks where we just observe timeouts, but can’t pin-point the cause).

    **Resolution**: Expectation: this is processing-flow level contention that should be handled by retrying the request with appropriate backoff.

    **Retry strategy**: Retry quickly (indefinitely or limited), but do not retry in load balancer.


DeadlineExceededRequestStateUnknown
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 3

    **gRPC status code**: DEADLINE_EXCEEDED

    **Default log level**: INFO

    **Description**: The request might not have been processed, as its deadline expired before its completion was signalled. Note that for requests that change the state of the system, this error may be returned even if the request has completed successfully. Note that known and well-defined timeouts are signalled as [[ContentionOnSharedResources]], while this category indicates that the state of the request is unknown.

    **Resolution**: Expectation: the deadline might have been exceeded due to transient resource congestion or due to a timeout in the request processing pipeline being too low. The transient errors might be solved by the application retrying. The non-transient errors will require operator intervention to change the timeouts.

    **Retry strategy**: Retry for a limited number of times with deduplication.


SystemInternalAssumptionViolated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 4

    **gRPC status code**: INTERNAL

    **Default log level**: ERROR

    **Description**: Request processing failed due to a violation of system internal invariants.

    **Resolution**: Expectation: this is due to a bug in the implementation or data corruption in the systems databases. Resolution will require operator intervention, and potentially vendor support.

    **Retry strategy**: Retry after operator intervention.


MaliciousOrFaultyBehaviour
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 5

    **gRPC status code**: UNKNOWN

    **Default log level**: WARN

    **Description**: Request processing failed due to unrecoverable data loss or corruption (e.g. detected via checksums)

    **Resolution**: Expectation: this can be a severe issue that requires operator attention or intervention, and potentially vendor support.

    **Retry strategy**: Retry after operator intervention.


AuthInterceptorInvalidAuthenticationCredentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 6

    **gRPC status code**: UNAUTHENTICATED

    **Default log level**: WARN

    **Description**: The request does not have valid authentication credentials for the operation.

    **Resolution**: Expectation: this is an application bug, application misconfiguration or ledger-level misconfiguration. Resolution requires application and/or ledger operator intervention.

    **Retry strategy**: Retry after app operator intervention.


InsufficientPermission
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 7

    **gRPC status code**: PERMISSION_DENIED

    **Default log level**: WARN

    **Description**: The caller does not have permission to execute the specified operation.

    **Resolution**: Expectation: this is an application bug or application misconfiguration. Resolution requires application operator intervention.

    **Retry strategy**: Retry after app operator intervention.


InvalidIndependentOfSystemState
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 8

    **gRPC status code**: INVALID_ARGUMENT

    **Default log level**: INFO

    **Description**: The request is invalid independent of the state of the system.

    **Resolution**: Expectation: this is an application bug or ledger-level misconfiguration (e.g. request size limits). Resolution requires application and/or ledger operator intervention.

    **Retry strategy**: Retry after app operator intervention.


InvalidGivenCurrentSystemStateOther
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 9

    **gRPC status code**: FAILED_PRECONDITION

    **Default log level**: INFO

    **Description**: The mutable state of the system does not satisfy the preconditions required to execute the request. We consider the whole Daml ledger including ledger config, parties, packages, and command deduplication to be mutable system state. Thus all Daml interpretation errors are reported as as this error or one of its specializations.

    **Resolution**: ALREADY_EXISTS and NOT_FOUND are special cases for the existence and non-existence of well-defined entities within the system state; e.g., a .dalf package, contracts ids, contract keys, or a transaction at an offset. OUT_OF_RANGE is a special case for reading past a range. Violations of the Daml ledger model always result in these kinds of errors. Expectation: this is due to application-level bugs, misconfiguration or contention on application-visible resources; and might be resolved by retrying later, or after changing the state of the system. Handling these errors requires an application-specific strategy and/or operator intervention.

    **Retry strategy**: Retry after app operator intervention.


InvalidGivenCurrentSystemStateResourceExists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 10

    **gRPC status code**: ALREADY_EXISTS

    **Default log level**: INFO

    **Description**: Special type of InvalidGivenCurrentSystemState referring to a well-defined resource.

    **Resolution**: Same as [[InvalidGivenCurrentSystemStateOther]].

    **Retry strategy**: Inspect resource failure and retry after resource failure has been resolved (depends on type of resource and application).


InvalidGivenCurrentSystemStateResourceMissing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 11

    **gRPC status code**: NOT_FOUND

    **Default log level**: INFO

    **Description**: Special type of InvalidGivenCurrentSystemState referring to a well-defined resource.

    **Resolution**: Same as [[InvalidGivenCurrentSystemStateOther]].

    **Retry strategy**: Inspect resource failure and retry after resource failure has been resolved (depends on type of resource and application).


InvalidGivenCurrentSystemStateSeekAfterEnd
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 12

    **gRPC status code**: OUT_OF_RANGE

    **Default log level**: INFO

    **Description**: This error is only used by the ledger Api server in connection with invalid offsets.

    **Resolution**: tbd

    **Retry strategy**: Retry after app operator intervention.


BackgroundProcessDegradationWarning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    **Category id**: 13

    **gRPC status code**: N/A

    **Default log level**: WARN

    **Description**: This error category is used internally to signal to the system operator an internal degradation.

    **Resolution**:

    **Retry strategy**: Not an API error, therefore not retryable.




Anatomy of an Error
---------------------------


Errors returned to users are represented as instances of standard StatusRuntimeException_.
As such they contain a `gRPC status code`_, a description and additional machine readable information
represented in the `rich gRPC error model`_.


Error Description
^^^^^^^^^^^^^^^^^^^

We use the `standard gRPC description`_ that additionally adheres to our custom message format:

.. code-block:: java

    <ERROR_CODE_ID>(<CATEGORY_ID>,<CORRELATION_ID_PREFIX>):<HUMAN_READABLE_MESSAGE>

The constituent parts are:

  - ``<ERROR_CODE_ID>`` - a unique non empty string containing at most 63 characters:
    upper-cased letters, underscores or digits.
    Identifies corresponding error code id.

  - ``<CATEGORY_ID>`` - a small integer identifying the corresponding error category.

  - ``<CORRELATION_ID_PREFIX>`` - a string aimed at identifying originating request.
    Absence of one is indicated by value ``0``.
    If present it is an 8 character long prefix of the corresponding request's submission id.
    Full correlation id can be found in error's additional machine readable information
    (see `Additional Machine Readable Information`_).

  - ``:`` - a colon character that serves as a separator for the machine and human readable parts.

  - ``<HUMAN_READABLE_MESSAGE>`` - a message targeted at a human reader.
    Should never be parsed by applications, as the description might change
    in future releases to improve clarity.

In a concrete example an error description might look like this:

.. code-block:: java

    TRANSACTION_NOT_FOUND(11,12345): Transaction not found, or not visible.


Additional Machine Readable Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We use following error details:

 - A mandatory ``com.google.rpc.ErrorInfo`` containing `error code id`.

 - A mandatory ``com.google.rpc.RequestInfo`` containing (not-truncated) correlation id
   (or ``0`` if correlation id is not available).

 - An optional ``com.google.rpc.RetryInfo`` containing retry interval in seconds.

 - An optional ``com.google.rpc.ResourceInfo`` containing information about the resource the failure is based on.
   Any request that fails due to some well-defined resource issues (such as contract, contract-key, package, party, template, domain, etc..) will contain these.
   Particular resources are implementation specific and vary across ledger implementations.

Many errors will include more information,
but there is no guarantee given that additional information will be preserved across versions.



Error Codes Inventory
**********************

.. list-all-error-codes::


Error Codes Migration Guide
---------------------------

The Ledger API gRPC error codes change introduced in the Daml SDK 1.18 release involves breaking
compatibility with previous releases for some service Ledger API endpoints.

The table below outlines all the cases and error conditions when a Ledger API service endpoint returns a different
gRPC status code in comparison to the pre-1.18 releases.

For example, a service endpoint previously returning gRPC status code ``CODE_A`` may now return
``CODE_B`` for some error condition while returning ``CODE_A`` for other conditions,
and only an entry for the change from ``CODE_A`` in ``CODE_B`` is included in the table below.

Ledger API
^^^^^^^^^^

The table below outlines generic gRPC status code changes pertaining to the Ledger API
and apply to all ledger backends. For changes specific to a ledger backend, check the next subsections.

+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|Service endpoint                               |gRPC status code (before SDK 1.18) |gRPC status code (since SDK 1.18) |Remarks                                                                                                                  |Ledger API error code ID                     |
+===============================================+===================================+==================================+=========================================================================================================================+=============================================+
|ActiveContractsService.getActiveContracts      |NOT_FOUND                          |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned.                 |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandCompletionService.completionStream      |NOT_FOUND                          |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned.                 |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.                                               |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |ABORTED                            |INTERNAL                          |INTERNAL is not returned on Command Service submissions on unexpected errors.                                            |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |ABORTED                            |UNAVAILABLE                       |UNAVAILABLE is now returned on Command Service submissions on backpressure.                                              |SERVICE_NOT_RUNNING                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |RESOURCE_EXHAUSTED                 |ABORTED                           |ABORTED is now returned on Command Service submissions on backpressure.                                                  |PARTICIPANT_BACKPRESSURE                     |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.                                               |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |ABORTED                            |INTERNAL                          |INTERNAL is not returned on Command Service submissions on unexpected errors.                                            |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |ABORTED                            |UNAVAILABLE                       |UNAVAILABLE is now returned on Command Service submissions on backpressure.                                              |SERVICE_NOT_RUNNING                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |RESOURCE_EXHAUSTED                 |ABORTED                           |ABORTED is now returned on Command Service submissions on backpressure.                                                  |PARTICIPANT_BACKPRESSURE                     |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.                                               |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |ABORTED                            |INTERNAL                          |INTERNAL is not returned on Command Service submissions on unexpected errors.                                            |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |ABORTED                            |UNAVAILABLE                       |UNAVAILABLE is now returned on Command Service submissions on backpressure.                                              |SERVICE_NOT_RUNNING                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |RESOURCE_EXHAUSTED                 |ABORTED                           |ABORTED is now returned on Command Service submissions on backpressure.                                                  |PARTICIPANT_BACKPRESSURE                     |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED is now returned on Command Service submissions timeouts.                                               |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |ABORTED                            |INTERNAL                          |INTERNAL is not returned on Command Service submissions on unexpected errors.                                            |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |ABORTED                            |UNAVAILABLE                       |UNAVAILABLE is now returned on Command Service submissions on backpressure.                                              |SERVICE_NOT_RUNNING                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |RESOURCE_EXHAUSTED                 |ABORTED                           |ABORTED is now returned on Command Service submissions on backpressure.                                                  |PARTICIPANT_BACKPRESSURE                     |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandSubmissionService.submit                |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |ABORTED                            |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when a configuration update was rejected.                                        |CONFIGURATION_ENTRY_REJECTED                 |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|GrpcHealthService.check                        |NOT_FOUND                          |INVALID_ARGUMENT                  |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|GrpcHealthService.watch                        |NOT_FOUND                          |INVALID_ARGUMENT                  |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PackageManagementService.uploadDarFile         |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PackageManagementService.uploadDarFile         |INVALID_ARGUMENT                   |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when a package upload was rejected.                                              |PACKAGE_UPLOAD_REJECTED                      |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ParticipantPruningService.prune                |INVALID_ARGUMENT                   |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when the supplied pruning offset is not before the ledger end.                   |OFFSET_OUT_OF_RANGE                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PartyManagementService.allocateParty           |ABORTED                            |DEADLINE_EXCEEDED                 |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                            |ALREADY_EXISTS                    |ALREADY_EXISTS can now be returned when there was a duplicate contract key during interpretation.                        |DUPLICATE_CONTRACT_KEY_DURING_INTERPRETATION |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                            |INTERNAL                          |INTERNAL can now be returned when validation fails on a mismatch during relay of the submitted transaction.              |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                            |NOT_FOUND                         |NOT_FOUND can now be returned when contract key was not found during interpretation.                                     |CONTRACT_NOT_FOUND                           |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when a Daml transaction fails during interpretation.                             |DAML_INTERPRETATION_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |INTERNAL                          |INTERNAL can now be returned in case of internal errors.                                                                 |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |NOT_FOUND                         |NOT_FOUND can now be returned when a Daml interpreter can not resolve a contract key to an active contract.              |CONTRACT_KEY_NOT_FOUND                       |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |NOT_FOUND                         |NOT_FOUND can now be returned when a Daml transaction was referring to a package which was not known to the participant. |MISSING_PACKAGE                              |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |NOT_FOUND                         |NOT_FOUND can now be returned when an exercise or fetch happens on a transaction-locally consumed contract.              |CONTRACT_NOT_ACTIVE                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                   |UNKNOWN                           |UNKNOWN can now be returned when package validation fails.                                                               |PACKAGE_VALIDATION_FAILED                    |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |UNAVAILABLE                        |NOT_FOUND                         |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getFlatTransactionByEventId |NOT_FOUND                          |INVALID_ARGUMENT                  |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionByEventId     |NOT_FOUND                          |INVALID_ARGUMENT                  |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionTrees         |INVALID_ARGUMENT                   |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when the supplied offset was out of range.                                       |OFFSET_OUT_OF_RANGE                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionTrees         |NOT_FOUND                          |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned.                 |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactions             |INVALID_ARGUMENT                   |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when the supplied offset was out of range.                                       |OFFSET_OUT_OF_RANGE                          |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactions             |NOT_FOUND                          |FAILED_PRECONDITION               |FAILED_PRECONDITION can now be returned when attempting to access the data that has already been pruned.                 |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+-----------------------------------+----------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+

Sandbox (classic)
^^^^^^^^^^^^^^^^^

The following gRPC status codes have changed for submission rejections in Sandbox classic.

+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|gRPC status code (before SDK 1.18) |gRPC status code (since SDK 1.18)      |Remarks                                                                                     |Ledger API error code ID                |
+===================================+=======================================+============================================================================================+========================================+
|ABORTED                            |ALREADY_EXISTS                         |ALREADY_EXISTS is now returned on duplicate contract key transaction rejections.            |DUPLICATE_CONTRACT_KEY                  |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on invalid ledger time transaction rejections.          |INVALID_LEDGER_TIME                     |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on transaction rejections on consistency errors.        |INCONSISTENT, INCONSISTENT_CONTRACT_KEY |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|ABORTED                            |NOT_FOUND                              |NOT_FOUND is now returned on transaction rejections on not found contract.                  |CONTRACT_NOT_FOUND                      |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|ABORTED                            |NOT_FOUND                              |NOT_FOUND is now returned on rejections occurring due to missing ledger configuration.      |LEDGER_CONFIGURATION_NOT_FOUND          |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|INVALID_ARGUMENT                   |INTERNAL                               |INTERNAL is now returned on transaction rejections on system faults.                        |DISPUTED                                |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+
|INVALID_ARGUMENT                   |NOT_FOUND                              |PARTY_NOT_KNOWN_ON_LEDGER is now returned on transaction rejections on unallocated parties. |PARTY_NOT_KNOWN_ON_LEDGER               |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------+----------------------------------------+

**NOTE**: Additionally, UNAVAILABLE is now returned when trying to reset the Sandbox server during an ongoing re-initialization (was FAILED_PRECONDITION).


Daml Sandbox and VMBC
^^^^^^^^^^^^^^^^^^^^^

The following gRPC status codes have changed for submission rejections in the Ledger API backed by KV-based ledgers (Daml Sandbox and VMBC).

+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|gRPC status code (before SDK 1.18) |gRPC status code (since SDK 1.18)      |Remarks                                                                                                 |Ledger API error code ID                                        |
+===================================+=======================================+========================================================================================================+================================================================+
|ABORTED                            |ALREADY_EXISTS                         |ALREADY_EXISTS is now returned on duplicate resource transaction rejections.                            |DUPLICATE_CONTRACT_KEY, DUPLICATE_COMMAND                       |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on a submission that has violated some constraint on ledger time.   |INVALID_LEDGER_TIME                                             |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on consistency error transaction rejections.                        |INCONSISTENT, INCONSISTENT_CONTRACT_KEY, INCONSISTENT_CONTRACTS |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on invalid record time transaction rejections.                      |INVALID_RECORD_TIME                                             |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on transaction rejections on record time bounds violations.         |RECORD_TIME_OUT_OF_RANGE                                        |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |FAILED_PRECONDITION                    |FAILED_PRECONDITION is now returned on transaction rejections on time monotonicity violations.          |CAUSAL_MONOTONICITY_VIOLATED                                    |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|ABORTED                            |INTERNAL                               |INTERNAL is now returned on submissions missing mandatory participant input.                            |MISSING_INPUT_STATE                                             |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|INVALID_ARGUMENT                   |INTERNAL                               |INTERNAL is now returned on an invalid transaction submission that was not detected by the participant. |DISPUTED                                                        |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|INVALID_ARGUMENT                   |INTERNAL                               |INTERNAL is now returned on consistency errors that should have been caught by the participant.         |INTERNALLY_INCONSISTENT_KEYS, INTERNALLY_DUPLICATE_KEYS         |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|INVALID_ARGUMENT                   |INTERNAL                               |INTERNAL is now returned on invalid transaction submissions.                                            |VALIDATION_FAILURE                                              |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|INVALID_ARGUMENT                   |INTERNAL                               |INTERNAL is now returned on transaction rejections when an invalid participant state has been detected. |INVALID_PARTICIPANT_STATE                                       |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|INVALID_ARGUMENT                   |NOT_FOUND                              |NOT_FOUND is now returned on transaction rejections on unallocated parties.                             |SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER, PARTY_NOT_KNOWN_ON_LEDGER |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
|UNKNOWN                            |INTERNAL                               |INTERNAL is now returned on transaction rejections without a status.                                    |REJECTION_REASON_NOT_SET                                        |
+-----------------------------------+---------------------------------------+--------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+
