.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Error Codes
###########

Overview
*********


.. _gRPC status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _gRPC status code: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _rich gRPC error model: https://cloud.google.com/apis/design/errors#error_details
.. _standard gRPC description: https://grpc.github.io/grpc-java/javadoc/io/grpc/Status.html#getDescription--


The majority of the errors are a result of some request processing.
They are logged and returned to the user as a failed gRPC response
containing the status code, an optional status message and optional metadata.

This approach remains unchanged in principle while we aim at
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
Clients can still migrate to Daml SDK 1.18 and use the pre-1.18 gRPC status code response behavior by using ``--use-pre-1.18-error-codes``
as a command line option. However, this option is deprecated and will be removed in a future release.


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
                  A value whose purpose is to allow the user to clearly identify the request,
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


Errors returned to users contain a `gRPC status code`_, a description and additional machine readable information
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


.. This file is generated:
.. include:: error-codes-inventory.rst


Error Codes Migration Guide
*****************************


The Ledger API gRPC error codes change introduced in the Daml SDK 1.18 release involves breaking
compatibility with previous releases for some service Ledger API endpoints.

The table below outlines all the cases and error conditions when a Ledger API service endpoint returns a different
gRPC status code in comparison to the pre-1.18 releases.

Common Ledger API changes
---------------------------

The table below outlines generic gRPC status code changes pertaining to the Ledger API
and apply to all ledger backends. For changes specific to a ledger backend, check the next subsections.

(Note that status codes that have not changed for particular endpoints are not listed in the tables below.)

.. include:: status-codes-migration-table-ledger-api.rst

Sandbox (classic)
---------------------------

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
---------------------------

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
