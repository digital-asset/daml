.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Self-Service Error Codes (Experimental)
#######################################

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

Feature Flag
=============

You can enable self-service error-codes by specifying ``--use-self-service-error-codes``
from command line.

By default self-service error codes are turned off.


Glossary
=============

Error - represents an occurrence of a failure.
        Consists of:

        - an `error code id`,

        - a `gRPC status code`_ (determined by its error category),

        - an `error category`,

        - a `correlation id`.

        - a human readable message

        - and optional additional metadata.

        You can think of it as an
        instantiation of an error code.

Error code - represents a class of failures.
             Identified by its error code id (we may use `error code` and `error code id` interchangeably in this document).
             Belongs to a single error category.

Error category - a broad categorization of error codes that you can base your error handling strategies on.
                 Map to exactly one `gRPC status code`_.
                 We recommended to deal with errors based on their error category.
                 However, if error category itself is too generic
                 you can act on particular error codes.

Correlation id - a value whose purpose is to allow a user to clearly identify the request,
                 such that the operator can lookup any log information associated with this error.


Error Categories
==========================

The error categories allow to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.

+------------------------------------------------+------------+--------------------+
|Error category                                  |Category id |gRPC code           |
+================================================+============+====================+
|TransientServerFailure                          |1           |UNAVAILABLE         |
+------------------------------------------------+------------+--------------------+
|ContentionOnSharedResources                     |2           |ABORTED             |
+------------------------------------------------+------------+--------------------+
|DeadlineExceededRequestStateUnknown             |3           |DEADLINE_EXCEEDED   |
+------------------------------------------------+------------+--------------------+
|SystemInternalAssumptionViolated                |4           |INTERNAL            |
+------------------------------------------------+------------+--------------------+
|MaliciousOrFaultyBehaviour                      |5           |UNKNOWN             |
+------------------------------------------------+------------+--------------------+
|AuthInterceptorInvalidAuthenticationCredentials |6           |UNAUTHENTICATED     |
+------------------------------------------------+------------+--------------------+
|InsufficientPermission                          |7           |PERMISSION_DENIED   |
+------------------------------------------------+------------+--------------------+
|InvalidIndependentOfSystemState                 |8           |INVALID_ARGUMENT    |
+------------------------------------------------+------------+--------------------+
|InvalidGivenCurrentSystemStateOther             |9           |FAILED_PRECONDITION |
+------------------------------------------------+------------+--------------------+
|InvalidGivenCurrentSystemStateResourceExists    |10          |ALREADY_EXISTS      |
+------------------------------------------------+------------+--------------------+
|InvalidGivenCurrentSystemStateResourceMissing   |11          |NOT_FOUND           |
+------------------------------------------------+------------+--------------------+
|InvalidGivenCurrentSystemStateSeekAfterEnd      |12          |OUT_OF_RANGE        |
+------------------------------------------------+------------+--------------------+
|BackgroundProcessDegradationWarning             |13          |N/A                 |
+------------------------------------------------+------------+--------------------+


Anatomy of an Error
==========================


Errors returned to users are represented as instances of standard StatusRuntimeException_.
As such they contain a `gRPC status code`_, a description and additional machine readable information
represented in the `rich gRPC error model`_.


Error Description
--------------------------

We use the `standard gRPC description`_ that additionally adheres to our custom message format:

.. code-block:: java

    <ERROR_CODE_ID>(<CATEGORY_ID>,<TRUNCATED_CORRELATION_ID>):<HUMAN_READABLE_MESSAGE>

The constituent parts are:

  - ``<ERROR_CODE_ID>`` - a unique non empty string containing at most 63 characters:
    upper-cased letters, underscores or digits.
    Identifies corresponding error code id.

  - ``<CATEGORY_ID>`` - a small integer identifying the corresponding error category.

  - ``<TRUNCATED_CORRELATION_ID>`` - a string aimed at identifying originating request.
    Contains at most 8 characters of the original correlation id.
    NOTE: Contains value ``0`` if no correlation id was given.
    Full correlation id can be found in error's additional machine readable information
    (see `Additional machine readable information`_).

  - ``:`` - a colon character that serves as a separator for the machine and human readable parts.

  - ``<HUMAN_READABLE_MESSAGE>`` - message targeted at a human reader.
                                   Should never be parsed by applications, as the description might change
                                   in future releases to improve clarity.

In a concrete example an error description might look like this:

.. code-block:: java

    TRANSACTION_NOT_FOUND(11,12345): Transaction not found, or not visible.


Additional Machine Readable Information
----------------------------------------------------

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




Logging
=============

Generally, we use the following log-levels on the server:

 - ``INFO`` to log user errors, where the error leads to a failure of the request but the system remains healthy.

 - ``WARN`` to log degradations of the system or point out rather unusual behaviour.

 - ``ERROR`` to log internal errors within the system, where the system does not behave properly and immediate attention is required.



Error Codes Inventory
**********************

.. list-all-error-codes::


Self-Service Error Codes Migration Guide
########################################

The introduction of the self-service error codes means that some of the gRPC error codes returned from service methods change.
Consult the table below for details on those changes.

Note that the table below contains entries only for cases where a gRPC codes was changed.
For example, a service method previously returning gRPC error code ``CODE_A`` may now return 
``CODE_B`` for some error condition while returning ``CODE_A`` for other conditions,
and only an entry for the change from ``CODE_A`` in ``CODE_B`` is included in the table below.


+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|Service method                                 |gRPC error code (legacy errors) |gRPC error code (self-service errors) |Remarks                                                                                                                  |Sef-service error code id                    |
+===============================================+================================+======================================+=========================================================================================================================+=============================================+
|ActiveContractsService.getActiveContracts      |NOT_FOUND                       |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when attempting to access the data that has already been pruned.                        |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandCompletionService.completionStream      |NOT_FOUND                       |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when attempting to access the data that has already been pruned.                        |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWait                   |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransaction     |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionId   |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandService.submitAndWaitForTransactionTree |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|CommandSubmissionService.submit                |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |ABORTED                         |DEADLINE_EXCEEDED                     |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |ABORTED                         |FAILED_PRECONDITION                   |FAILED_PRECONDITION can now be returned when a configuration update was rejected.                                        |CONFIGURATION_ENTRY_REJECTED                 |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ConfigManagementService.setTimeModel           |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|GrpcHealthService.check                        |NOT_FOUND                       |INVALID_ARGUMENT                      |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|GrpcHealthService.watch                        |NOT_FOUND                       |INVALID_ARGUMENT                      |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PackageManagementService.uploadDarFile         |ABORTED                         |DEADLINE_EXCEEDED                     |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PackageManagementService.uploadDarFile         |INVALID_ARGUMENT                |FAILED_PRECONDITION                   |FAILED_PRECONDITION can now be returned when a package upload was rejected.                                              |PACKAGE_UPLOAD_REJECTED                      |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|ParticipantPruningService.prune                |INVALID_ARGUMENT                |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when the supplied pruning offset was out of range.                                      |REQUESTED_OFFSET_OUT_OF_RANGE                |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|PartyManagementService.allocateParty           |ABORTED                         |DEADLINE_EXCEEDED                     |DEADLINE_EXCEEDED can now be returned when a time out was reached.                                                       |REQUEST_TIME_OUT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                         |ALREADY_EXISTS                        |ALREADY_EXISTS can now be returned when there was a duplicate contract key during interpretation.                        |DUPLICATE_CONTRACT_KEY_DURING_INTERPRETATION |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                         |INTERNAL                              |INTERNAL can now be returned when validation fails on a mismatch during relay of the submitted transaction.              |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |ABORTED                         |NOT_FOUND                             |NOT_FOUND can now be returned when contract key was not found during interpretation.                                     |CONTRACT_NOT_FOUND                           |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |FAILED_PRECONDITION                   |FAILED_PRECONDITION can now be returned when a Daml transaction fails during interpretation.                             |DAML_INTERPRETATION_ERROR                    |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |INTERNAL                              |INTERNAL can now be returned in case of internal errors.                                                                 |LEDGER_API_INTERNAL_ERROR                    |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |NOT_FOUND                             |NOT_FOUND can now be returned when a Daml interpreter can not resolve a contract key to an active contract.              |CONTRACT_KEY_NOT_FOUND                       |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |NOT_FOUND                             |NOT_FOUND can now be returned when a Daml transaction was referring to a package which was not known to the participant. |MISSING_PACKAGE                              |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |NOT_FOUND                             |NOT_FOUND can now be returned when an exercise or fetch happens on a transaction-locally consumed contract.              |CONTRACT_NOT_ACTIVE                          |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |INVALID_ARGUMENT                |UNKNOWN                               |UNKNOWN can now be returned when package validation fails.                                                               |PACKAGE_VALIDATION_FAILED                    |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|SubmissionService.submit                       |UNAVAILABLE                     |NOT_FOUND                             |NOT_FOUND can now be returned when a ledger configuration was not found.                                                 |LEDGER_CONFIGURATION_NOT_FOUND               |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getFlatTransactionByEventId |NOT_FOUND                       |INVALID_ARGUMENT                      |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionByEventId     |NOT_FOUND                       |INVALID_ARGUMENT                      |INVALID_ARGUMENT can now be returned when the received request contains invalid values.                                  |INVALID_ARGUMENT                             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionTrees         |INVALID_ARGUMENT                |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when the supplied offset was out of range.                                              |REQUESTED_OFFSET_OUT_OF_RANGE                |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactionTrees         |NOT_FOUND                       |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when attempting to access the data that has already been pruned.                        |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactions             |INVALID_ARGUMENT                |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when the supplied offset was out of range.                                              |REQUESTED_OFFSET_OUT_OF_RANGE                |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
|TransactionService.getTransactions             |NOT_FOUND                       |OUT_OF_RANGE                          |OUT_OF_RANGE can now be returned when attempting to access the data that has already been pruned.                        |PARTICIPANT_PRUNED_DATA_ACCESSED             |
+-----------------------------------------------+--------------------------------+--------------------------------------+-------------------------------------------------------------------------------------------------------------------------+---------------------------------------------+
