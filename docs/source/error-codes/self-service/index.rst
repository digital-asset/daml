.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Self-Service Error Codes (Experimental)
#######################################

.. toctree::
   :hidden:

Overview
*********


TODO: Failure vs. error.

.. _gRPC: https://grpc.github.io/grpc/core/md_doc_statuscodes.html

Participant API server has been using a combination of gRPC_
status codes and custom messages to signal erroneous or unexpected conditions.
This approach remains unchanged in principle while self-service error codes aim at
enhancing it by providing:

- improved consistency of the returned errors across API endpoints,

- richer message format with clearly distinguished machine readable parts to facilitate
  automated error handling strategies,

- complete inventory of all error codes with an explanation, suggested resolution and
  more (as documented in this section).


The goal is to enable users, developers and operators to act upon encountered
errors in a self-service manner such that the need to escalate the problem
or file a ticket occurs occurs infrequently.

Feature flag
************
You can enable self-service error-codes by specifying ``--use-self-service-error-codes``
from command line.

By default self-service error codes are turned off.


Anatomy of an error code
*****************************

TODO Error categories:

 - what they are,

 - how to make use of them,

 - provide a list of all error categories.

Error message
===================
.. code-block:: java

    <ERROR_CODE_ID>(<CATEGORY_ID>,<CORRELATION_ID>:<HUMAN_READABLE_MESSAGE>

The constituent parts are:

  - ``<ERROR_CODE_ID>`` - a unique non empty string containing at most 63 characters:
    upper-cased letters, underscores or digits.
  - ``<CATEGORY_ID>`` - a small integer identifying the corresponding error categories.
  - ``<CORRELATION_ID>`` - a string helpful at identifying originating request.
    ``0`` if no correlation id is given. TODO: Are there limits to its size?
     - The purpose of the correlation-id is to allow a user to clearly identify the request,
       such that the operator can lookup any log information associated with this error.
       TODO: Make sure that's true.
  - ``:`` - a colon character that server as a separator for the machine and human readable parts.
  - ``<HUMAN_READABLE_MESSAGE>`` - message targeting a human reader.

A concrete example how a message might look like this:

.. code-block:: java

    TRANSACTION_NOT_FOUND(11,12345): Transaction not found, or not visible.

Error metadata
====================

In addition error response contain the following metadata:
- mandatory ``com.google.rpc.ErrorInfo`` containing <ERROR_CODE>,

- mandatory ``com.google.rpc.RequestInfo`` containing trace id,

- optional ``com.google.rpc.RetryInfo`` containing retry interval,

- optional ``com.google.rpc.ResourceInfo`` containing key value pair
  of the form: <RESOURCE_NAME>: <RESOURCE_IDENTIFIER>,
  e.g. ""TRANSACTION_ID", "tId12345".
  TODO: Document all possible resource names?


Logging
********

TODO: Excerpt copied form Canton docs verbatim:


Generally, we use the following log-levels on the server:

INFO to log user errors, where the error leads to a failure of the request but the system remains healthy.

WARN to log degradations of the system or point out rather unusual behaviour.

ERROR to log internal errors within the system, where the system does not behave properly and immediate attention is required.


Error categories
******************

TODO: Excerpt copied form Canton docs verbatim:

The error categories allow to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.


Other
********

TODO: Read no further (in this section). Here be dragons.

For participant operators: We aim to guarantee that every error returned from the Participant API:
- is logged,
- is logged at appropriate level,
- error returned to the user contains metadata (trace id) by which participant operators can identify
  related log messages.

In particular self-service error codes aim at improve in the following areas:
- Offer a complete inventory of possible Ledger API error codes as documented on this page.
- Describe each error code byProvide meaningful Ledger error codes on which an application
  or participant operator can act in many cases in a self-service manner.
- Explanation, Category, Conveyance and Resolution.


Error codes inventory
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
