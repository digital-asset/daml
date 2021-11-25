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
======================================

The new error code formats and adapted gRPC response statuses are returned by default starting with the Daml 1.18 SDK release.
Clients can still migrate to Daml SDK 1.18 and use the pre-1.18 gRPC status code response behavior by using ``--use-pre-1.18-error-codes``
as a command line option. However, this option is deprecated and will be removed in a future release.


Glossary
======================================

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
======================================

The error categories allow to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.

.. This file is generated:
.. include:: error-categories-inventory.rst


Anatomy of an Error
======================================


Errors returned to users contain a `gRPC status code`_, a description and additional machine readable information
represented in the `rich gRPC error model`_.


Error Description
--------------------------------------

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
----------------------------------------------------------------------------

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
======================================

The table below outlines generic gRPC status code changes pertaining to the Ledger API
and apply to all ledger backends. For changes specific to a ledger backend, check the next subsections.

(Note that status codes that have not changed for particular endpoints are not listed in the tables below.)

.. include:: status-codes-migration-table-ledger-api.rst

Sandbox (classic)
======================================

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
======================================

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
