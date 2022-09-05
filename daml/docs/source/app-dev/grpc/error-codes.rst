.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


.. Set max depth of the local table contents (visible in the right hand sidebar in the rendered HTML)
.. See https://www.sphinx-doc.org/en/master/usage/restructuredtext/field-lists.html?highlight=tocdepth

:tocdepth: 2


Error Codes
###########

Overview
********


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


Glossary
********

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


Anatomy of an Error
*******************


Errors returned to users contain a `gRPC status code`_, a description and additional machine readable information
represented in the `rich gRPC error model`_.


Error Description
=================

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
=======================================

We use following error details:

 - A mandatory ``com.google.rpc.ErrorInfo`` containing `error code id`.

 - A mandatory ``com.google.rpc.RequestInfo`` containing (not-truncated) correlation id
   (or ``0`` if correlation id is not available).

 - An optional ``com.google.rpc.RetryInfo`` containing retry interval with milliseconds resolution.

 - An optional ``com.google.rpc.ResourceInfo`` containing information about the resource the failure is based on.
   Any request that fails due to some well-defined resource issues (such as contract, contract-key, package, party, template, domain, etc..) will contain these.
   Particular resources are implementation specific and vary across ledger implementations.

Many errors will include more information,
but there is no guarantee given that additional information will be preserved across versions.


Prevent Security Leaks in Error Codes
=====================================

For any error that could leak information to an attacker, the system will return an error message via the API that 
will not leak any valuable information. The log file will contain the full error message.



Work With Error Codes
*********************

This example shows how a user can extract the relevant error information.

.. code-block:: scala

    object SampleClientSide {

      import com.google.rpc.ResourceInfo
      import com.google.rpc.{ErrorInfo, RequestInfo, RetryInfo}
      import io.grpc.StatusRuntimeException
      import scala.jdk.CollectionConverters._

      def example(): Unit = {
        try {
          DummmyServer.serviceEndpointDummy()
        } catch {
          case e: StatusRuntimeException =>
            // Converting to a status object.
            val status = io.grpc.protobuf.StatusProto.fromThrowable(e)

            // Extracting gRPC status code.
            assert(status.getCode == io.grpc.Status.Code.ABORTED.value())
            assert(status.getCode == 10)

            // Extracting error message, both
            // machine oriented part: "MY_ERROR_CODE_ID(2,full-cor):",
            // and human oriented part: "A user oriented message".
            assert(status.getMessage == "MY_ERROR_CODE_ID(2,full-cor): A user oriented message")

            // Getting all the details
            val rawDetails: Seq[com.google.protobuf.Any] = status.getDetailsList.asScala.toSeq

            // Extracting error code id, error category id and optionally additional metadata.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[ErrorInfo]) =>
                  val v = any.unpack(classOf[ErrorInfo])
                  assert(v.getReason == "MY_ERROR_CODE_ID")
                  assert(v.getMetadataMap.asScala.toMap == Map("category" -> "2", "foo" -> "bar"))
              }.isDefined
            }

            // Extracting full correlation id, if present.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[RequestInfo]) =>
                  val v = any.unpack(classOf[RequestInfo])
                  assert(v.getRequestId == "full-correlation-id-123456790")
              }.isDefined
            }

            // Extracting retry information if the error is retryable.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[RetryInfo]) =>
                  val v = any.unpack(classOf[RetryInfo])
                  assert(v.getRetryDelay.getSeconds == 123, v.getRetryDelay.getSeconds)
                  assert(v.getRetryDelay.getNanos == 456 * 1000 * 1000, v.getRetryDelay.getNanos)
              }.isDefined
            }

            // Extracting resource if the error pertains to some well defined resource.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[ResourceInfo]) =>
                  val v = any.unpack(classOf[ResourceInfo])
                  assert(v.getResourceType == "CONTRACT_ID")
                  assert(v.getResourceName == "someContractId")
              }.isDefined
            }
        }
      }
    }


Error Categories Inventory
**************************

The error categories allow to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.

.. This file is generated:
.. include:: error-categories-inventory.rst.inc



Error Codes Inventory
**********************


.. This file is generated:
.. include:: error-codes-inventory.rst.inc
