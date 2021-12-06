// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.samples

object DummmyServer {

  import com.daml.error.{
    BaseError,
    ContextualizedErrorLogger,
    DamlContextualizedErrorLogger,
    ErrorCategory,
    ErrorCategoryRetry,
    ErrorClass,
    ErrorCode,
    ErrorResource,
  }
  import com.daml.logging.{ContextualizedLogger, LoggingContext}

  import scala.concurrent.duration.Duration

  object ErrorCodeFoo
      extends ErrorCode(id = "MY_ERROR_CODE_ID", ErrorCategory.ContentionOnSharedResources)(
        ErrorClass.root()
      ) {

    case class Error(message: String) extends BaseError.Impl(cause = message) {
      override def loggingContext: ContextualizedErrorLogger = new DamlContextualizedErrorLogger(
        ContextualizedLogger.get(getClass),
        LoggingContext.newLoggingContext(identity),
        Some("full-correlation-id-123456790"),
      )

      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.ContractId -> "someContractId"
      )

      override def retryable: Option[ErrorCategoryRetry] = Some(
        ErrorCategoryRetry("me", Duration("123 s"))
      )

      override def context: Map[String, String] = Map("foo" -> "bar")
    }

  }

  def serviceEndpointDummy(): Unit = {
    throw ErrorCodeFoo.Error("A user oriented message").asGrpcError
  }

}

/** This shows how a user can handle error codes.
  * In particular it shows how to extract useful information from the signalled exception with minimal library dependencies.
  *
  * NOTE: This class is given as an example in the official Daml documentation. If you change it here change it also in the docs.
  */
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

        // Extracting error code id.
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

        // Extracting full correlation id if present.
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
              assert(v.getRetryDelay.getSeconds == 123)
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
