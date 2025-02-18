// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.error.*
import com.daml.error.BaseError.RedactedMessage
import com.daml.error.ErrorCategory.GenericErrorCategory
import com.google.protobuf.any
import com.google.rpc.error_details.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import com.google.rpc.status.Status as RpcStatus
import io.grpc.{Status, StatusRuntimeException}
import org.slf4j.event.Level
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.util.Try

/** Generic error class used for creating error instances from deserialized gRPC statuses
  * that resulted from serialization of self-service error codes (children of [[BaseError]]).
  * Its aim is to be used in client applications and components for simplifying programmatic
  * inspection and/or enrichment of Canton errors received over-the-wire or from persistence.
  *
  * Note: Do NOT use this class for explicitly instantiating errors.
  *       Instead, use or create a fully-typed error instance.
  */
final case class DecodedCantonError(
    code: ErrorCode,
    cause: String,
    correlationId: Option[String],
    traceId: Option[String],
    override val context: Map[String, String],
    override val resources: Seq[(ErrorResource, String)],
    override val definiteAnswerO: Option[Boolean] = None,
) extends BaseError {
  def toRpcStatusWithForwardedRequestId: RpcStatus = super.rpcStatus(None)(
    new NoLogging(properties = Map.empty, correlationId = correlationId, traceId = traceId)
  )

  def retryIn: Option[FiniteDuration] = code.category.retryable.map(_.duration)

  def isRetryable: Boolean = retryIn.nonEmpty
}

object DecodedCantonError {

  /** Deserializes a [[com.google.rpc.status.Status]] to [[DecodedCantonError]].
    * With the exception of throwables, all serialized error information is extracted,
    * making this method an inverse of [[BaseError.rpcStatus]].
    */
  def fromGrpcStatus(status: RpcStatus): Either[String, DecodedCantonError] = {
    val rawDetails = status.details

    val statusCode = Status.fromCodeValue(status.code).getCode
    status.message match {
      case RedactedMessage(correlationId, traceId) =>
        Right(
          redactedError(
            grpcCode = statusCode,
            correlationId = correlationId,
            traceId = traceId,
          )
        )
      case _ => tryDeserializeStatus(status, rawDetails)
    }
  }

  def fromStatusRuntimeException(
      statusRuntimeException: StatusRuntimeException
  ): Either[String, DecodedCantonError] =
    Either
      .catchOnly[IllegalArgumentException](
        io.grpc.protobuf.StatusProto.fromThrowable(statusRuntimeException)
      )
      .leftMap(ex => s"Failed to decode error from exception: ${ex.getMessage}")
      .map(RpcStatus.fromJavaProto)
      .flatMap(fromGrpcStatus)

  private def tryDeserializeStatus(
      status: RpcStatus,
      rawDetails: Seq[any.Any],
  ): Either[String, DecodedCantonError] =
    for {
      errorInfoSeq <- extractErrorDetail[ErrorInfo](rawDetails)
      errorInfo <- errorInfoSeq.exactlyOne
      requestInfoSeq <- extractErrorDetail[RequestInfo](rawDetails)
      requestInfoO <- requestInfoSeq.atMostOne
      retryInfoSeq <- extractErrorDetail[RetryInfo](rawDetails)
      retryInfo <- retryInfoSeq.atMostOne
      resourceInfo <- extractErrorDetail[ResourceInfo](rawDetails)
      resources = resourceInfo.map { resourceInfo =>
        ErrorResource(resourceInfo.resourceType) -> resourceInfo.resourceName
      }
      errorCategory <- extractErrorCategory(
        errorInfo = errorInfo,
        statusCode = status.code,
        retryableDuration = retryInfo.flatMap(_.retryDelay).map(_.asJavaDuration.toScala),
      )
      traceId = errorInfo.metadata.get("tid")
      cause = extractCause(status)
      // The RequestInfo.requestId is set primarily to the ContextualizedErrorLogger.correlationId
      // with the ContextualizedErrorLogger.traceId as a fallback.
      correlationId = requestInfoO.collect {
        case requestInfo if !traceId.contains(requestInfo.requestId) => requestInfo.requestId
      }
    } yield DecodedCantonError(
      code = GenericErrorCode(id = errorInfo.reason, category = errorCategory),
      cause = cause,
      context = errorInfo.metadata,
      resources = resources,
      correlationId = correlationId,
      traceId = traceId,
    )

  private def extractCause(status: RpcStatus) =
    ErrorCodeMsg
      .extract(status.message)
      .map { case (_, _, _, cause) => cause }
      // We don't guarantee backwards-compatibility for error message formats
      // Hence fallback to the original cause on failure to parse
      .getOrElse(status.message)

  private def redactedError(
      grpcCode: Status.Code,
      correlationId: Option[String],
      traceId: Option[String],
  ): DecodedCantonError =
    DecodedCantonError(
      code = GenericErrorCode(
        id = "NA",
        category = GenericErrorCategory(
          grpcCode = Some(grpcCode),
          logLevel = Level.ERROR,
          retryable = None,
          redactDetails = true,
          // Security sensitive errors do not carry the category id
          asInt = -1,
          rank = 1,
        ),
      ),
      cause = "A security-sensitive error has been received",
      correlationId = correlationId,
      traceId = traceId,
      context = Map.empty,
      resources = Seq.empty,
    )

  private def extractErrorCategory(
      errorInfo: ErrorInfo,
      statusCode: Int,
      retryableDuration: Option[FiniteDuration],
  ): Either[String, ErrorCategory] = {
    def unknownCategory(categoryId: Int) =
      GenericErrorCategory(
        grpcCode = Some(Status.fromCodeValue(statusCode).getCode),
        // If we log it, we use INFO since it's received from an
        // external component
        logLevel = Level.INFO,
        retryable = retryableDuration.map(ErrorCategoryRetry.apply),
        redactDetails = false,
        asInt = categoryId,
        rank = -1,
      )

    for {
      categoryValue <- errorInfo.metadata
        .get("category")
        .toRight(s"category key not found in error metadata: ${errorInfo.metadata}")
      categoryId <- Try(categoryValue.toInt).toEither.left.map(e =>
        s"Failed parsing category value: ${e.getMessage}"
      )
    } yield ErrorCategory.all.find(_.asInt == categoryId).getOrElse(unknownCategory(categoryId))
  }

  private def extractErrorDetail[T <: GeneratedMessage](
      errorDetails: Seq[com.google.protobuf.any.Any]
  )(implicit
      expectedTypeCompanion: GeneratedMessageCompanion[T]
  ): Either[String, List[T]] =
    errorDetails.toList
      .filter(_ is expectedTypeCompanion)
      .traverse { errDetail =>
        Try(errDetail.unpack[T]).toEither.left.map(throwable =>
          s"Could not extract ${expectedTypeCompanion.scalaDescriptor.fullName} from error details: ${throwable.getMessage}"
        )
      }

  private implicit class AritySelectors[T <: GeneratedMessage](seq: Seq[T])(implicit
      expectedTypeCompanion: GeneratedMessageCompanion[T]
  ) {
    def atMostOne: Either[String, Option[T]] =
      Either.cond(seq.sizeIs <= 1, seq.headOption, invalid("at most one"))

    def exactlyOne: Either[String, T] = seq match {
      case Seq(errInfo) => Right(errInfo)
      case _ => Left(invalid("exactly one"))
    }

    private def invalid(times: String) =
      s"Could not extract error detail. Expected $times ${expectedTypeCompanion.scalaDescriptor.fullName} in status details, but got ${seq.size}"
  }

  /** Dummy error class for the purpose of creating the [[GenericErrorCode]].
    * It has no effect on documentation as its intended user ([[DecodedCantonError]])
    * does not appear in documentation.
    */
  private implicit val genericErrorClass: ErrorClass = ErrorClass(
    List(Grouping("generic", "ErrorClass"))
  )

  /** Generic wrapper for error codes received from deserialized gRPC-statuses */
  private final case class GenericErrorCode(
      override val id: String,
      override val category: ErrorCategory,
  ) extends ErrorCode(id, category)
}
