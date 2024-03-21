// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.MaxCauseLogLength
import com.daml.error.SerializableErrorCodeComponents.validateTraceIdAndCorrelationId
import com.google.rpc.Status
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.slf4j.event.Level

import scala.annotation.StaticAnnotation
import scala.util.control.{NoStackTrace, NonFatal}

/** Error Code Definition
  *
  * We want to support our users and our developers with good error codes. Therefore, every error that an API
  * returns should refer to a documented error code and provide some context information.
  *
  * Every error code is uniquely identified using an error-id of max 63 CAPITALIZED_WITH_UNDERSCORES characters
  *
  * Errors are organised according to ErrorGroups. And we separate the error code definition (using a singleton
  * by virtue of using objects to express the nested hierarchy) and the actual error.
  *
  * Please note that there is some implicit argument passing involved in the example below:
  *
  * object SyncServiceErrors extends ParticipantErrorGroup {
  *   object ConnectionErrors extends ErrorGroup {
  *     object DomainUnavailable extends ErrorCode(id="DOMAIN_UNAVAILABLE", ..) {
  *        case class ActualError(someContext: Val) extends BaseError with SyncServiceError
  *        // this error will actually be referring to the same error code!
  *        case class OtherError(otherContext: Val) extends BaseError with SyncServiceError
  *     }
  *   }
  *   object HandshakeErrors extends ErrorGroup {
  *     ...
  *   }
  * }
  */
abstract class ErrorCode(val id: String, val category: ErrorCategory)(implicit
    val parent: ErrorClass
) {

  require(id.nonEmpty, "error-id must be non empty")
  require(id.length < 64, s"error-id is too long: $id")
  require(id.forall(c => c.isUpper || c == '_' || c.isDigit), s"Invalid characters in error-id $id")

  implicit val code: ErrorCode = this

  /** The machine readable error code string, uniquely identifiable by the error id, error category and correlation id.
    * e.g. NO_DOMAINS_CONNECTED(2,ABC234)
    */
  def codeStr(correlationId: Option[String]): String =
    ErrorCodeMsg.codeStr(code.id, category.asInt, correlationId)

  /** @return message including error category id, error code id, correlation id and cause
    */
  def toMsg(cause: => String, correlationId: Option[String]): String = {
    val truncatedCause =
      if (cause.length > MaxCauseLogLength)
        cause.take(MaxCauseLogLength) + "..."
      else
        cause
    ErrorCodeMsg(id, category.asInt, correlationId, truncatedCause)
  }

  /** Log level of the error code
    *
    * Generally, the log level is defined by the error category.
    * In rare cases, it might be overridden by the error code.
    */
  def logLevel: Level = category.logLevel

  /** True if this error may appear on the API */
  protected def exposedViaApi: Boolean = category.grpcCode.nonEmpty

  /** The error conveyance doc string provides a statement about the form this error will be returned to the user */
  def errorConveyanceDocString: Option[String] = {
    val loggedAs = s"This error is logged with log-level $logLevel on the server side"
    val apiLevel = (category.grpcCode, exposedViaApi) match {
      case (Some(code), true) =>
        if (category.securitySensitive)
          s". It is exposed on the API with grpc-status $code without any details for security reasons."
        else
          s" and exposed on the API with grpc-status $code including a detailed error message."
      case _ => "."
    }
    Some(loggedAs ++ apiLevel)
  }
}

object ErrorCodeMsg {
  private val ErrorCodeMsgRegex = """([A-Z_]+)\((\d+),(.+?)\): (.*)""".r

  def apply(
      errorCodeId: String,
      errorCategoryInt: Int,
      maybeCorrelationId: Option[String],
      cause: String,
  ): String =
    s"${codeStr(errorCodeId, errorCategoryInt, maybeCorrelationId)}: $cause"

  def codeStr(
      errorCodeId: String,
      errorCategoryInt: Int,
      maybeCorrelationId: Option[String],
  ): String = s"$errorCodeId($errorCategoryInt,${maybeCorrelationId.getOrElse("0").take(8)})"

  def extract(errorCodeMsg: String): Either[String, (String, Int, String, String)] =
    errorCodeMsg match {
      case ErrorCodeMsgRegex(errorCodeId, errorCategoryIdIntAsString, corrId, cause) =>
        Right((errorCodeId, errorCategoryIdIntAsString.toInt, corrId, cause))
      case other => Left(s"Could not extract error code constituents from $other")
    }
}

object ErrorCode {

  /** The maximum size (in characters) of the self-service error description. */
  val MaxCauseLogLength = 512

  /**  Maximum size (in bytes) of the [[com.google.rpc.Status]] proto that a self-service error code can be serialized into.
    *
    *  We choose this value with the following considerations:
    *  -  The serialized error Status proto is packed into a [[io.grpc.Metadata]] together with the error description,
    *  which is then enriched with additional gRPC-internal entries and converted into HTTP2 headers for transmission.
    *  - The default maximum gRPC metadata size is 8KB (for both clients and servers). We MUST ensure that we don't
    *  exceed this value for error-returning gRPC metadata, otherwise INTERNAL errors may be reported on both the client and server.
    *  - The error description is packed twice in the serialized metadata
    *  (see [[ErrorCode.asGrpcError]] and how it creates a [[io.grpc.StatusRuntimeException]]).
    *
    *  Conservatively we allow a buffer of > 3KB for gRPC and gRPC->HTTP2 internals overhead.
    *  (considering a [[MaxErrorContentBytes]] maximum Status proto size and - [[MaxCauseLogLength]] - description size).
    *
    *  Note: instead of increasing this value, consider limiting better the error contents.
    */
  val MaxErrorContentBytes = 4096

  def asGrpcError(err: BaseError)(implicit
      loggingContext: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    val status = asGrpcStatus(err)(loggingContext)
    // Builder methods for metadata are not exposed, so going route via creating an exception
    val e = StatusProto.toStatusRuntimeException(status)
    // Stripping stacktrace
    err match {
      case _: DamlError =>
        new ErrorCode.LoggedApiException(e.getStatus, e.getTrailers)
      case _ => new ErrorCode.ApiException(e.getStatus, e.getTrailers)
    }
  }

  def asGrpcStatus(err: BaseError)(implicit loggingContext: ContextualizedErrorLogger): Status =
    asGrpcStatus(err, MaxErrorContentBytes)

  private[error] def asGrpcStatus(err: BaseError, maxSerializedErrorSize: Int)(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    try
      SerializableErrorCodeComponents(
        errorCode = err.code,
        loggingContext = loggingContext,
        rawCorrelationId = loggingContext.correlationId,
        rawTraceId = loggingContext.traceId,
        cause = err.cause,
        definiteAnswer = err.definiteAnswerO,
        errorResources = err.resources,
        contextMap = err.context ++ loggingContext.properties,
        retryableInfo = err.retryable.map(_.duration),
      ).toStatusProto(maxSerializedErrorSize)
    catch {
      case NonFatal(e) =>
        val (traceId, correlationId) =
          validateTraceIdAndCorrelationId(loggingContext.traceId, loggingContext.correlationId)

        loggingContext.error(s"Error building gRPC status for error $err", e)
        com.google.rpc.Status
          .newBuilder()
          .setCode(Code.INTERNAL.value())
          .setMessage(
            BaseError.SecuritySensitiveMessage(correlationId = correlationId, traceId = traceId)
          )
          .build()
    }

  class ApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends StatusRuntimeException(status, metadata)
      with NoStackTrace

  /** Exception that has already been logged.
    */
  class LoggedApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends ApiException(status, metadata)
}

// Use these annotations to add more information to the documentation for an error on the website
final case class Explanation(explanation: String) extends StaticAnnotation
final case class Resolution(resolution: String) extends StaticAnnotation
final case class Description(description: String) extends StaticAnnotation
final case class RetryStrategy(retryStrategy: String) extends StaticAnnotation
