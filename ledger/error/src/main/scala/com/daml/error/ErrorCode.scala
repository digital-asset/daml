// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.{StatusInfo, loggingValueToString, truncateResourceForTransport}
import com.daml.logging.entries.LoggingValue
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.slf4j.event.Level

import scala.annotation.StaticAnnotation
import scala.util.control.NoStackTrace
import scala.util.matching.Regex

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

  /** The error code string, uniquely identifiable by the error id, error category and correlation id.
    * e.g. NO_DOMAINS_CONNECTED(2,ABC234)
    */
  def codeStr(correlationId: Option[String]): String =
    s"$id(${category.asInt},${correlationId.getOrElse("0").take(8)})"

  /** Render the cause including codestring and error name */
  def toMsg(cause: => String, correlationId: Option[String]): String =
    s"${codeStr(correlationId)}: ${ErrorCode.truncateCause(cause)}"

  def asGrpcError(err: BaseError, logger: ContextualizedLogger, correlationId: Option[String])(
      loggingContext: LoggingContext
  ): StatusRuntimeException = {
    val StatusInfo(codeInt, message, contextMap, _) =
      getStatusInfo(err, correlationId, logger)(loggingContext)

    // Provide error id and context via ErrorInfo
    val errInfoBuilder = com.google.rpc.ErrorInfo
      .newBuilder()
      .setReason(id)
    val errInfoPrep =
      if (!code.category.securitySensitive) {
        contextMap
          .foldLeft(errInfoBuilder) { case (bld, (k, v)) =>
            bld.putMetadata(k, v)
          }
      } else errInfoBuilder
    val errInfo = com.google.protobuf.Any.pack(errInfoPrep.build())

    // Build retry info
    val retryInfo = err.retryable.map { ri =>
      val millis = ri.duration.toMillis % 1000
      val seconds = (ri.duration.toMillis - millis) / 1000
      val dt = com.google.protobuf.Duration
        .newBuilder()
        .setNanos(millis.toInt * 1000000)
        .setSeconds(seconds)
        .build()

      com.google.protobuf.Any.pack(
        com.google.rpc.RetryInfo
          .newBuilder()
          .setRetryDelay(dt)
          .build()
      )
    }

    // Build request info based on correlation id
    val requestInfo = correlationId.map { ci =>
      com.google.protobuf.Any.pack(com.google.rpc.RequestInfo.newBuilder().setRequestId(ci).build())
    }

    // Build (truncated) resource infos
    val resourceInfo =
      if (code.category.securitySensitive) Seq()
      else
        truncateResourceForTransport(err.resources).map { case (rs, item) =>
          com.google.protobuf.Any
            .pack(
              com.google.rpc.ResourceInfo
                .newBuilder()
                .setResourceType(rs.asString)
                .setResourceName(item)
                .build()
            )
        }

    // Build status
    val statusBuilder = com.google.rpc.Status
      .newBuilder()
      .setCode(codeInt.value())
      .setMessage(message)

    (Seq(errInfo) ++ retryInfo.toList ++ requestInfo.toList ++ resourceInfo)
      .foldLeft(statusBuilder) { case (acc, item) =>
        acc.addDetails(item)
      }

    // Builder methods for metadata are not exposed, so going route via creating an exception
    val ex = StatusProto.toStatusRuntimeException(statusBuilder.build())
    // Strip stack trace from exception
    new ErrorCode.ApiException(ex.getStatus, ex.getTrailers)
  }

  def formatContextAsString(contextMap: Map[String, String]): String =
    contextMap.view
      .filter(_._2.nonEmpty)
      .map { case (k, v) => s"$k=$v" }
      .mkString(", ")

  /** log level of the error code
    *
    * Generally, the log level is defined by the error category. In rare cases, it might be overridden
    * by the error code.
    */
  protected def logLevel: Level = category.logLevel

  /** True if this error may appear on the API */
  protected def exposedViaApi: Boolean = category.grpcCode.nonEmpty

  // TODO error codes: support polymorphic logging/tracing
  private[error] def log(
      logger: ContextualizedLogger,
      err: BaseError,
      correlationId: Option[String],
      extra: Map[String, String],
  )(implicit
      loggingContext: LoggingContext
  ): Unit = {
    val mergedContext = err.context ++ err.location.map(("location", _)).toList.toMap ++ extra

    LoggingContext.withEnrichedLoggingContext(
      "err-context" -> ("{" + formatContextAsString(mergedContext) + "}")
    ) { implicit loggingContext =>
      val message = toMsg(err.cause, correlationId)
      (logLevel, err.throwableO) match {
        case (Level.INFO, None) => logger.info(message)
        case (Level.INFO, Some(tr)) => logger.info(message, tr)
        case (Level.WARN, None) => logger.warn(message)
        case (Level.WARN, Some(tr)) => logger.warn(message, tr)
        // an error that is logged with < INFO is not an error ...
        case (_, None) => logger.error(message)
        case (_, Some(tr)) => logger.error(message, tr)
      }
    }
  }

  def getStatusInfo(
      err: BaseError,
      correlationId: Option[String],
      logger: ContextualizedLogger,
  )(loggingContext: LoggingContext): StatusInfo = {
    val message =
      if (code.category.securitySensitive)
        s"${BaseError.SECURITY_SENSITIVE_MESSAGE_ON_API}: ${correlationId.getOrElse("<no-correlation-id>")}"
      else
        code.toMsg(err.cause, correlationId)

    val codeInt = category.grpcCode
      .getOrElse {
        logger.warn(s"Passing non-grpc error via grpc $id ")(loggingContext)
        Code.INTERNAL
      }

    val contextMap =
      getTruncatedContext(err, loggingContext) + ("category" -> category.asInt.toString)

    StatusInfo(codeInt, message, contextMap, correlationId)
  }

  private[error] def getTruncatedContext(
      err: BaseError,
      loggingContext: LoggingContext,
  ): Map[String, String] = {
    val loggingContextEntries =
      loggingContext.entries.contents.view.map { case (key, value) =>
        key -> loggingValueToString(value)
      }
    val raw =
      (err.context ++ loggingContextEntries).toSeq.filter(_._2.nonEmpty).sortBy(_._2.length)
    val maxPerEntry = ErrorCode.MaxContentBytes / Math.max(1, raw.size)
    // truncate smart, starting with the smallest value strings such that likely only truncate the largest args
    raw
      .foldLeft((Map.empty[String, String], 0)) { case ((map, free), (k, v)) =>
        val adjustedKey = ErrorCode.ValidMetadataKeyRegex.replaceAllIn(k, "").take(63)
        val maxSize = free + maxPerEntry - adjustedKey.length
        val truncatedValue = if (maxSize >= v.length || v.isEmpty) v else v.take(maxSize) + "..."
        // note that we silently discard empty context values and we automatically make the
        // key "gRPC compliant"
        if (v.isEmpty || adjustedKey.isEmpty) {
          (map, free + maxPerEntry)
        } else {
          // keep track of "free space" such that we can keep larger values around
          val newFree = free + maxPerEntry - adjustedKey.length - truncatedValue.length
          (map + (k -> truncatedValue), newFree)
        }
      }
      ._1
  }

  /** The error conveyance doc string provides a statement about the form this error will be returned to the user */
  private[error] def errorConveyanceDocString: Option[String] = {
    val loggedAs = s"This error is logged with log-level $logLevel on the server side."
    val apiLevel = (category.grpcCode, exposedViaApi) match {
      case (Some(code), true) =>
        if (category.securitySensitive)
          s"\nThis error is exposed on the API with grpc-status $code without any details due to security reasons"
        else
          s"\nThis error is exposed on the API with grpc-status $code including a detailed error message"
      case _ => ""
    }
    Some(loggedAs ++ apiLevel)
  }
}

object ErrorCode {
  private val ValidMetadataKeyRegex: Regex = "[^(a-zA-Z0-9-_)]".r
  private val MaxContentBytes = 2000
  private[error] val MaxCauseLogLength = 512

  // TODO error-codes: Extract this function into `LoggingContext` companion (and test)
  private val loggingValueToString: LoggingValue => String = {
    case LoggingValue.Empty => ""
    case LoggingValue.False => "false"
    case LoggingValue.True => "true"
    case LoggingValue.OfString(value) => s"'$value'"
    case LoggingValue.OfInt(value) => value.toString
    case LoggingValue.OfLong(value) => value.toString
    case LoggingValue.OfIterable(sequence) =>
      sequence.map(loggingValueToString).mkString("[", ", ", "]")
    case LoggingValue.Nested(entries) =>
      entries.contents.view
        .map { case (key, value) => s"$key: ${loggingValueToString(value)}" }
        .mkString("{", ", ", "}")
    case LoggingValue.OfJson(json) => json.toString()
  }

  class ApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends StatusRuntimeException(status, metadata)
      with NoStackTrace

  case class StatusInfo(
      codeGrpc: io.grpc.Status.Code,
      message: String,
      contextMap: Map[String, String],
      correlationId: Option[String],
  )

  /** Truncate resource information such that we don't exceed a max error size */
  def truncateResourceForTransport(
      res: Seq[(ErrorResource, String)]
  ): Seq[(ErrorResource, String)] = {
    res
      .foldLeft((Seq.empty[(ErrorResource, String)], 0)) { case ((acc, spent), elem) =>
        val tot = elem._1.asString.length + elem._2.length
        val newSpent = tot + spent
        if (newSpent < ErrorCode.MaxContentBytes) {
          (acc :+ elem, newSpent)
        } else {
          // TODO error codes: Here we silently drop resource info.
          //                   Signal that it was truncated by logs or truncate only expensive fields?
          (acc, spent)
        }
      }
      ._1
  }

  private[error] def truncateCause(cause: String): String =
    if (cause.length > MaxCauseLogLength) {
      cause.take(MaxCauseLogLength) + "..."
    } else cause
}

// Use these annotations to add more information to the documentation for an error on the website
case class Explanation(explanation: String) extends StaticAnnotation
case class Resolution(resolution: String) extends StaticAnnotation
