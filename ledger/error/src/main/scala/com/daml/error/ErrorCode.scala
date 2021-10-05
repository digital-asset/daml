// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.{ValidMetadataKeyRegex, truncateResourceForTransport}
import com.daml.logging.entries.LoggingValue
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.slf4j.MDC
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

  def asGrpcError(err: BaseError)(implicit
      loggingContext: ErrorCodeLoggingContext
  ): StatusRuntimeException = {

    val ErrorCode.StatusInfo(codeGrpc, message, contextMap, correlationId) =
      getStatusInfo(err)

    // provide error id and context via ErrorInfo
    val errInfoBld = com.google.rpc.ErrorInfo.newBuilder().setReason(id)
    if (!code.category.securitySensitive) {
      contextMap.foreach { case (k, v) => errInfoBld.putMetadata(k, v) }
    }

    // TODO error codes: Resolve dependency and use constant
    //    val definiteAnswerKey = com.daml.ledger.grpc.GrpcStatuses.DefiniteAnswerKey
    val definiteAnswerKey = "definite_answer"
    err.definiteAnswerO.foreach { definiteAnswer =>
      errInfoBld.putMetadata(definiteAnswerKey, definiteAnswer.toString)
    }
    val errInfo = com.google.protobuf.Any.pack(errInfoBld.build())

    // add retry info
    val retryInfoO = err.retryable.map { ri =>
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

    // add correlation id
    val requestInfoO = correlationId.map { ci =>
      com.google.protobuf.Any.pack(com.google.rpc.RequestInfo.newBuilder().setRequestId(ci).build())
    }

    // add resource info
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

    // build status
    val bld = com.google.rpc.Status
      .newBuilder()
      .setCode(codeGrpc.value())
      .setMessage(message)

    (Seq(errInfo) ++ retryInfoO.toList ++ requestInfoO.toList ++ resourceInfo).foldLeft(bld) {
      case (acc, item) => acc.addDetails(item)
    }

    // builder methods for metadata are not exposed, so going route via creating an exception
    val ex = StatusProto.toStatusRuntimeException(bld.build())
    // strip stack trace from exception
    new ErrorCode.ApiException(ex.getStatus, ex.getTrailers)
  }

  /** Formats the context as a string for e.g. transport or file logging */
  def formatContextAsString(contextMap: Map[String, String]): String = {
    contextMap
      .filter(_._2.nonEmpty)
      .map { case (k, v) =>
        s"$k=$v"
      }
      .mkString(", ")
  }

  /** log level of the error code
    *
    * Generally, the log level is defined by the error category. In rare cases, it might be overridden
    * by the error code.
    */
  protected def logLevel: Level = category.logLevel

  /** True if this error may appear on the API */
  protected def exposedViaApi: Boolean = category.grpcCode.nonEmpty

  /** Log the cause while adding the context into the MDC
    *
    * We add the context twice to the MDC: first, every map item is added directly
    * and then we add a second string version as "err-context". When we log to file,
    * we add the err-context to the log output.
    * When we log to JSON, we ignore the err-context field.
    */
  def log(err: BaseError, extra: Map[String, String] = Map())(implicit
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Unit = {
    val mergedContext = err.context ++ err.location.map(("location", _)).toList.toMap ++ extra
    // we are putting the context into the MDC twice, once as a serialised string, once argument by argument
    // for text logging, we'll use the err-context string, for json logging, we use the arguments and ignore the err-context
    val arguments = mergedContext ++ Map(
      "error-code" -> codeStr(errorCodeLoggingContext.correlationId),
      "err-context" -> ("{" + formatContextAsString(mergedContext) + "}"),
    )
    arguments.foreach { case (name, value) =>
      MDC.put(name, value)
    }
    val message = toMsg(err.cause, errorCodeLoggingContext.correlationId)
    (logLevel, err.throwableO) match {
      case (Level.INFO, None) => errorCodeLoggingContext.info(message)
      case (Level.INFO, Some(tr)) => errorCodeLoggingContext.info(message, tr)
      case (Level.WARN, None) => errorCodeLoggingContext.warn(message)
      case (Level.WARN, Some(tr)) => errorCodeLoggingContext.warn(message, tr)
      // an error that is logged with < INFO is not an error ...
      case (_, None) => errorCodeLoggingContext.error(message)
      case (_, Some(tr)) => errorCodeLoggingContext.error(message, tr)
    }
    arguments.keys.foreach(key => MDC.remove(key))
  }

  def getStatusInfo(
      err: BaseError
  )(implicit loggingContext: ErrorCodeLoggingContext): ErrorCode.StatusInfo = {
    val correlationId = loggingContext.correlationId
    val message =
      if (code.category.securitySensitive)
        s"${BaseError.SecuritySensitiveMessageOnApi} ${correlationId.getOrElse("<no-correlation-id>")}"
      else
        code.toMsg(err.cause, loggingContext.correlationId)
    val codeGrpc = category.grpcCode
      .getOrElse {
        loggingContext.warn(s"Passing non-grpc error via grpc ${id} ")
        Code.INTERNAL
      }
    val contextMap = getTruncatedContext(err) + ("category" -> category.asInt.toString)

    ErrorCode.StatusInfo(codeGrpc, message, contextMap, correlationId)
  }

  private[error] def getTruncatedContext(
      err: BaseError
  )(implicit loggingContext: ErrorCodeLoggingContext): Map[String, String] = {
    val raw =
      (err.context ++ loggingContext.properties).toSeq.filter(_._2.nonEmpty).sortBy(_._2.length)
    val maxPerEntry = ErrorCode.MaxContentBytes / Math.max(1, raw.size)
    // truncate smart, starting with the smallest value strings such that likely only truncate the largest args
    raw
      .foldLeft((Map.empty[String, String], 0)) { case ((map, free), (k, v)) =>
        val adjustedKey = ValidMetadataKeyRegex.replaceAllIn(k, "").take(63)
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
