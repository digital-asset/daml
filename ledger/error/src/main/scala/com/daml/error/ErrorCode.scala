// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.{ValidMetadataKeyRegex, truncateResourceForTransport}
import com.daml.error.definitions.DamlError
import com.daml.error.utils.ErrorDetails
import com.google.rpc.Status
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.slf4j.event.Level

import scala.annotation.StaticAnnotation
import scala.util.control.NoStackTrace
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._

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
    s"$id(${category.asInt},${correlationId.getOrElse("0").take(8)})"

  /** @return message including error category id, error code id, correlation id and cause
    */
  def toMsg(cause: => String, correlationId: Option[String]): String =
    s"${codeStr(correlationId)}: ${ErrorCode.truncateCause(cause)}"

  def asGrpcStatus(err: BaseError)(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val statusInfo = getStatusInfo(err)(loggingContext)
    // Provide error id and context via ErrorInfo
    val errorInfo =
      if (code.category.securitySensitive) {
        None
      } else {
        val errorInfo = ErrorDetails.ErrorInfoDetail(
          errorCodeId = id,
          metadata = statusInfo.contextMap ++
            err.definiteAnswerO.fold(Map.empty[String, String])(value =>
              Map(com.daml.ledger.grpc.GrpcStatuses.DefiniteAnswerKey -> value.toString)
            ),
        )
        Some(errorInfo)
      }
    // Build retry info
    val retryInfo = err.retryable.map(r =>
      ErrorDetails.RetryInfoDetail(
        duration = r.duration
      )
    )
    // Build request info
    val requestInfo = statusInfo.correlationId.map { correlationId =>
      ErrorDetails.RequestInfoDetail(
        correlationId = correlationId
      )
    }
    // Build resource infos
    val resourceInfos =
      if (code.category.securitySensitive) Seq()
      else
        truncateResourceForTransport(err.resources)
          .map { case (resourceType, resourceValue) =>
            ErrorDetails.ResourceInfoDetail(
              typ = resourceType.asString,
              name = resourceValue,
            )
          }
    val allDetails: Seq[ErrorDetails.ErrorDetail] =
      errorInfo.toList ++ retryInfo.toList ++ requestInfo.toList ++ resourceInfos
    // Build status
    val status = com.google.rpc.Status
      .newBuilder()
      .setCode(statusInfo.grpcStatusCode.value())
      .setMessage(statusInfo.message)
      .addAllDetails(allDetails.map(_.toRpcAny).asJava)
      .build()
    status
  }

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

  /** Log level of the error code
    *
    * Generally, the log level is defined by the error category.
    * In rare cases, it might be overridden by the error code.
    */
  def logLevel: Level = category.logLevel

  /** True if this error may appear on the API */
  protected def exposedViaApi: Boolean = category.grpcCode.nonEmpty

  private def getStatusInfo(
      err: BaseError
  )(implicit loggingContext: ContextualizedErrorLogger): ErrorCode.StatusInfo = {
    val correlationId = loggingContext.correlationId
    val message =
      if (code.category.securitySensitive)
        s"${BaseError.SecuritySensitiveMessageOnApiPrefix} ${correlationId.getOrElse("<no-correlation-id>")}"
      else
        code.toMsg(err.cause, loggingContext.correlationId)
    val grpcStatusCode = category.grpcCode
      .getOrElse {
        loggingContext.warn(s"Passing non-grpc error via grpc $id ")
        Code.INTERNAL
      }
    val contextMap = getTruncatedContext(err) + ("category" -> category.asInt.toString)

    ErrorCode.StatusInfo(
      grpcStatusCode = grpcStatusCode,
      message = message,
      contextMap = contextMap,
      correlationId = correlationId,
    )
  }

  private def getTruncatedContext(
      error: BaseError
  )(implicit loggingContext: ContextualizedErrorLogger): Map[String, String] = {
    val raw: Seq[(String, String)] =
      (error.context ++ loggingContext.properties).toSeq.filter(_._2.nonEmpty).sortBy(_._2.length)
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
          (map + (adjustedKey -> truncatedValue), newFree)
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
  private[error] val MaxContentBytes = 2000
  private[error] val MaxCauseLogLength = 512

  class ApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends StatusRuntimeException(status, metadata)
      with NoStackTrace

  /** Exception that has already been logged.
    */
  class LoggedApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends ApiException(status, metadata)

  case class StatusInfo(
      grpcStatusCode: io.grpc.Status.Code,
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

  /** Formats the context as a string for e.g. transport or file logging */
  def formatContextAsString(contextMap: Map[String, String]): String = {
    contextMap
      .filter(_._2.nonEmpty)
      .toSeq
      .sortBy(_._1)
      .map { case (k, v) =>
        s"$k=$v"
      }
      .mkString(", ")
  }

  private[error] def truncateCause(cause: String): String =
    if (cause.length > MaxCauseLogLength) {
      cause.take(MaxCauseLogLength) + "..."
    } else cause
}

// Use these annotations to add more information to the documentation for an error on the website
case class Explanation(explanation: String) extends StaticAnnotation
case class Resolution(resolution: String) extends StaticAnnotation
case class Description(description: String) extends StaticAnnotation
case class RetryStrategy(retryStrategy: String) extends StaticAnnotation
