// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.StatusRuntimeException
import org.slf4j.event.Level

import scala.annotation.StaticAnnotation
import scala.util.control.NoStackTrace

/** Error Code Definition
  *
  * We want to support our users and our developers with good error codes. Therefore, every error that an API
  * returns should refer to a documented error code and provide some context information.
  *
  * Every error code is uniquely identified using an error-id of max 63 CAPTIALIZED_WITH_UNDERSCORES characters
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

  // TODO error codes: Implement `asGrpcError`

  require(id.length < 64, s"error-id is too long: $id")
  require(id.forall(c => c.isUpper || c == '_' || c.isDigit), s"Invalid characters in error-id $id")

  implicit val code: ErrorCode = this

  def toMsg(cause: => String): String =
    s"${ErrorCode.truncateCause(cause)}"

  // TODO error codes: Pass correlation id
  // TODO error codes: Allow injectable tracing
  def log(logger: ContextualizedLogger, err: BaseError)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    val message = toMsg(err.cause)
    // TODO error codes: Enrich logging context with:
    //                     * error context
    //                     * error location
    //                     * error code
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

  /** log level of the error code
    *
    * Generally, the log level is defined by the error category. In rare cases, it might be overridden
    * by the error code.
    */
  protected def logLevel: Level = category.logLevel

  /** True if this error may appear on the API */
  protected def exposedViaApi: Boolean = category.grpcCode.nonEmpty

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
  private[error] val MaxCauseLogLength = 512

  class ApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends StatusRuntimeException(status, metadata)
      with NoStackTrace

  private[error] def truncateCause(cause: String): String =
    if (cause.length > MaxCauseLogLength) {
      cause.take(MaxCauseLogLength) + "..."
    } else cause
}

// Use these annotations to add more information to the documentation for an error on the website
case class Explanation(explanation: String) extends StaticAnnotation
case class Resolution(resolution: String) extends StaticAnnotation
