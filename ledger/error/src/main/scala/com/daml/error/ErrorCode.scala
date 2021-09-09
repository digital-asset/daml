package com.daml.error

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.StatusRuntimeException
import org.slf4j.event.Level

import scala.annotation.StaticAnnotation
import scala.util.control.NoStackTrace

abstract class ErrorCode(val id: String, val category: ErrorCategory)(implicit
    val parent: ErrorClass
) {

  require(id.length < 64, s"error-id is too long: $id")
  require(id.forall(c => c.isUpper || c == '_' || c.isDigit), s"Invalid characters in error-id $id")

  implicit val code: ErrorCode = this

  /** The error code string
    *
    * We'll get e.g. NO_DOMAINS_CONNECTED(2,ABC234)
    */
  def codeStr(correlationId: Option[String]): String =
    s"$id(${category.asInt},${correlationId.getOrElse("0").take(8)})"

  def toMsg(cause: => String): String =
    s"${ErrorCode.truncateCause(cause)}"

  def log(logger: ContextualizedLogger, err: BaseError)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    val message = toMsg(err.cause)
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
  def errorConveyanceDocString: Option[String] = {
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
  class ApiException(status: io.grpc.Status, metadata: io.grpc.Metadata)
      extends StatusRuntimeException(status, metadata)
      with NoStackTrace

  private[error] def truncateCause(cause: String): String =
    if (cause.length > 512) {
      cause.take(512) + "..."
    } else cause
}

// Use these annotations to add more information to the documentation for an error on the website
case class Explanation(explanation: String) extends StaticAnnotation
case class Resolution(resolution: String) extends StaticAnnotation
