// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{
  BaseError,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  LogOnCreation,
  RpcError,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.google.rpc.Status
import com.google.rpc.error_details.ErrorInfo
import io.grpc.StatusRuntimeException

import scala.util.Try
import scala.util.matching.Regex

object ErrorCodeUtils {

  import cats.syntax.either.*

  /** regex suitable to parse an error code string and extract the error recoverability code the
    * (?s) supports multi-line matches
    */
  lazy val errorCodeCategoryRegexp: Regex = "(?s)^[0-9A-Z_]+\\(([0-9]+),[A-Za-z0-9]+\\).*".r

  def errorCategoryFromString(str: String): Option[ErrorCategory] =
    str match {
      case errorCodeCategoryRegexp(retryability, _*) =>
        Either
          .catchOnly[NumberFormatException](retryability.toInt)
          .toOption
          .flatMap(ErrorCategory.fromInt)
      case _ => None
    }

  def isError(str: String, errorCode: ErrorCode): Boolean =
    str.startsWith(errorCode.id)

}

/** The main Canton error for everything that should be logged and notified
  *
  * PREFER [[CantonError]] OVER [[CantonBaseError]] IN ORDER TO LOG THE ERROR IMMEDIATELY UPON
  * CREATION TO ENSURE WE DON'T LOSE THE ERROR MESSAGE.
  *
  * In many cases, we return errors that are communicated to clients as a Left. For such cases, we
  * should use [[com.digitalasset.base.error.RpcError]] to report them.
  *
  * For an actual error instance, you should extend one of the given abstract error classes such as
  * [[CantonError.Impl]] further below (or transaction error).
  *
  * There are two ways to communicate such an error: write it into a log or send it as a string to
  * the user. In most cases, we'll do both: log the error appropriately locally and communicate it
  * to the user by failing the api call with an error string.
  *
  * When we log the error, then we write:
  *   1. ErrorCode
  *   1. ErrorName (name of the class defining the error code)
  *   1. The cause
  *   1. The context
  *
  * The context is given by the following:
  *   1. All arguments of the error case class turned into strings (which invokes pretty printing of
  *      the arguments) EXCEPT: we ignore arguments that have the following RESERVED name: cause,
  *      loggingContext, throwable.
  *   1. The context of the logger (e.g. participant=participant1, synchronizer=da)
  *   1. The trace id.
  */
trait CantonBaseError extends BaseError {

  override def context: Map[String, String] =
    super.context ++ BaseError.extractContext(this)

  // note that all of the following arguments must be constructor arguments, not body values
  // as otherwise we won't be able to log on creation (parent class is initialized before derived class,
  // but constructor arguments are initialized first).
  // so anything using def, lay val and constructor arguments works. just not val. but best, just use
  // [[CantonUserError]] or [[CantonInternalError]]

  def rpcStatusWithoutLoggingContext(): com.google.rpc.status.Status = rpcStatus()(NoLogging)

  def log()(implicit loggingContext: ErrorLoggingContext): Unit = logWithContext()(loggingContext)

  def asGrpcError(implicit loggingContext: ErrorLoggingContext): StatusRuntimeException =
    ErrorCode.asGrpcError(this)(loggingContext)

  def asGoogleGrpcStatus(implicit loggingContext: ErrorLoggingContext): com.google.rpc.Status =
    ErrorCode.asGrpcStatus(this)(loggingContext)

  def toCantonRpcError(implicit loggingContext: ErrorLoggingContext): RpcError = {
    val base = this
    GenericCantonRpcError(
      asGrpcError = base.asGrpcError(loggingContext),
      cause = base.cause,
      asGrpcStatus = base.asGoogleGrpcStatus,
      code = base.code,
      context = base.context,
      resources = base.resources,
      correlationId = loggingContext.correlationId,
      traceId = loggingContext.traceId,
    )
  }
}

trait ContextualizedCantonError extends CantonBaseError with RpcError with LogOnCreation {

  /** The logging context obtained when we created the error, usually passed in as implicit via
    * [[com.digitalasset.canton.logging.NamedLogging]]
    */
  def loggingContext: ErrorLoggingContext

  def logger: ErrorLoggingContext = loggingContext
  def logError(): Unit = logWithContext()(logger)

  /** Flag to control if an error should be logged at creation Generally, we do want to log upon
    * creation, except in the case of "nested" or combined errors, where we just nest the error but
    * don't want it to be logged twice. See [[com.digitalasset.canton.error.ParentCantonError]] as
    * an example.
    */
  override def logOnCreation: Boolean = true

  def asGrpcError: StatusRuntimeException =
    ErrorCode.asGrpcError(this)(loggingContext)

  def asGrpcStatus: com.google.rpc.Status =
    ErrorCode.asGrpcStatus(this)(loggingContext)

  def correlationId: Option[String] = loggingContext.correlationId

  def traceId: Option[String] = loggingContext.traceId

}

object CantonBaseError {

  /** [[CantonError.Impl]]s are logged immediately when they are created. Therefore, they usually
    * expect an implicit [[com.digitalasset.canton.logging.ErrorLoggingContext]] to be around when
    * they are created. If you are creating such an error in a class extending
    * [[com.digitalasset.canton.logging.NamedLogging]], then the implicit function will provide you
    * with such a context. If you don't have that context, then you can also use [[CantonBaseError]]
    * and invoke the logging yourself at a later point in time (which is what we do, for example,
    * with [[TransactionError]]).
    */

  abstract class Impl(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends CantonBaseError

  /** Custom matcher to extract [[com.google.rpc.error_details.ErrorInfo]] from
    * [[com.google.protobuf.any.Any]]
    */
  object AnyToErrorInfo {
    def unapply(any: com.google.protobuf.any.Any): Option[ErrorInfo] =
      if (any.is(ErrorInfo)) {
        Try(any.unpack(ErrorInfo)).toOption
      } else None
  }

  def statusErrorCodes(status: com.google.rpc.status.Status): Seq[String] =
    status.details.collect { case AnyToErrorInfo(errorInfo) => errorInfo.reason }

  def isStatusErrorCode(errorCode: ErrorCode, status: com.google.rpc.status.Status): Boolean =
    extractStatusErrorCodeMessage(errorCode, status).isDefined

  def extractStatusErrorCodeMessage(
      errorCode: ErrorCode,
      status: com.google.rpc.status.Status,
  ): Option[String] = {
    val code = errorCode.category.grpcCode.getOrElse(
      throw new IllegalArgumentException(s"Error code $errorCode does not have a gRPC code")
    )
    Option.when(status.code == code.value() && statusErrorCodes(status).contains(errorCode.id))(
      status.message
    )
  }
}

object CantonError {

  abstract class Impl(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends ContextualizedCantonError

  def stringFromContext(
      error: RpcError
  )(implicit loggingContext: ErrorLoggingContext): String =
    error match {
      case error: CombinedError =>
        (if (error.errors.sizeIs > 1) error.cause + ": " else "") + error.orderedErrors
          .map(stringFromContext(_)(loggingContext))
          .toList
          .mkString(", ")

      case error =>
        val contextMap = error.context ++ loggingContext.properties
        val errorCodeMsg =
          error.code.toMsg(error.cause, loggingContext.traceContext.traceId, limit = None)
        if (contextMap.nonEmpty) {
          errorCodeMsg + "; " + ErrorLoggingContext.formatContextAsString(contextMap)
        } else {
          errorCodeMsg
        }
    }

}

/** Mixing trait for nested errors
  *
  * The classic situation when we re-wrap errors:
  *
  * {{{
  * sealed trait CryptoError extends CantonErrorBuilder
  *
  * sealed trait ProcessingError extends CantonErrorBuilder
  *
  * // NOTE, this error is NOT created within an ErrorCode, as we just inherit the parent error
  * case class CryptoNoBueno(someArgs: String, parent: CryptoError) extends ProcessingError
  *    with ParentCantonError[CryptoError]  {
  *   // we can mixin our context variables
  *   override def mixinContext: Map[String, String] = Map("someArgs" -> someArgs)
  * }
  * }}}
  *
  * Now in the following situation, the someCryptoOp method would generate the CryptoError. This
  * CryptoError would be logged already (on creation) and therefore, the ParentCantonError disabled
  * logging on creation.
  *
  * {{{
  * for {
  *   _ <- someCryptoOp(..).leftMap(CryptoNoBueno("oh nooo", _))
  * } yields ()
  * }}}
  */
trait ParentCantonError[+T <: CantonBaseError] extends CantonBaseError {

  /** The parent error that we want to nest */
  def parent: T

  /** The context we want to mix-in */
  def mixinContext: Map[String, String] = Map()

  override def code: ErrorCode = parent.code
  override def cause: String = parent.cause
  override def context: Map[String, String] = parent.context ++ mixinContext

}

/** Combine several errors into one
  *
  * This is a rare case but can happen. In some cases, we don't have a single parent error like
  * [[ParentCantonError]], but many of them. This trait can be used for such situations.
  *
  * Useful for situations with [[com.digitalasset.canton.util.CheckedT]] collecting several user
  * errors.
  */
trait CombinedError extends RpcError {

  def loggingContext: ErrorLoggingContext

  def errors: NonEmpty[Seq[RpcError]]

  lazy val orderedErrors: NonEmpty[Seq[RpcError]] = errors.sortBy(_.code.category.rank)

  def cause: String = s"A series of ${errors.length} failures occurred"

  def code: ErrorCode = orderedErrors.head1.code

}

/** Generic canton error usually produced when an un-contextualized error want to return an error
  * that has context
  */
final case class GenericCantonRpcError(
    code: ErrorCode,
    context: Map[String, String],
    cause: String,
    resources: Seq[(ErrorResource, String)],
    correlationId: Option[String],
    traceId: Option[String],
    asGrpcStatus: Status,
    asGrpcError: StatusRuntimeException,
) extends RpcError {}
