// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.rpc.error_details.ErrorInfo
import io.grpc.StatusRuntimeException

import scala.util.Try
import scala.util.matching.Regex

object ErrorCodeUtils {

  import cats.syntax.either.*

  /** regex suitable to parse an error code string and extract the error recoverability code
    * the (?s) supports multi-line matches
    */
  lazy val errorCodeCategoryRegexp: Regex = "(?s)^[0-9A-Z_]+\\(([0-9]+),[A-Za-z0-9]+\\).*".r

  def errorCategoryFromString(str: String): Option[ErrorCategory] = {
    str match {
      case errorCodeCategoryRegexp(retryability, _*) =>
        Either
          .catchOnly[NumberFormatException](retryability.toInt)
          .toOption
          .flatMap(ErrorCategory.fromInt)
      case _ => None
    }
  }

  def isError(str: String, errorCode: ErrorCode): Boolean =
    str.startsWith(errorCode.id)

}

/** The main Canton error for everything that should be logged and notified
  *
  * PREFER [[CantonError]] OVER [[BaseCantonError]] IN ORDER TO LOG THE ERROR IMMEDIATELY UPON CREATION
  * TO ENSURE WE DON'T LOSE THE ERROR MESSAGE.
  *
  * In many cases, we return errors that are communicated to clients as a Left. For such cases,
  * we should use CantonError to report them.
  *
  * For an actual error instance, you should extend one of the given abstract error classes such as [[CantonError.Impl]]
  * further below (or transaction error).
  *
  * There are two ways to communicate such an error: write it into a log or send it as a string to the user.
  * In most cases, we'll do both: log the error appropriately locally and communicate it to the user
  * by failing the api call with an error string.
  *
  * When we log the error, then we write:
  *   1) ErrorCode
  *   2) ErrorName (name of the class defining the error code)
  *   3) The cause
  *   4) The context
  *
  * The context is given by the following:
  *   1) All arguments of the error case class turned into strings (which invokes pretty printing of the arguments)
  *      EXCEPT: we ignore arguments that have the following RESERVED name: cause, loggingContext, throwable.
  *   2) The context of the logger (e.g. participant=participant1, domain=da)
  *   3) The trace id.
  */
trait BaseCantonError extends BaseError {

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
    code.asGrpcError(this)(loggingContext)

  def asGoogleGrpcStatus(implicit loggingContext: ErrorLoggingContext): com.google.rpc.Status =
    code.asGrpcStatus(this)(loggingContext)

}

object CantonErrorResource {

  private lazy val all =
    Seq(ContractId, ContractKey, DalfPackage, LedgerId, DomainId, DomainAlias, CommandId)

  def fromString(str: String): Option[ErrorResource] = all.find(_.asString == str)

  object ContractId extends ErrorResource {
    def asString: String = "CONTRACT_ID"
  }
  object ContractKey extends ErrorResource {
    def asString: String = "CONTRACT_KEY"
  }
  object DalfPackage extends ErrorResource {
    def asString: String = "PACKAGE"
  }
  object LedgerId extends ErrorResource {
    def asString: String = "LEDGER_ID"
  }
  object DomainId extends ErrorResource {
    def asString: String = "DOMAIN_ID"
  }
  object DomainAlias extends ErrorResource {
    def asString: String = "DOMAIN_ALIAS"
  }
  object CommandId extends ErrorResource {
    def asString: String = "COMMAND_ID"
  }
}

/** [[CantonError]]s are logged immediately when they are created. Therefore, they usually expect
  * an implicit [[com.digitalasset.canton.logging.ErrorLoggingContext]] to be around when they are created.
  * If you are creating such an error in a class extending [[com.digitalasset.canton.logging.NamedLogging]],
  * then the implicit function will provide you with such a context. If you don't have that context, then you can
  * also use [[BaseCantonError]] and invoke the logging yourself at a later point in time (which is what we do,
  * for example, with [[TransactionError]]).
  */
trait CantonError extends BaseCantonError {

  /** The logging context obtained when we created the error, usually passed in as implicit via [[com.digitalasset.canton.logging.NamedLogging]] */
  def loggingContext: ErrorLoggingContext

  /** Flag to control if an error should be logged at creation
    *
    * Generally, we do want to log upon creation, except in the case of "nested" or combined errors,
    * where we just nest the error but don't want it to be logged twice.
    * See [[com.digitalasset.canton.error.ParentCantonError]] as an example.
    */
  def logOnCreation: Boolean = true

  def log(): Unit = logWithContext()(loggingContext)

  def asGrpcError: StatusRuntimeException =
    code.asGrpcError(this)(loggingContext)

  // automatically log the error on generation
  if (logOnCreation) {
    log()
  }

}

object BaseCantonError {
  abstract class Impl(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends BaseCantonError {}

  /** Custom matcher to extract [[com.google.rpc.error_details.ErrorInfo]] from [[com.google.protobuf.any.Any]] */
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
      extends CantonError {}

  def stringFromContext(error: BaseError)(implicit loggingContext: ErrorLoggingContext): String =
    error match {
      case error: CombinedError[_] =>
        (if (error.errors.length > 1) error.cause + ": " else "") + error.orderedErrors
          .map(stringFromContext(_)(loggingContext))
          .toList
          .mkString(", ")

      case error =>
        val contextMap = error.context ++ loggingContext.properties
        val errorCodeMsg = error.code.toMsg(error.cause, loggingContext.traceContext.traceId)
        if (contextMap.nonEmpty) {
          errorCodeMsg + "; " + ContextualizedErrorLogger.formatContextAsString(contextMap)
        } else {
          errorCodeMsg
        }
    }
}

/** Mixing trait for nested errors
  *
  * The classic situation when we re-wrap errors:
  *
  * sealed trait CryptoError extends CantonError
  *
  * sealed trait ProcessingError extends CantonError
  *
  * // NOTE, this error is NOT created within an ErrorCode, as we just inherit the parent error
  * case class CryptoNoBueno(someArgs: String, parent: CryptoError) extends ProcessingError
  *    with ParentCantonError[CryptoError]  {
  *   // we can mixin our context variables
  *   override def mixinContext: Map[String, String] = Map("someArgs" -> someArgs)
  * }
  *
  * Now in the following situation, the someCryptoOp method would generate the CryptoError.
  * This CryptoError would be logged already (on creation) and therefore, the ParentCantonError
  * disabled logging on creation.
  *
  * for {
  *   _ <- someCryptoOp(..).leftMap(CryptoNoBueno("oh nooo", _))
  * } yields ()
  */
trait ParentCantonError[+T <: BaseCantonError] extends BaseCantonError {

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
  * This is a rare case but can happen. In some cases, we don't have a single
  * parent error like [[ParentCantonError]], but many of them. This trait can
  * be used for such situations.
  *
  * Useful for situations with [[com.digitalasset.canton.util.CheckedT]] collecting
  * several user errors.
  */
trait CombinedError[+T <: BaseCantonError] {
  this: BaseCantonError =>

  def loggingContext: ErrorLoggingContext

  def errors: NonEmpty[Seq[T]]

  lazy val orderedErrors: NonEmpty[Seq[T]] = errors.sortBy(_.code.category.rank)

  override def cause: String = s"A series of ${errors.length} failures occurred"

  override def code: ErrorCode = orderedErrors.head1.code

}
