// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.google.rpc.Status
import io.grpc.StatusRuntimeException

/** The main error interface for everything that should be logged and notified.
  *
  * There are two ways to communicate an error to the user: write it into a log or send it as a string.
  * In most cases, we'll do both: log the error appropriately locally and communicate it to the user
  * by failing the API call with an error string.
  */
trait BaseError extends LocationMixin {

  /** The error code, usually passed in as implicit where the error class is defined */
  def code: ErrorCode

  /** A human readable string indicating the error */
  def cause: String

  /** An optional argument to log exceptions
    *
    * If you want to log an exception as part of your error, then use the following example:
    *
    * object MyCode extends ErrorCode(id="SUPER_DUPER_ERROR") {
    *   case class MyError(someString: String, throwable: Throwable) extends SomeInternalError(
    *     cause = "Something failed with an exception bla",
    *     throwableO = Some(throwable)
    *   )
    * }
    *
    * NOTE: This throwable's details are not included the exception communicated to the gRPC clients
    *       so if you want them communicated, you need to explicitly add them to the e.g. context map or cause string.
    */
  def throwableO: Option[Throwable] = None

  /** The context (declared fields) of this error
    */
  def context: Map[String, String] = Map()

  /** The resources related to this error
    *
    * We return the set of resources via com.google.rpc.ResourceInfo. Override this method
    * in order to return resource information via com.google.rpc.Status
    */
  def resources: Seq[(ErrorResource, String)] = Seq()

  def logWithContext(extra: Map[String, String] = Map())(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Unit =
    contextualizedErrorLogger.logError(this, extra)

  /** Returns retryability information of this particular error
    *
    * In some cases, error instances would like to provide custom retry intervals.
    * This can be achieved by locally overriding this method.
    *
    * Do not use this to change the contract of the error categories. Non-retryable errors shouldn't
    * be made retryable. Only use it for adjusting the retry intervals.
    */
  def retryable: Option[ErrorCategoryRetry] = code.category.retryable

  /** Controls whether a `definite_answer` error detail is added to the gRPC status code */
  def definiteAnswerO: Option[Boolean] = None
}

/** Base class for errors for which error context is known at creation.
  */
trait ContextualizedError extends BaseError {

  protected def errorContext: ContextualizedErrorLogger

  def asGrpcStatus: Status =
    code.asGrpcStatus(this)(errorContext)

  def asGrpcError: StatusRuntimeException =
    code.asGrpcError(this)(errorContext)

}

trait LocationMixin {

  /** Contains the location where the error has been created. */
  val location: Option[String] = {
    val stack = Thread.currentThread().getStackTrace
    val thisClassName = this.getClass.getName
    val idx = stack.indexWhere { _.getClassName == thisClassName }
    if (idx != -1 && (idx + 1) < stack.length) {
      val stackTraceElement = stack(idx + 1)
      Some(s"${stackTraceElement.getFileName}:${stackTraceElement.getLineNumber}")
    } else None
  }
}

object BaseError {
  val SecuritySensitiveMessageOnApiPrefix =
    "An error occurred. Please contact the operator and inquire about the request"

  def isSanitizedSecuritySensitiveMessage(msg: String): Boolean = {
    // NOTE: Currently we can't be much more precise than checking only the message prefix
    // as the suffix is a correlation id which is unbounded as ledger implementations
    // are free to choose whatever kind of value is most appropriate for them.
    msg.startsWith(SecuritySensitiveMessageOnApiPrefix)
  }

}
