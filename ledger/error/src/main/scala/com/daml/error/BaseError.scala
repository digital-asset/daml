// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    */
  def throwableO: Option[Throwable] = None

  /** The context (declared fields) of this error
    *
    * At the moment, we'll figure them out using reflection.
    */
  def context: Map[String, String] = BaseError.extractContext(this)

  /** The resources related to this error
    *
    * We return the set of resources via com.google.rpc.ResourceInfo. Override this method
    * in order to return resource information via com.google.rpc.Status
    */
  def resources: Seq[(ErrorResource, String)] = Seq()

  def logWithContext(extra: Map[String, String] = Map())(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Unit =
    errorCodeLoggingContext.logError(this, extra)

  def asGrpcStatusFromContext(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Status =
    code.asGrpcStatus(this)

  def asGrpcErrorFromContext(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): StatusRuntimeException =
    code.asGrpcError(this)

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
  private val ignoreFields = Set("cause", "throwable", "loggingContext")
  val SecuritySensitiveMessageOnApi =
    "An error occurred. Please contact the operator and inquire about the request"

  def extractContext[D](obj: D): Map[String, String] =
    obj.getClass.getDeclaredFields
      .filterNot(x => ignoreFields.contains(x.getName) || x.getName.startsWith("_"))
      .map { field =>
        field.setAccessible(true)
        (field.getName, field.get(obj).toString)
      }
      .toMap

  abstract class Impl(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends BaseError {

    /** The logging context obtained when we created the error, usually passed in as implicit */
    def loggingContext: ContextualizedErrorLogger

    /** Flag to control if an error should be logged at creation
      *
      * Generally, we do want to log upon creation, except in the case of "nested" or combined errors,
      * where we just nest the error but don't want it to be logged twice.
      */
    def logOnCreation: Boolean = true

    def log(): Unit = logWithContext()(loggingContext)

    def asGrpcStatus: Status =
      code.asGrpcStatus(this)(loggingContext)

    def asGrpcError: StatusRuntimeException =
      code.asGrpcError(this)(loggingContext)

    // Automatically log the error on generation
    if (logOnCreation) {
      log()
    }
  }
}
