package com.daml.error

import com.daml.logging.{ContextualizedLogger, LoggingContext}

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
   *   case class MyError(someString: String, throwable: Throwable) extends CantonInternalError(
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

  def logWithContext(logger: ContextualizedLogger)(implicit loggingContext: LoggingContext): Unit =
    code.log(logger, this)

  /** Returns retryability information of this particular error
   *
   * In some cases, error instances would like to provide custom retry intervals.
   * This can be achieved by locally overriding this method.
   *
   * Do not use this to change the contract of the error categories. Non-retryable errors shouldn't
   * be made retryable. Only use it for adjusting the retry intervals.
   *
   */
  private[error] def retryable: Option[ErrorCategoryRetry] = code.category.retryable

}

trait LocationMixin {
  /** Contains the location where the error has been created.  */
  val location: Option[String] = {
    val stack = Thread.currentThread().getStackTrace
    val idx = stack.indexWhere { element =>
      element.getClassName == this.getClass.getName
    }
    if (idx != -1 && (idx + 1) < stack.length) {
      val stackTraceElement = stack(idx + 1)
      Some(s"${stackTraceElement.getFileName}:${stackTraceElement.getLineNumber}")
    } else None
  }
}

object BaseError {
  private val ignoreFields = Set("cause", "throwable", "loggingContext")

  def extractContext[D](obj: D): Map[String, String] = {
    obj.getClass.getDeclaredFields
      .filterNot(x => ignoreFields.contains(x.getName) || x.getName.startsWith("_"))
      .map { field =>
        field.setAccessible(true)
        (field.getName, field.get(obj).toString)
      }
      .toMap
  }
}

