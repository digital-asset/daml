package com.daml.platform.apiserver.error

import com.daml.error.{Explanation, Resolution}
import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}
import com.daml.logging.LoggingContext

@Explanation("Things happen on the API.")
@Resolution("Just restart the server.")
case object SomeApiError
    extends ErrorCode("BLUE_SCREEN", ErrorCategory.SystemInternalAssumptionViolated)(
      ErrorClass.root()
    ) {
  case class Error(someErrArg: String)(implicit val loggingContext: LoggingContext)
      extends BaseError {

    /** The error code, usually passed in as implicit where the error class is defined */
    override def code: ErrorCode = SomeApiError.code

    /** A human readable string indicating the error */
    override def cause: String = "Some cause"

    override def throwableO: Option[Throwable] = Some(
      new IllegalStateException("Should not happen")
    )
  }
}
