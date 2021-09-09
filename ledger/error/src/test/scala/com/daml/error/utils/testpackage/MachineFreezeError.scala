package com.daml.error.utils.testpackage

import com.daml.error.{Explanation, Resolution}
import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}
import com.daml.logging.LoggingContext

@Explanation("Things happen.")
@Resolution("Turn it off and on again.")
case object MachineFreezeError
    extends ErrorCode("BLUE_SCREEN", ErrorCategory.SystemInternalAssumptionViolated)(
      ErrorClass.root()
    ) {
  case class Error(someErrArg: String)(implicit val loggingContext: LoggingContext)
      extends BaseError {

    /** The error code, usually passed in as implicit where the error class is defined */
    override def code: ErrorCode = MachineFreezeError.code

    /** A human readable string indicating the error */
    override def cause: String = "Some cause"

    override def throwableO: Option[Throwable] = Some(new IllegalStateException("Should not happen"))
  }
}
