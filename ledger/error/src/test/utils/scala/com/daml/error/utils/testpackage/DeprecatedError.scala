// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils.testpackage

import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode, Explanation, Resolution}
import com.daml.logging.LoggingContext

@deprecated(since = "since now", message = "This is deprecated")
@Resolution("Turn it off and on again.")
@Explanation("Things happen.")
case object DeprecatedError
    extends ErrorCode("DEPRECATED_ERROR", ErrorCategory.SystemInternalAssumptionViolated)(
      ErrorClass.root()
    ) {
  case class Error(cause: String)(implicit val loggingContext: LoggingContext) extends BaseError {

    /** The error code, usually passed in as implicit where the error class is defined */
    override def code: ErrorCode = SeriousError.code

    override def throwableO: Option[Throwable] = Some(
      new IllegalStateException("Should not happen")
    )
  }
}
