// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils.testpackage.subpackage

import com.daml.error._
import com.daml.logging.LoggingContext

@Explanation("Test: Things like this always happen.")
@Resolution("Test: Why not ignore?")
case object NotSoSeriousError
    extends ErrorCode(
      "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
      ErrorCategory.BackgroundProcessDegradationWarning,
    )(
      ErrorClass.root().extend("Some error class")
    ) {
  case class Error(someErrArg: String)(implicit val loggingContext: LoggingContext)
      extends BaseError {

    override def code: ErrorCode = NotSoSeriousError.code

    override def cause: String = "Some obscure cause"
  }
}
