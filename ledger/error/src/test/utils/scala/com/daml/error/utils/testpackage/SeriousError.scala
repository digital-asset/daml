// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils.testpackage

import com.daml.error.{Explanation, Resolution}
import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}

@Explanation("Things happen.")
@Resolution("Turn it off and on again.")
case object SeriousError
    extends ErrorCode("BLUE_SCREEN", ErrorCategory.SystemInternalAssumptionViolated)(
      ErrorClass.root()
    ) {
  case class Error(
      cause: String,
      override val context: Map[String, String] = Map.empty,
      override val definiteAnswerO: Option[Boolean] = Some(true),
  ) extends BaseError {

    /** The error code, usually passed in as implicit where the error class is defined */
    override def code: ErrorCode = SeriousError.code

    override def throwableO: Option[Throwable] = Some(
      new IllegalStateException("Should not happen")
    )
  }
}
