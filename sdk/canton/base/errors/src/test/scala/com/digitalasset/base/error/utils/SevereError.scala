// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error.utils

import com.digitalasset.base.error.{
  BaseErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorClass,
  ErrorCode,
  Explanation,
  Resolution,
}

@Explanation("Things happen.")
@Resolution("Turn it off and on again.")
case object SevereError
    extends ErrorCode("BLUE_SCREEN", ErrorCategory.SystemInternalAssumptionViolated)(
      ErrorClass.root()
    ) {
  final case class Reject(
      message: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit
      loggingContext: BaseErrorLogger
  ) extends DamlErrorWithDefiniteAnswer(
        cause = message,
        extraContext = Map("throwableO" -> throwableO.toString),
      )
}
