// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.{
  ContextualizedErrorLogger,
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
      loggingContext: ContextualizedErrorLogger
  ) extends DamlErrorWithDefiniteAnswer(
        cause = message,
        extraContext = Map("throwableO" -> throwableO.toString),
      )
}
