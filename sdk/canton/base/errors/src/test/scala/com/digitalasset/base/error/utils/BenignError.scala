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

@Explanation("Not the end of the world.")
@Resolution(
  "Retry re-submitting the request. If the error persists, contact the participant operator."
)
case object BenignError
    extends ErrorCode("BENIGN_ERROR", ErrorCategory.TransientServerFailure)(
      ErrorClass.root()
    ) {

  final case class Reject(serviceName: String)(implicit
      loggingContext: BaseErrorLogger
  ) extends DamlErrorWithDefiniteAnswer(
        cause = s"Benign problem in $serviceName.",
        extraContext = Map("service_name" -> serviceName),
      )
}
