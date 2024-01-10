// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.*

@Explanation("Not the end of the world.")
@Resolution(
  "Retry re-submitting the request. If the error persists, contact the participant operator."
)
case object BenignError
    extends ErrorCode("BENIGN_ERROR", ErrorCategory.TransientServerFailure)(
      ErrorClass.root()
    ) {

  final case class Reject(serviceName: String)(implicit
      loggingContext: ContextualizedErrorLogger
  ) extends DamlErrorWithDefiniteAnswer(
        cause = s"Benign problem in $serviceName.",
        extraContext = Map("service_name" -> serviceName),
      )
}
