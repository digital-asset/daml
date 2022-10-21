// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.commons.validation
import io.gatling.core.check.{Check, CheckResult}

object Checks {

  /* Useful for debugging */
  def printResponseCheck[R]: Check[R] = Check.Simple[R](
    f = (response, _, _) => {
      println(s"Response: $response")
      validation.Success(CheckResult(None, None))
    },
    condition = None,
  )
}
