// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.commons.validation
import io.gatling.core.Predef._
import io.gatling.core.check.{Check, CheckResult}

object Checks {

  def printResponseCheck[R]: Check[R] = new PrintResponseCheck[R]

  /** Useful for debugging.
    *
    * @tparam R response type
    */
  private class PrintResponseCheck[R] extends Check[R] {
    override def check(
        response: R,
        session: Session,
        preparedCache: java.util.Map[Any, Any],
    ): validation.Validation[CheckResult] = {
      println(s"Response: $response")
      validation.Success(CheckResult(None, None))
    }
  }
}
