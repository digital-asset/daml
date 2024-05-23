// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TracedScaffeineTest extends AsyncWordSpec with BaseTest {

  private def getValueBroken(input: Int): FutureUnlessShutdown[Int] = FutureUnlessShutdown(
    Future(UnlessShutdown.AbortedDueToShutdown)
  )
  private def getValue(input: Int): FutureUnlessShutdown[Int] = FutureUnlessShutdown.pure(input)

  "TracedScaffeineUS" should {
    "should get a value when not shutting down" in {
      val keysCache =
        TracedScaffeine.buildTracedAsyncFutureUS[Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = traceContext => input => getValue(input),
        )(logger)
      for {
        result <- keysCache.getUS(10)(TraceContext.empty)
      } yield {
        result shouldBe 10
      }
    }.failOnShutdown

    "Handle an AbortDueToShutdownException" in {
      val keysCache =
        TracedScaffeine.buildTracedAsyncFutureUS[Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = traceContext => input => getValueBroken(input),
        )(logger)

      for {
        result <-
          keysCache.getUS(10).failed
      } yield {
        succeed
      }
    }.failOnShutdown
  }

}
