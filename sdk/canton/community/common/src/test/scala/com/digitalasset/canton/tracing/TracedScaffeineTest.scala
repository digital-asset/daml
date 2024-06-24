// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
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

    "Handle AbortDueToShutdownException in get" in {
      val keysCache =
        TracedScaffeine.buildTracedAsyncFutureUS[Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = traceContext => input => getValueBroken(input),
        )(logger)

      for {
        result <-
          keysCache.getUS(10).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    // Note that when Scaffeine.getAll returns a failed future that wraps the underlying exception
    // with java.util.concurrent.CompletionException
    "Handle AbortDueToShutdownException in getAll" in {
      val keysCache =
        TracedScaffeine.buildTracedAsyncFutureUS[Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = traceContext => input => getValueBroken(input),
        )(logger)

      for {
        result <- keysCache.getAllUS(Set(10)).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Allow entries to be cleared" in {

      val loads = new AtomicInteger(0)
      def getValueCount(input: Int): FutureUnlessShutdown[Int] = {
        loads.incrementAndGet()
        FutureUnlessShutdown.pure(input)
      }

      val keysCache =
        TracedScaffeine.buildTracedAsyncFutureUS[Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = traceContext => input => getValueCount(input),
        )(logger)

      for {
        _ <- keysCache.getUS(2)
        _ <- keysCache.getUS(3)
        _ <- keysCache.getAllUS(Seq(2, 3)).map { m =>
          keysCache.clear((i, _) => i == 2)
          m
        }
        _ <- keysCache.getUS(2)
        _ <- keysCache.getUS(3)
      } yield {
        loads.get() shouldBe 3 // Initial 2 + 1 reload
      }
    }.failOnShutdown

  }

}
