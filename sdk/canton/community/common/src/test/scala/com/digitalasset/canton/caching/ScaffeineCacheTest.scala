// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class ScaffeineCacheTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  private def getValueBroken(input: Int): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.abortedDueToShutdown
  private def getValue(input: Int): FutureUnlessShutdown[Int] = FutureUnlessShutdown.pure(input)

  "buildAsync" should {
    "Get a value when not shutting down" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValue,
        )(logger)
      for {
        result <- keysCache.get(10)
      } yield {
        result shouldBe 10
      }
    }

    "Handle AbortDueToShutdown in get" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValueBroken,
        )(logger)

      for {
        result <- keysCache.get(10).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    // Note that when Scaffeine.getAll returns a failed future that wraps the underlying exception
    // with java.util.concurrent.CompletionException
    "Handle AbortDueToShutdown in getAll" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValueBroken,
          allLoader = Some(_ => FutureUnlessShutdown.abortedDueToShutdown),
        )(logger)

      for {
        result <- keysCache.getAll(Set(10)).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Handle AbortedDueToShutdown in compute" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValue,
        )(logger)

      for {
        result <- keysCache.compute(10, (_, _) => FutureUnlessShutdown.abortedDueToShutdown).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Pass cached value to compute" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValue,
        )(logger)
      val previousValue = new AtomicReference[Option[Int]]()
      for {
        _ <- keysCache.get(10)
        newValue <- keysCache.compute(
          10,
          (_, previousO) => {
            previousValue.set(previousO)
            FutureUnlessShutdown.pure(20)
          },
        )
      } yield {
        newValue shouldBe 20
        previousValue.get should contain(10)
      }
    }

    "Allow entries to be cleared" in {
      val loads = new AtomicInteger(0)
      def getValueCount(input: Int): FutureUnlessShutdown[Int] = {
        loads.incrementAndGet()
        FutureUnlessShutdown.pure(input)
      }

      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
          loader = getValueCount,
        )(logger)

      for {
        _ <- keysCache.get(2)
        _ <- keysCache.get(3)
        _ <- keysCache.getAll(Seq(2, 3))
        _ = keysCache.clear((i, _) => i == 2)
        _ <- keysCache.get(2)
        _ <- keysCache.get(3)
      } yield {
        loads.get() shouldBe 3 // Initial 2 + 1 reload
      }
    }
  }

  "buildTracedAsync" should {
    "ignore the trace context stored with a key" in {
      val counter = new AtomicInteger()
      val keysCache = ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Int, Int](
        cache = CachingConfigs.testing.mySigningKeyCache.buildScaffeine(),
        loader = traceContext => input => getValue(counter.incrementAndGet() + input),
      )(logger)
      for {
        result1 <- keysCache.get(10)(TraceContext.empty)
        result2 <- keysCache.get(10)(TraceContext.createNew())
      } yield {
        result1 shouldBe result2
      }
    }.failOnShutdown
  }
}
