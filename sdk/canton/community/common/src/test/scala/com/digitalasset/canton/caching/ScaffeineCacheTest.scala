// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  Executor,
  ExecutorService,
  Executors,
  RejectedExecutionException,
  TimeUnit,
}
import scala.concurrent.ExecutionContext

class ScaffeineCacheTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  private def getValueBroken: Int => FutureUnlessShutdown[Nothing] = (_: Int) =>
    FutureUnlessShutdown.abortedDueToShutdown

  private def getValue(input: Int): FutureUnlessShutdown[Int] = FutureUnlessShutdown.pure(input)

  "buildAsync" should {
    "Get a value when not shutting down" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValue,
        )(logger, "")
      for {
        result <- keysCache.get(10)
      } yield {
        result shouldBe 10
      }
    }

    "Handle AbortDueToShutdown in get" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValueBroken,
        )(logger, "")

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
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValueBroken,
          allLoader = Some(_ => FutureUnlessShutdown.abortedDueToShutdown),
        )(logger, "")

      for {
        result <- keysCache.getAll(Set(10)).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Handle AbortedDueToShutdown in compute" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValue,
        )(logger, "")

      for {
        result <- keysCache.compute(10, (_, _) => FutureUnlessShutdown.abortedDueToShutdown).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Pass cached value to compute" in {
      val keysCache =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValue,
        )(logger, "")
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
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          loader = getValueCount,
        )(logger, "")

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
        cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
        loader = _ => input => getValue(counter.incrementAndGet() + input),
      )(logger, "")
      for {
        result1 <- keysCache.get(10)(TraceContext.empty)
        result2 <- keysCache.get(10)(TraceContext.createNew("test"))
      } yield {
        result1 shouldBe result2
      }
    }.failOnShutdown
  }

  class TestExecutor extends Executor {
    val delegate: ExecutorService = Executors.newFixedThreadPool(1)
    val fallbackExecutions = new AtomicInteger(0)
    def terminate(): Unit = {
      delegate.shutdown()
      delegate.awaitTermination(5, TimeUnit.SECONDS)
    }
    override def execute(command: Runnable): Unit =
      try {
        delegate.execute(command)
      } catch {
        case e: RejectedExecutionException =>
          fallbackExecutions.incrementAndGet()
          throw e
      }
  }

  "shutdown gracefully" should {

    "using loader based caches" in {

      val testExecutor = new TestExecutor()

      // Calling buildAsync calls buildAsyncFuture
      val cache: ScaffeineCache.TunnelledAsyncLoadingCache[FutureUnlessShutdown, Int, Int] =
        ScaffeineCache.buildAsync[FutureUnlessShutdown, Int, Int](
          cache = CachingConfigs.testing.keyCache
            .buildScaffeine(loggerFactory)(ExecutionContext.fromExecutor(testExecutor)),
          loader = i => FutureUnlessShutdown.pure(i),
        )(logger, "")

      for {
        _ <- cache.get(0)
        _ = cache.invalidate(0)
        _ = testExecutor.fallbackExecutions.get() shouldBe 0
        _ = testExecutor.terminate()
        _ <- cache.get(1)
        _ = cache.invalidate(1)
        _ <- cache.get(2)
        _ = cache.invalidateAll()
        _ <- cache.get(3)
        _ = cache.cleanUp()
      } yield {
        testExecutor.fallbackExecutions.get() shouldNot be(0)
        succeed
      }

    }

    "using getFuture based caches" in {

      val testExecutor = new TestExecutor()

      // Calling buildMappedAsync calls buildAsync
      val cache: ScaffeineCache.TunnelledAsyncCache[Int, Int] =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)(
            ExecutionContext.fromExecutor(testExecutor)
          )
        )(logger, "")

      val pureGet: Int => FutureUnlessShutdown[Int] = { i =>
        FutureUnlessShutdown.pure(i)
      }

      for {
        _ <- cache.getFuture(0, pureGet)
        _ = cache.invalidate(0)
        _ = testExecutor.fallbackExecutions.get() shouldBe 0
        _ = testExecutor.terminate()
        _ <- cache.getFuture(1, pureGet)
        _ = cache.invalidate(1)
        _ <- cache.getFuture(2, pureGet)
        _ = cache.invalidateAll()
        _ <- cache.getFuture(3, pureGet)
        _ = cache.cleanUp()
      } yield {
        testExecutor.fallbackExecutions.get() shouldNot be(0)
        succeed
      }

    }

  }

}
