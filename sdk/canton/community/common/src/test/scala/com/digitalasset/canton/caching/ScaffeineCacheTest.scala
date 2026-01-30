// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  Executor,
  ExecutorService,
  Executors,
  RejectedExecutionException,
  TimeUnit,
}
import scala.concurrent.{
  ExecutionContext,
  ExecutionContextExecutor,
  Future,
} // Ensure implicits for FlatMap are in scope

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

  "buildMappedAsync" should {

    "Handle AbortDueToShutdown in getFuture" in {
      val keysCache =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)
        )(logger, "")

      for {
        result <- keysCache.getFuture(10, _ => FutureUnlessShutdown.abortedDueToShutdown).unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Handle AbortedDueToShutdown in compute" in {
      val keysCache =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)
        )(logger, "")

      for {
        result <- keysCache
          .compute(10)(_ => Some(FutureUnlessShutdown.abortedDueToShutdown))
          .value
          .unwrap
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "Pass cached value to compute" in {
      val keysCache =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)
        )(logger, "")
      val previousValue = new AtomicReference[Option[FutureUnlessShutdown[Int]]]()
      for {
        _ <- keysCache.getFuture(10, _ => FutureUnlessShutdown.pure(11))
        newValue <- keysCache
          .compute(10) { previousO =>
            previousValue.set(previousO)
            Some(FutureUnlessShutdown.pure(20))
          }
          .value
        getNewValue = keysCache.getIfPresentSync(10)
      } yield {
        newValue shouldBe 20
        getNewValue shouldBe Some(newValue)
        previousValue.get.value.futureValueUS shouldBe 11
      }
    }

    "Remove cached value through compute" in {
      val keysCache =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)
        )(logger, "")

      for {
        _ <- keysCache.getFuture(10, _ => FutureUnlessShutdown.pure(11))
        previousValue = keysCache.getIfPresentSync(10)
        newValue = keysCache.compute(10)(_ => None)
        getNewValue = keysCache.getIfPresentSync(10)
      } yield {
        newValue should not be defined
        getNewValue shouldBe newValue
        previousValue should contain(11)
      }
    }

    "handle compute atomically" in {
      val executor = Executors.newFixedThreadPool(16)
      implicit val executionContext: ExecutionContextExecutor =
        ExecutionContext.fromExecutor(executor)

      val numFutures = 10000
      var counter = 0

      val keysCache =
        ScaffeineCache.buildMappedAsync[Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory)
        )(logger, "")

      val futures = (1 to numFutures).map { i =>
        Future(
          keysCache
            .compute(1) { _ =>
              counter = counter + 1
              Some(FutureUnlessShutdown(Future.apply(UnlessShutdown.Outcome(i))))
            }
            .value
        )
      }

      (for {
        resultsFUS <- Future.sequence(futures)
        results <- FutureUnlessShutdown.sequence(resultsFUS)
      } yield {
        counter shouldBe numFutures
        results.sorted shouldBe (1 to numFutures)
      }).thereafter { _ =>
        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.SECONDS)
      }
    }
  }

  "TunnelledAsyncCacheWithAuxCache" should {

    implicit val executionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

    val longRunningComputation: Int => FutureUnlessShutdown[Int] = { i =>
      // Simulate long-running computation
      FutureUnlessShutdown {
        Future {
          UnlessShutdown.Outcome {
            Threading.sleep(1000)
            i
          }
        }
      }
    }

    "add a long-running computation to the aux cache eventually" in {

      val keysCache =
        new ScaffeineCache.TunnelledAsyncCacheWithAuxCache[Int, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          getAuxKeyO = x => Some(x + 1),
          sizeMetric = CommonMockMetrics.dbStorage.internalContractIdsCacheSize,
          tracedLogger = logger,
          context = "",
          loggerFactory = loggerFactory,
        )

      val computationF = keysCache.getFuture(10, longRunningComputation)
      val auxCacheEntry0 = keysCache.getIfPresentAuxKey(11) // check the auxiliary cache
      val cacheEntry0 = keysCache.getIfPresentSync(10) // check the main cache
      for {
        _ <- computationF // wait for the computation to finish
      } yield {
        auxCacheEntry0 shouldBe None
        cacheEntry0 shouldBe None
        // we need eventually here since the auxiliary cache is populated after the main cache in the onComplete
        // callback of getFuture, and we do not have a way to wait until this happens
        eventually(timeUntilSuccess = 5.seconds) {
          keysCache.getIfPresentAuxKey(11) shouldBe Some(10) // check the auxiliary cache
          keysCache.getIfPresentSync(10) shouldBe Some(10) // check the main cache
        }
      }
    }

    "ignore a long-running computation for an entry that has already been removed" in {

      val keysCache =
        new ScaffeineCache.TunnelledAsyncCacheWithAuxCache[Int, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          getAuxKeyO = x => Some(x + 1),
          sizeMetric = CommonMockMetrics.dbStorage.internalContractIdsCacheSize,
          tracedLogger = logger,
          context = "",
          loggerFactory = loggerFactory,
        )

      val computationF = keysCache.getFuture(10, longRunningComputation)
      keysCache.invalidate(10) // evict the cache entry
      for {
        _ <- computationF // wait for the computation to finish
        // wait a bit more to ensure the auxiliary cache update would have happened
        _ = Threading.sleep(500)
        _ = keysCache.cleanUp()
        auxCacheEntry = keysCache.getIfPresentAuxKey(11) // check the auxiliary cache
        cacheEntry = keysCache.getIfPresentSync(10) // check the main cache
      } yield {
        auxCacheEntry shouldBe None
        cacheEntry shouldBe None
      }
    }

    "respect a newer addition even for a long-running computation for an entry that has already been removed" in {

      val keysCache =
        new ScaffeineCache.TunnelledAsyncCacheWithAuxCache[Int, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          getAuxKeyO = x => Some(x + 1),
          sizeMetric = CommonMockMetrics.dbStorage.internalContractIdsCacheSize,
          tracedLogger = logger,
          context = "",
          loggerFactory = loggerFactory,
        )

      val computationF = keysCache.getFuture(10, longRunningComputation)
      keysCache.invalidate(10) // evict the cache entry
      keysCache.getFuture(10, _ => FutureUnlessShutdown.pure(42)).discard
      for {
        _ <- computationF // wait for the computation to finish
        // wait a bit more to ensure the auxiliary cache update would have happened
        _ = Threading.sleep(500)
        auxCacheEntry0 = keysCache.getIfPresentAuxKey(11) // first addition should not be present
        auxCacheEntry1 = keysCache.getIfPresentAuxKey(43) // newer addition should be present
        cacheEntry = keysCache.getIfPresentSync(10) // should be the newer addition
      } yield {
        auxCacheEntry0 shouldBe None
        auxCacheEntry1 shouldBe Some(10)
        cacheEntry shouldBe Some(42)
      }
    }

    "respect a newer addition for a long-running computation for an entry that has not been removed" in {

      val keysCache =
        new ScaffeineCache.TunnelledAsyncCacheWithAuxCache[Int, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          getAuxKeyO = x => Some(x + 1),
          sizeMetric = CommonMockMetrics.dbStorage.internalContractIdsCacheSize,
          tracedLogger = logger,
          context = "",
          loggerFactory = loggerFactory,
        )

      val computationF = keysCache.getFuture(10, longRunningComputation)
      keysCache.put(10, 42)
      for {
        _ <- computationF // wait for the computation to finish
        // wait a bit more to ensure the auxiliary cache update would have happened
        _ = Threading.sleep(500)
        auxCacheEntry0 = keysCache.getIfPresentAuxKey(11) // first addition should not be present
        auxCacheEntry1 = keysCache.getIfPresentAuxKey(43) // newer addition should be present
        cacheEntry = keysCache.getIfPresentSync(10) // should be the newer addition
      } yield {
        auxCacheEntry0 shouldBe None
        auxCacheEntry1 shouldBe Some(10)
        cacheEntry shouldBe Some(42)
      }
    }

    "handle two evictions of the same entry" in {

      val keysCache =
        new ScaffeineCache.TunnelledAsyncCacheWithAuxCache[Int, Int, Int](
          cache = CachingConfigs.testing.keyCache.buildScaffeine(loggerFactory),
          getAuxKeyO = x => Some(x + 1),
          sizeMetric = CommonMockMetrics.dbStorage.internalContractIdsCacheSize,
          tracedLogger = logger,
          context = "",
          loggerFactory = loggerFactory,
        )

      for {
        _ <- keysCache.getFuture(10, _ => FutureUnlessShutdown.pure(41))
        _ = eventually() {
          keysCache.getIfPresentAuxKey(42) shouldBe Some(10)
        }
        _ = keysCache.invalidate(10)
        auxCacheEntry1 = keysCache.getIfPresentAuxKey(42)
        _ = keysCache.invalidate(10)
        auxCacheEntry2 = keysCache.getIfPresentAuxKey(42)
      } yield {
        auxCacheEntry1 shouldBe None
        auxCacheEntry2 shouldBe None
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
      val cache: ScaffeineCache.TunnelledAsyncCacheImpl[Int, Int] =
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
