// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.noop.{NoOpMetricsFactory, NoOpTimer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.digitalasset.canton.caching.{CaffeineCache, ConcurrentCache, SizedCache}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.github.benmanes.caffeine.cache.Caffeine
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{FiniteDuration, *}
import scala.concurrent.{Future, Promise}
import scala.util.Success

class StateCacheSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with Eventually
    with BaseTest
    with HasExecutionContext {

  private val className = classOf[StateCache[?, ?]].getSimpleName

  private val cacheUpdateTimer = NoOpTimer(
    MetricInfo(MetricName("state_update"), "", MetricQualification.Debug)
  )

  behavior of s"$className.putAsync"

  it should "asynchronously store the update" in {
    val cache = mock[ConcurrentCache[String, String]]
    val someOffset = offset(0L)
    val stateCache = StateCache[String, String](
      initialCacheIndex = Some(someOffset),
      emptyLedgerState = "",
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
      loggerFactory = loggerFactory,
    )

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult =
      stateCache.putAsync(
        "key",
        {
          case `someOffset` => asyncUpdatePromise.future
          case _ => fail()
        },
      )
    asyncUpdatePromise.completeWith(Future.successful("value"))

    for {
      _ <- putAsyncResult
    } yield {
      verify(cache).put("key", "value")
      // Async update should not insert in the cache
      verifyNoMoreInteractions(cache)
      succeed
    }
  }

  it should "store the latest key update in face of conflicting pending updates" in {
    val `number of competing updates` = 100L
    val `number of keys in cache` = 100L

    val stateCache = buildStateCache(`number of keys in cache`)

    val insertions = prepare(`number of competing updates`, `number of keys in cache`)

    val (insertionFutures, insertionDuration) = insertTimed(stateCache)(insertions)

    insertionDuration should be < 1.second

    insertions.foreach { case (_, (promise, value)) => promise.complete(Success(value)) }

    for {
      result <- Future.sequence(insertionFutures.toVector)
    } yield {
      result should not be empty
      assertCacheElements(stateCache)(insertions, `number of competing updates`)
    }
  }

  it should "putAsync 50_000 values for the same key in 1 second" in {

    val `number of competing updates` = 50000L
    val `number of keys in cache` = 1L

    // if this test runs in CI, we might struggle due to noisy neighbors. as we can't really control
    // the environment in every case, we just retry the test a few times.
    // if the algorithm has been made slow, it will never succeed, so the test will fail
    eventually {
      val stateCache = buildStateCache(`number of keys in cache`)

      val insertions = prepare(`number of competing updates`, `number of keys in cache`)

      val (insertionFutures, insertionDuration) = insertTimed(stateCache)(insertions)

      insertionDuration should be < 1.second

      insertions.foreach { case (_, (promise, value)) => promise.complete(Success(value)) }

      for {
        result <- Future.sequence(insertionFutures.toVector)
      } yield {
        result should not be empty
        assertCacheElements(stateCache)(insertions, `number of competing updates`)
      }
    }
  }

  behavior of s"$className.put"

  it should "synchronously update the cache in front of older asynchronous updates" in {
    val cache = mock[ConcurrentCache[String, String]]
    val initialOffset = offset(0L)
    val stateCache = StateCache[String, String](
      initialCacheIndex = Some(initialOffset),
      emptyLedgerState = "",
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
      loggerFactory = loggerFactory,
    )

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult =
      stateCache.putAsync(
        "key",
        {
          case `initialOffset` => asyncUpdatePromise.future
          case _ => fail()
        },
      )
    stateCache.putBatch(
      offset(2L),
      Map("key" -> "value", "key2" -> "value2"),
    )
    asyncUpdatePromise.completeWith(Future.successful("should not update the cache"))

    for {
      _ <- putAsyncResult
    } yield {
      verify(cache).putAll(Map("key" -> "value", "key2" -> "value2"))
      // Async update with older `validAt` should not insert in the cache
      verifyNoMoreInteractions(cache)
      succeed
    }
  }

  it should "not update the cache if called with a non-increasing `validAt`" in {
    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](None, "", cache, cacheUpdateTimer, loggerFactory)

    stateCache.putBatch(offset(2L), Map("key" -> "value"))
    loggerFactory.assertLogs(
      within = {
        // `Put` at a decreasing validAt
        stateCache.putBatch(offset(1L), Map("key" -> "earlier value"))
        stateCache
          .putBatch(offset(2L), Map("key" -> "value at same validAt"))
      },
      assertions = _.warningMessage should include(
        "Ignoring incoming synchronous update at an index (1000000001) equal to or before the cache index (1000000002)"
      ),
      _.warningMessage should include(
        "Ignoring incoming synchronous update at an index (1000000002) equal to or before the cache index (1000000002)"
      ),
    )

    verify(cache).putAll(Map("key" -> "value"))
    verifyNoMoreInteractions(cache)
    succeed
  }

  behavior of s"$className.reset"

  it should "correctly reset the state cache" in {
    val stateCache =
      new StateCache[String, String](
        initialCacheIndex = Some(offset(1L)),
        emptyLedgerState = "",
        cache = SizedCache.from(
          SizedCache.Configuration(2),
          new CacheMetrics(
            new MetricName(Vector("test")),
            NoOpMetricsFactory,
          ),
        ),
        registerUpdateTimer = cacheUpdateTimer,
        loggerFactory = loggerFactory,
      )

    val syncUpdateKey = "key"
    val asyncUpdateKey = "other_key"

    // Add eagerly an entry into the cache
    stateCache.putBatch(
      offset(2L),
      Map(syncUpdateKey -> "some initial value"),
    )
    stateCache.get(syncUpdateKey) shouldBe Some("some initial value")

    // Register async update to the cache
    val asyncUpdatePromise = Promise[String]()
    val putAsyncF =
      loggerFactory.assertLogs(
        within = stateCache.putAsync(
          asyncUpdateKey,
          Map(offset(2L) -> asyncUpdatePromise.future),
        ),
        assertions = _.warningMessage should include(
          "Pending updates tracker for other_key not registered. This could be due to a transient error causing a restart in the index service."
        ),
      )

    // Reset the cache
    stateCache.reset(Some(offset(1L)))
    // Complete async update
    asyncUpdatePromise.completeWith(Future.successful("some value"))

    // Assert the cache is empty after completion of the async update
    putAsyncF.map { _ =>
      stateCache.cacheIndex shouldBe Some(offset(1L))
      stateCache.get(syncUpdateKey) shouldBe None
      stateCache.get(asyncUpdateKey) shouldBe None
    }
  }

  private def buildStateCache(cacheSize: Long): StateCache[String, String] =
    StateCache[String, String](
      initialCacheIndex = None,
      emptyLedgerState = "",
      cache = CaffeineCache[String, String](
        Caffeine
          .newBuilder()
          .maximumSize(cacheSize),
        None,
      ),
      registerUpdateTimer = cacheUpdateTimer,
      loggerFactory = loggerFactory,
    )

  private def prepare(
      `number of competing updates`: Long,
      `number of keys in cache`: Long,
  ): Seq[(String, (Promise[String], String))] =
    for {
      i <- 1L to `number of keys in cache`
      j <- 1L to `number of competing updates`
    } yield (s"key-$i", (Promise[String](), s"value-$j"))

  private def assertCacheElements(stateCache: StateCache[String, String])(
      insertions: Seq[(String, (Promise[String], String))],
      numberOfCompetingUpdates: Long,
  ): Assertion = {
    insertions
      .map(_._1)
      .toSet
      .foreach((key: String) =>
        stateCache
          .get(key)
          .getOrElse(s"Missing $key") shouldBe s"value-$numberOfCompetingUpdates"
      )
    stateCache.pendingUpdates shouldBe empty
  }

  private def insertTimed(stateCache: StateCache[String, String])(
      insertions: Seq[(String, (Promise[String], String))]
  ): (Seq[Future[Unit]], FiniteDuration) =
    time {
      var cacheIdx = 0L
      insertions.map { case (key, (promise, _)) =>
        cacheIdx += 1L
        val validAt = offset(cacheIdx)
        stateCache.cacheIndex = Some(validAt)
        stateCache
          .putAsync(
            key,
            {
              case `validAt` => promise.future
              case _ => fail()
            },
          )
          .map(_ => ())
      }
    }

  private def time[T](f: => T): (T, FiniteDuration) = {
    val start = System.nanoTime()
    val r = f
    val duration = FiniteDuration((System.nanoTime() - start) / 1000000L, TimeUnit.MILLISECONDS)
    (r, duration)
  }

  private def offset(idx: Long): Offset = {
    val base = 1000000000L
    Offset.tryFromLong(base + idx)
  }
}
