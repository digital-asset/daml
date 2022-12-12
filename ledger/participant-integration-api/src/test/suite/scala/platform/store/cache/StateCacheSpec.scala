// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.daml.caching.{CaffeineCache, ConcurrentCache, SizedCache}
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.api.MetricName
import com.daml.metrics.{CacheMetrics, Metrics}
import com.github.benmanes.caffeine.cache.Caffeine
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.math.BigInt.long2bigInt
import scala.util.Success

class StateCacheSpec extends AsyncFlatSpec with Matchers with MockitoSugar with Eventually {
  private val className = classOf[StateCache[_, _]].getSimpleName

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  override implicit def executionContext: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  private val cacheUpdateTimer = Metrics.ForTesting.timer(MetricName("cache-update"))

  behavior of s"$className.putAsync"

  it should "asynchronously store the update" in {
    val cache = mock[ConcurrentCache[String, String]]
    val someOffset = offset(0L)
    val stateCache = StateCache[String, String](
      initialCacheIndex = someOffset,
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
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

  it should "putAsync 100_000 values for the same key in 1 second" in {
    val `number of competing updates` = 100000L
    val `number of keys in cache` = 1L

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

  behavior of s"$className.put"

  it should "synchronously update the cache in front of older asynchronous updates" in {
    val cache = mock[ConcurrentCache[String, String]]
    val initialOffset = offset(0L)
    val stateCache = StateCache[String, String](
      initialCacheIndex = initialOffset,
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
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
    stateCache.putBatch(offset(2L), Map("key" -> "value", "key2" -> "value2"))
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
    val stateCache = StateCache[String, String](offset(0L), cache, cacheUpdateTimer)

    stateCache.putBatch(offset(2L), Map("key" -> "value"))
    // `Put` at a decreasing validAt
    stateCache.putBatch(offset(1L), Map("key" -> "earlier value"))
    stateCache.putBatch(offset(2L), Map("key" -> "value at same validAt"))

    verify(cache).putAll(Map("key" -> "value"))
    verifyNoMoreInteractions(cache)
    succeed
  }

  behavior of s"$className.reset"

  it should "correctly reset the state cache" in {
    val stateCache =
      new StateCache[String, String](
        initialCacheIndex = offset(1L),
        cache = SizedCache.from(
          SizedCache.Configuration(2),
          new CacheMetrics(new MetricName(Vector("test")), new MetricRegistry),
        ),
        registerUpdateTimer = cacheUpdateTimer,
      )

    val syncUpdateKey = "key"
    val asyncUpdateKey = "other_key"

    // Add eagerly an entry into the cache
    stateCache.putBatch(offset(2L), Map(syncUpdateKey -> "some initial value"))
    stateCache.get(syncUpdateKey) shouldBe Some("some initial value")

    // Register async update to the cache
    val asyncUpdatePromise = Promise[String]()
    val putAsyncF =
      stateCache.putAsync(asyncUpdateKey, Map(offset(2L) -> asyncUpdatePromise.future))

    // Reset the cache
    stateCache.reset(offset(1L))
    // Complete async update
    asyncUpdatePromise.completeWith(Future.successful("some value"))

    // Assert the cache is empty after completion of the async update
    putAsyncF.map { _ =>
      stateCache.cacheIndex shouldBe offset(1L)
      stateCache.get(syncUpdateKey) shouldBe None
      stateCache.get(asyncUpdateKey) shouldBe None
    }
  }

  private def buildStateCache(cacheSize: Long): StateCache[String, String] =
    StateCache[String, String](
      initialCacheIndex = Offset.beforeBegin,
      cache = CaffeineCache[String, String](
        Caffeine
          .newBuilder()
          .maximumSize(cacheSize),
        None,
      ),
      registerUpdateTimer = cacheUpdateTimer,
    )(scala.concurrent.ExecutionContext.global)

  private def prepare(
      `number of competing updates`: Long,
      `number of keys in cache`: Long,
  ): Seq[(String, (Promise[String], String))] = {
    for {
      i <- 1L to `number of keys in cache`
      j <- 1L to `number of competing updates`
    } yield (s"key-$i", (Promise[String](), s"value-$j"))
  }

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
        stateCache.cacheIndex = offset(cacheIdx)
        val validAt = stateCache.cacheIndex
        stateCache
          .putAsync(
            key,
            {
              case `validAt` => promise.future
              case _ => fail()
            },
          )
          .map(_ => ())(scala.concurrent.ExecutionContext.global)
      }
    }

  private def time[T](f: => T): (T, FiniteDuration) = {
    val start = System.nanoTime()
    val r = f
    val duration = FiniteDuration((System.nanoTime() - start) / 1000000L, TimeUnit.MILLISECONDS)
    (r, duration)
  }

  private def offset(idx: Long) = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
