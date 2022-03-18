// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.TimeUnit
import com.codahale.metrics.MetricRegistry
import com.daml.caching.{CaffeineCache, ConcurrentCache}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.StateCache.PendingUpdatesState
import com.github.benmanes.caffeine.cache.Caffeine
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

class StateCacheSpec extends AsyncFlatSpec with Matchers with MockitoSugar with Eventually {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  override implicit def executionContext: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  private val cacheUpdateTimer = new Metrics(
    new MetricRegistry
  ).daml.execution.cache.registerCacheUpdate

  behavior of s"${classOf[StateCache[_, _]].getSimpleName}.putAsync"

  it should "asynchronously store the update" in {
    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](
      initialCacheIndex = 0L,
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
    )

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult = stateCache.putAsync("key", { case 0L => asyncUpdatePromise.future })
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

  behavior of s"${classOf[StateCache[_, _]].getSimpleName}.put"

  it should "synchronously update the cache in front of older asynchronous updates" in {
    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](
      initialCacheIndex = 0L,
      cache = cache,
      registerUpdateTimer = cacheUpdateTimer,
    )

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult = stateCache.putAsync("key", { case 0L => asyncUpdatePromise.future })
    stateCache.put("key", 2L, "value")
    asyncUpdatePromise.completeWith(Future.successful("should not update the cache"))

    for {
      _ <- putAsyncResult
    } yield {
      verify(cache).put("key", "value")
      // Async update with older `validAt` should not insert in the cache
      verifyNoMoreInteractions(cache)
      succeed
    }
  }

  it should "not update the cache, invalidate the key and abort pending updates if called with a decreasing `validAt`" in {
    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](0L, cache, cacheUpdateTimer)

    val asyncUpdatePromise = Promise[String]()
    stateCache.put("key", 2L, "value")
    val asyncPutResult = stateCache.putAsync("key", { _ => asyncUpdatePromise.future })
    val pendingUpdatesKeyAfterAsyncPut = stateCache.pendingUpdates.get("key")
    // `Put` at a decreasing validAt
    stateCache.put("key", 1L, "earlier value")
    val pendingUpdatesKeyEntryAfterFaultyPut = stateCache.pendingUpdates.get("key")
    // Async update completes but should not make it in the cache
    asyncUpdatePromise.completeWith(Future.successful("async put value"))

    for {
      _ <- asyncPutResult
    } yield {
      pendingUpdatesKeyAfterAsyncPut shouldBe Some(PendingUpdatesState(1L, 2L))
      pendingUpdatesKeyEntryAfterFaultyPut shouldBe None
      verify(cache).put("key", "value")
      verify(cache).invalidate("key")
      verifyNoMoreInteractions(cache)
      succeed
    }
  }

  private def buildStateCache(cacheSize: Long): StateCache[String, String] =
    StateCache[String, String](
      initialCacheIndex = 0L,
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
      insertions.map { case (key, (promise, _)) =>
        stateCache._cacheIndex += 1
        val validAt = stateCache._cacheIndex
        stateCache
          .putAsync(key, { case `validAt` => promise.future })
          .map(_ => ())(scala.concurrent.ExecutionContext.global)
      }
    }

  private def time[T](f: => T): (T, FiniteDuration) = {
    val start = System.nanoTime()
    val r = f
    val duration = FiniteDuration((System.nanoTime() - start) / 1000000L, TimeUnit.MILLISECONDS)
    (r, duration)
  }
}
