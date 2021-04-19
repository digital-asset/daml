// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.daml.caching.{CaffeineCache, ConcurrentCache}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.github.benmanes.caffeine.cache.Caffeine
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

class StateCacheSpec extends AnyFlatSpec with Matchers with MockitoSugar with Eventually {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val cacheUpdateTimer = new Metrics(
    new MetricRegistry
  ).daml.execution.cache.registerCacheUpdate

  behavior of s"${classOf[StateCache[_, _]].getSimpleName}.putAsync"

  it should "asynchronously store the update" in {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](cache, cacheUpdateTimer)

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult = stateCache.putAsync("key", 1L, asyncUpdatePromise.future)
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
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    val `number of competing updates` = 100L
    val `number of keys in cache` = 100L

    val stateCache = buildStateCache(`number of keys in cache`)

    val insertions = prepare(`number of competing updates`, `number of keys in cache`)

    val (insertionFutures, insertionDuration) = insertTimed(stateCache)(insertions)

    insertionDuration should be < 1.second

    insertions.foreach { case (_, (promise, value, _)) =>
      promise.complete(Success(value))
    }

    for {
      result <- Future.sequence(insertionFutures.toVector)
    } yield {
      result should not be empty
      assertCacheElements(stateCache)(insertions, `number of competing updates`)
    }
  }

  it should "putAsync 100_000 values for the same key in 1 second" in {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    val `number of competing updates` = 100000L
    val `number of keys in cache` = 1L

    val stateCache = buildStateCache(`number of keys in cache`)

    val insertions = prepare(`number of competing updates`, `number of keys in cache`)

    val (insertionFutures, insertionDuration) = insertTimed(stateCache)(insertions)

    insertionDuration should be < 1.second

    insertions.foreach { case (_, (promise, value, _)) =>
      promise.complete(Success(value))
    }

    for {
      result <- Future.sequence(insertionFutures.toVector)
    } yield {
      result should not be empty
      assertCacheElements(stateCache)(insertions, `number of competing updates`)
    }
  }

  behavior of s"${classOf[StateCache[_, _]].getSimpleName}.put"

  it should "synchronously update the cache in front of older asynchronous updates" in {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](cache, cacheUpdateTimer)

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult = stateCache.putAsync("key", 1L, asyncUpdatePromise.future)
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

  it should "not update the cache if the update is older than other competing updates" in {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    val cache = mock[ConcurrentCache[String, String]]
    val stateCache = StateCache[String, String](cache, cacheUpdateTimer)

    val asyncUpdatePromise = Promise[String]()
    val putAsyncResult = stateCache.putAsync("key", 2L, asyncUpdatePromise.future)
    stateCache.put("key", 1L, "should not update the cache")
    asyncUpdatePromise.completeWith(Future.successful("value"))

    for {
      _ <- putAsyncResult
    } yield {
      verify(cache).put("key", "value")
      // Synchronous update should not insert in the cache
      verifyNoMoreInteractions(cache)
      succeed
    }
  }

  private def buildStateCache(cacheSize: Long): StateCache[String, String] =
    StateCache[String, String](
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
  ): Seq[(String, (Promise[String], String, Long))] = {
    for {
      i <- 1L to `number of keys in cache`
      j <- 1L to `number of competing updates`
    } yield (s"key-$i", (Promise[String](), s"value-$j", j))
  }

  private def assertCacheElements(stateCache: StateCache[String, String])(
      insertions: Seq[(String, (Promise[String], String, Long))],
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
      insertions: Seq[(String, (Promise[String], String, Long))]
  ): (Seq[Future[Unit]], FiniteDuration) =
    time {
      insertions.map { case (key, (promise, _, validAt)) =>
        stateCache.putAsync(key, validAt, promise.future)
      }
    }

  private def time[T](f: => T): (T, FiniteDuration) = {
    val start = System.nanoTime()
    val r = f
    val duration = FiniteDuration((System.nanoTime() - start) / 1000000L, TimeUnit.MILLISECONDS)
    (r, duration)
  }
}
