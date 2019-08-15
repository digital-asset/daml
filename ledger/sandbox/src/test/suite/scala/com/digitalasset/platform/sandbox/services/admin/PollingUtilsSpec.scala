// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

final class PollingUtilsSpec extends AsyncFlatSpec with Matchers {

  private[this] val actorSystem = ActorSystem("PollingUtilsSpec")
  private[this] val materializer = ActorMaterializer()(actorSystem)
  private[this] val scheduler = materializer.system.scheduler

  behavior of "pollUntilPersisted"

  it should "succeed with 1 attempt if there are no failures" in {
    PollingUtils.pollUntilPersisted(() => Future.successful(42))(
      _ == 42,
      "",
      0.millis,
      0.millis,
      identity,
      scheduler) map { attempts =>
      assert(attempts == 1)
    }
  }

  it should "succeed with 4 attempts after 3 failures" in {
    val process = failNTimesAndThenReturn(3, 42)
    PollingUtils.pollUntilPersisted(() => process.next()())(
      _ == 42,
      "",
      0.millis,
      0.millis,
      identity,
      scheduler) map { attempts =>
      assert(attempts == 4)
    }
  }

  it should "succeed with 4 attempts after 3 misses" in {
    val process = returnNTimesAndThenReturn(3, 41, 42)
    PollingUtils.pollUntilPersisted(() => process.next()())(
      _ == 42,
      "",
      0.millis,
      0.millis,
      identity,
      scheduler) map { attempts =>
      assert(attempts == 4)
    }
  }

  it should "respect minimal wait time" in {
    // There should be at least 100ms between attempts
    val process = returnUntil(950L, 41, 42)
    PollingUtils.pollUntilPersisted(process)(
      _ == 42,
      "",
      100.millis,
      100.millis,
      identity,
      scheduler) map { attempts =>
      assert(attempts < 11)
    }
  }

  it should "respect exponential backoff" in {
    // Exponential backoff
    // Delays are: 100ms, 200ms, 400ms, 800ms
    // Attempts are at: 0ms, 100ms, 300ms, 700ms, 1500ms
    val process = returnUntil(750L, 41, 42)
    PollingUtils.pollUntilPersisted(process)(
      _ == 42,
      "",
      100.millis,
      1000.millis,
      d => d * 2,
      scheduler) map { attempts =>
      assert(attempts < 6)
    }
  }

  /** An iterator that returns n failed futures, and then a successful future */
  private def failNTimesAndThenReturn[A](n: Int, a: A): Iterator[() => Future[A]] =
    Iterator.tabulate(n)(c => () => Future.failed(new RuntimeException(s"failure #${c + 1}"))) ++ Iterator
      .single(() => Future.successful(a))

  /** An iterator that returns n failed futures, and then a successful future */
  private def returnNTimesAndThenReturn[A](n: Int, a1: A, a2: A): Iterator[() => Future[A]] =
    Iterator.tabulate(n)(_ => () => Future.successful(a1)) ++ Iterator.single(() =>
      Future.successful(a2))

  /** An iterator that continuously emits a1 until the given delay (from calling this function) has passed, then emits a2 */
  private def returnUntil[A](millis: Long, a1: A, a2: A): () => Future[A] = {
    val start = System.nanoTime()
    () =>
      {
        val millisPassed = (System.nanoTime() - start) / 1000000L
        Future.successful(if (millisPassed < millis) a1 else a2)
      }
  }

}
