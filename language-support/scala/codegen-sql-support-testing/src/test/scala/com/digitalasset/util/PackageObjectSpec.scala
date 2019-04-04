// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.control.NonFatal

final class PackageObjectSpec extends FlatSpec with Matchers {

  behavior of "retryingAfter"

  it should "try at least once even without retries" in {
    retryingAfter()(() => 42) shouldEqual 42
  }

  it should "fail if the function fail more than once and there are no retries" in {
    val process = failNTimesAndThenReturn(1, 42)
    an[Exception] shouldBe thrownBy {
      retryingAfter()(() => process.next()()) shouldEqual 42
    }
  }

  it should "succeed with 3 failure and 3 retries" in {
    val process = failNTimesAndThenReturn(3, 42)
    retryingAfter(0.millis, 0.millis, 0.millis)(() => process.next()()) shouldEqual 42
  }

  it should "fail with 4 failures and 3 retries" in {
    val process = failNTimesAndThenReturn(4, 42)
    an[Exception] shouldBe thrownBy {
      retryingAfter(0.millis, 0.millis, 0.millis)(() => process.next()()) shouldEqual 42
    }
  }

  it should "wait at least the expected amount of time" in {
    val process = Iterator.continually(() => throw new RuntimeException)
    val start = System.nanoTime()
    try {
      retryingAfter(200.millis, 200.millis, 200.millis)(() => process.next()())
    } catch {
      case NonFatal(_) => // simply ignore
    } finally {
      val end = System.nanoTime()
      val gapMillis = (end - start) / 1000000
      assert(gapMillis >= 600)
      ()
    }
  }

  it should "not retry a successful process" in {
    var n = 0
    retryingAfter(0.millis, 0.millis, 0.millis)(() => n += 1)
    n shouldEqual 1
  }

  private def failNTimesAndThenReturn[A](n: Int, a: A): Iterator[() => A] =
    Iterator.tabulate(n)(c => () => throw new RuntimeException(s"failure #${c + 1}")) ++ Iterator
      .single(() => a)

}
