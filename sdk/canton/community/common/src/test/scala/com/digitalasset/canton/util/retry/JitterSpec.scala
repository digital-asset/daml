// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import com.google.common.primitives.UnsignedInteger
import org.scalatest.funspec.AnyFunSpec

import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class JitterSpec extends AnyFunSpec {
  // val rng = new SecureRandom()
  val rng = new java.util.Random()
  val rand = Jitter.randomSource(rng)
  val cap = 2000 milliseconds

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def testJitter(jitter: Jitter)(): Unit = {
    val min = rand(1, 100) milliseconds
    var sleep = rand(1, 1000) milliseconds

    for (i <- 0 until 10000) {
      val delay = sleep
      sleep = jitter(delay, min, i + 1)
      assert(sleep.unit === TimeUnit.MILLISECONDS)
      assert(sleep.length >= 0)
      assert(sleep.length <= cap.length)
    }
  }

  describe("retry.Defaults.random") {
    it("should return sane random values") {
      for (i <- 0 until 1000) {
        val result = rand(0, 10)
        assert(result >= 0)
        assert(result <= 10)
      }
    }

    it("should handle swapped bounds") {
      for (i <- 0 until 1000) {
        val result = rand(10, 0)
        assert(result >= 0)
        assert(result <= 10)
      }
    }

    it("should not cache random values") {
      val counter = new AtomicInteger()
      val rng = new Random() {
        override def nextInt(): Int =
          counter.addAndGet(1)
        override def nextInt(n: Int): Int =
          counter.addAndGet(1)
      }

      val rand = Jitter.randomSource(rng)
      rand(0, 100)
      // intentionally using the guava UnsignedInteger.MAX_VALUE over Int.MaxValue
      rand(0, UnsignedInteger.MAX_VALUE.longValue() + 1L)
      assert(counter.get() === 4)
    }

    it("should handle bounds greater than Int.MaxValue correctly") {
      // this would previously throw an IllegalArgumentException
      rand(0, Int.MaxValue.toLong + 1L)
    }
  }

  describe("retry.Jitter.none") {
    it("should perform backoff correctly") {
      testJitter(Jitter.none(cap))()
    }
  }

  describe("retry.Jitter.decorrelated") {
    it("should perform decorrelated jitter correctly") {
      testJitter(Jitter.decorrelated(cap))()
    }
  }

  describe("retry.Jitter.full") {
    it("should perform full jitter correctly") {
      testJitter(Jitter.full(cap))()
    }
  }

  describe("retry.Jitter.equal") {
    it("should perform equal jitter correctly") {
      testJitter(Jitter.equal(cap))()
    }
  }
}
