// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class MaxDelaySpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  "Maximum delay SLO" should {
    "correctly report violation" in {
      import DelayMetric.Value
      val randomValue = Random.nextInt(10000).toLong
      val randomSmaller = randomValue - 1
      val randomLarger = randomValue + 1
      val maxDelay = DelayMetric.MaxDelay(randomValue)
      val cases = Table(
        ("Metric value", "Expected violated"),
        (Value(None), false),
        (Value(Some(randomSmaller)), false),
        (Value(Some(randomValue)), false),
        (Value(Some(randomLarger)), true),
      )

      forAll(cases) { (metricValue, expectedViolated) =>
        maxDelay.isViolatedBy(metricValue) shouldBe expectedViolated
      }
    }

    "correctly pick a value more violating requirements" in {
      import DelayMetric.Value
      val randomNumber = Random.nextInt(10).toLong
      val higherNumber = randomNumber + 1
      val cases = Table(
        ("first", "second", "expected result"),
        (Value(Some(randomNumber)), Value(Some(higherNumber)), Value(Some(higherNumber))),
        (Value(Some(higherNumber)), Value(Some(randomNumber)), Value(Some(higherNumber))),
        (Value(Some(randomNumber)), Value(None), Value(Some(randomNumber))),
        (Value(None), Value(Some(randomNumber)), Value(Some(randomNumber))),
        (Value(None), Value(None), Value(None)),
      )

      forAll(cases) { (first, second, expected) =>
        Ordering[Value].max(first, second) shouldBe expected
      }
    }
  }
}
