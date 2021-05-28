// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.objectives.{MaxDelay, MinConsumptionSpeed}
import com.daml.ledger.api.benchtool.metrics.{ConsumptionSpeedMetric, DelayMetric}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class ServiceLevelObjectiveSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  "Maximum delay SLO" should {
    "correctly report violation" in {
      import DelayMetric.Value
      val randomValue = Random.nextInt(10000).toLong
      val randomSmaller = randomValue - 1
      val randomLarger = randomValue + 1
      val maxDelay = MaxDelay(randomValue)
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

  "Min consumption speed SLO" should {
    "correctly report violation" in {
      import ConsumptionSpeedMetric.Value
      val objectiveSpeed = Random.nextDouble()
      val objective = MinConsumptionSpeed(objectiveSpeed)
      val lowerSpeed = objectiveSpeed - 1.0
      val higherSpeed = objectiveSpeed + 1.0
      val cases = Table(
        ("Metric value", "Expected violated"),
        (Value(None), true),
        (Value(Some(lowerSpeed)), true),
        (Value(Some(objectiveSpeed)), false),
        (Value(Some(higherSpeed)), false),
      )

      forAll(cases) { (metricValue, expectedViolated) =>
        objective.isViolatedBy(metricValue) shouldBe expectedViolated
      }
    }
  }
}
