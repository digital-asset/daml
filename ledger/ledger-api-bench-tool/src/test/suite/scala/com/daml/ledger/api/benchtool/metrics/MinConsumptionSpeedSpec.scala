// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class MinConsumptionSpeedSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  "Min consumption speed SLO" should {
    "correctly report violation" in {
      import ConsumptionSpeedMetric.Value
      val objectiveSpeed = Random.nextDouble()
      val objective = ConsumptionSpeedMetric.MinConsumptionSpeed(objectiveSpeed)
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
