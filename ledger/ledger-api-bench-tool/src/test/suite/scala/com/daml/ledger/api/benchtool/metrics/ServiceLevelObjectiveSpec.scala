// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.Metric.DelayMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class ServiceLevelObjectiveSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  "Maximum delay SLO" should {
    "correctly report violation" in {
      val randomValue = Random.nextLong(10000)
      val randomSmaller = Random.nextLong(randomValue)
      val randomLarger = randomValue + Random.nextLong(10000)
      val maxDelay = DelayMetric.MaxDelay(randomValue)
      val cases = Table(
        ("Metric value", "Expected violated"),
        (DelayMetric.Value(None), false),
        (DelayMetric.Value(Some(randomSmaller)), false),
        (DelayMetric.Value(Some(randomValue)), false),
        (DelayMetric.Value(Some(randomLarger)), true),
      )

      forAll(cases) { (metricValue, expectedViolated) =>
        maxDelay.isViolatedBy(metricValue) shouldBe expectedViolated
      }
    }
  }
}
