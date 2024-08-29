// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.Member
import org.scalatest.Assertion

import scala.reflect.ClassTag

/** Utility methods to assert state of metrics.
  */
trait MetricsUtils { this: BaseTest =>

  protected val onDemandMetricsReader: OpenTelemetryOnDemandMetricsReader =
    new OpenTelemetryOnDemandMetricsReader()

  // def instead of val because some test suites might require a new metrics factory for each test
  // If that's not the case, fix the factory once in the test
  protected def metricsFactory(
      histogramInventory: HistogramInventory
  ): OpenTelemetryMetricsFactory = testableMetricsFactory(
    this.getClass.getSimpleName,
    onDemandMetricsReader,
    histogramInventory.registered().map(_.name.toString()).toSet,
  )

  def getMetricValues[TargetType <: MetricValue](name: String)(implicit
      M: ClassTag[TargetType]
  ): Seq[TargetType] =
    MetricValue
      .fromMetricData(
        onDemandMetricsReader
          .read()
          .find(_.getName.endsWith(name))
          .value
      )
      .flatMap { metricData =>
        metricData.select[TargetType]
      }

  def assertInContext(name: String, key: String, value: String): Assertion =
    clue(s"metric $name has value $value for key $key in context") {
      getMetricValues[MetricValue.LongPoint](name).headOption
        .flatMap(_.attributes.get(key)) shouldBe Some(value)
    }

  def assertNotInContext(name: String, key: String): Assertion =
    clue(s"metric $name has value $value for key $key in context") {
      getMetricValues[MetricValue.LongPoint](name).headOption
        .flatMap(_.attributes.get(key)) shouldBe empty
    }

  def assertMemberIsInContext(name: String, member: Member): Assertion =
    assertInContext(name, "member", member.toString)

  def assertLongValue(name: String, expected: Long): Assertion =
    clue(s"metric $name has value $expected") {
      getMetricValues[MetricValue.LongPoint](name).loneElement.value shouldBe expected
    }

  def assertNoValue(name: String): Assertion =
    clue(s"metric $name has no value") {
      onDemandMetricsReader
        .read()
        .exists(_.getName.endsWith(name)) shouldBe false
    }
}
