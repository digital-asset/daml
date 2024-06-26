// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.Member
import org.scalatest.Assertion

import scala.reflect.ClassTag

/** Utility methods to assert state of metrics.
  */
trait MetricsUtils { this: BaseTest =>

  def getMetricValues[TargetType <: MetricValue](name: String)(implicit
      M: ClassTag[TargetType],
      onDemandMetricsReader: OnDemandMetricsReader,
  ): Seq[TargetType] = {
    MetricValue
      .fromMetricData(
        onDemandMetricsReader
          .read()
          .find(_.getName.contains(name))
          .value
      )
      .flatMap { metricData =>
        metricData.select[TargetType]
      }
  }

  def assertInContext(name: String, key: String, value: String)(implicit
      onDemandMetricsReader: OnDemandMetricsReader
  ): Assertion = {
    getMetricValues[MetricValue.LongPoint](name).headOption
      .flatMap(_.attributes.get(key)) shouldBe Some(value)
  }

  def assertSenderIsInContext(name: String, sender: Member)(implicit
      onDemandMetricsReader: OnDemandMetricsReader
  ): Assertion = {
    assertInContext(name, "sender", sender.toString)
  }

  def assertLongValue(name: String, expected: Long)(implicit
      onDemandMetricsReader: OnDemandMetricsReader
  ): Assertion = {
    getMetricValues[MetricValue.LongPoint](name).loneElement.value shouldBe expected
  }

}
