// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.TimeoutException

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricsContext, MetricName => MN}

import scala.concurrent.{Await, ExecutionContext, Future}

class HealthMetrics(val factory: LabeledMetricsFactory) {
  import HealthMetrics._

  def registerHealthGauge(componentName: String, supplier: () => Boolean): Unit = {
    val asLong = () => if (supplier()) 1L else 0L
    factory.gaugeWithSupplier(MetricName, asLong, "The status of the Daml components")(
      MetricsContext((ComponentLabel, componentName))
    )
  }

  def registerHealthGauge(componentName: String, supplier: () => Future[Boolean])(implicit
      executionContext: ExecutionContext
  ): Unit = {
    registerHealthGauge(
      componentName,
      // gaugeWithSupplier underlying code requires the value to be provided in a finite amount of time
      // If the future has not been completed after a short timeout, force return false.
      (() => {
        try {
          Await.result(supplier().fallbackTo(Future(false)), FutureSupplierTimeout)
        } catch {
          case _: TimeoutException =>
            false
        }
      }),
    )
  }
}

object HealthMetrics {

  import scala.concurrent.duration._

  final val MetricName = MN.Daml :+ "health" :+ "status"

  final val ComponentLabel = "component"

  final val FutureSupplierTimeout = Duration(100, MILLISECONDS)
}
