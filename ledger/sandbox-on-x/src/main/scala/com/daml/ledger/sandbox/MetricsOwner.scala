// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.{JvmMetricSet, Metrics}
import com.daml.platform.config.MetricsConfig
import com.daml.platform.config.MetricsConfig.MetricRegistryType
import io.opentelemetry.api.metrics.Meter

import scala.concurrent.Future

case class MetricsOwner(meter: Meter, config: MetricsConfig, name: String)
    extends ResourceOwner[Metrics] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[Metrics] = {
    val metricRegistry = config.registryType match {
      case MetricRegistryType.JvmShared =>
        SharedMetricRegistries.getOrCreate(name)
      case MetricRegistryType.New =>
        new MetricRegistry
    }
    val reporter = Option.when(config.enabled) {
      val runningReporter = config.reporter
        .register(metricRegistry)
      runningReporter.start(config.reportingInterval.toMillis, TimeUnit.MILLISECONDS)
      runningReporter
    }

    metricRegistry.registerAll(new JvmMetricSet)
    JvmMetricSet.registerObservers()

    Resource(
      Future(
        Metrics(
          metricRegistry,
          meter,
        )
      )
    ) { _ =>
      Future {
        reporter.foreach(_.close())
      }
    }
  }
}
