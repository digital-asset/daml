// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.metrics.api.reporters

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricRegistry, Reporter, Slf4jReporter}
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.metrics.{JvmMetricSet, OpenTelemetryMeterOwner}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Manages metrics and reporters.
  *
  * Creates the [[MetricRegistry]].
  *
  * All out-of-the-box JVM metrics are added to the registry.
  *
  * Creates at least two reporters:
  *
  *   - a [[JmxReporter]], which exposes metrics over JMX, and
  *   - an [[Slf4jReporter]], which logs metrics on shutdown at DEBUG level.
  *
  * Also optionally creates the reporter specified in the constructor.
  *
  * Note that metrics are in general light-weight and add negligible overhead.
  * They are not visible to everyday users so they can be safely enabled all the time.
  */
final class MetricsReporting(
    jmxDomain: String,
    extraMetricsReporter: Option[MetricsReporter],
    extraMetricsReportingInterval: Duration,
) extends ResourceOwner[HttpJsonApiMetrics] {
  def acquire()(implicit context: ResourceContext): Resource[HttpJsonApiMetrics] = {
    val registry = new MetricRegistry
    registry.registerAll(new JvmMetricSet)
    for {
      openTelemetryMeter <- OpenTelemetryMeterOwner(enabled = true, extraMetricsReporter).acquire()
      slf4JReporter <- acquire(newSlf4jReporter(registry))
      _ <- acquire(newJmxReporter(registry))
        .map(_.start())
      _ <- extraMetricsReporter.fold(Resource.unit) { reporter =>
        acquire(reporter.register(registry))
          .map(_.start(extraMetricsReportingInterval.toSeconds, TimeUnit.SECONDS))
      }
      // Trigger a report to the SLF4J logger on shutdown.
      _ <- Resource(Future.successful(slf4JReporter))(reporter =>
        Future.successful(reporter.report())
      )
    } yield new HttpJsonApiMetrics(registry, new OpenTelemetryFactory(openTelemetryMeter))
  }

  private def newJmxReporter(registry: MetricRegistry): JmxReporter =
    JmxReporter
      .forRegistry(registry)
      .inDomain(jmxDomain)
      .build()

  private def newSlf4jReporter(registry: MetricRegistry): Slf4jReporter =
    Slf4jReporter
      .forRegistry(registry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .withLoggingLevel(LoggingLevel.DEBUG)
      .build()

  private def acquire[T <: Reporter](reporter: => T)(implicit
      context: ResourceContext
  ): Resource[T] =
    ResourceOwner.forCloseable(() => reporter).acquire()
}
