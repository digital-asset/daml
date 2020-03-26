// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.jvm.{
  ClassLoadingGaugeSet,
  GarbageCollectorMetricSet,
  JvmAttributeGaugeSet,
  MemoryUsageGaugeSet,
  ThreadStatesGaugeSet
}
import com.codahale.metrics.{
  ConsoleReporter,
  CsvReporter,
  MetricRegistry,
  Reporter,
  ScheduledReporter,
  Slf4jReporter
}
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

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
) extends ResourceOwner[MetricRegistry] {
  def acquire()(implicit executionContext: ExecutionContext): Resource[MetricRegistry] = {
    val registry = new MetricRegistry
    registry.registerAll("jvm.class_loader", new ClassLoadingGaugeSet)
    registry.registerAll("jvm.garbage_collector", new GarbageCollectorMetricSet)
    registry.registerAll("jvm.attributes", new JvmAttributeGaugeSet)
    registry.registerAll("jvm.memory_usage", new MemoryUsageGaugeSet)
    registry.registerAll("jvm.thread_states", new ThreadStatesGaugeSet)
    for {
      slf4JReporter <- acquire(newSlf4jReporter(registry))
      _ <- acquire(newJmxReporter(registry))
        .map(_.start())
      _ <- extraMetricsReporter.fold(Resource.unit) { reporter =>
        acquire(newReporter(reporter, registry))
          .map(_.start(extraMetricsReportingInterval.getSeconds, TimeUnit.SECONDS))
      }
      // Trigger a report to the SLF4J logger on shutdown.
      _ <- Resource(Future.successful(slf4JReporter))(reporter =>
        Future.successful(reporter.report()))
    } yield registry
  }

  private def newReporter(reporter: MetricsReporter, registry: MetricRegistry)(
      implicit executionContext: ExecutionContext
  ): ScheduledReporter = reporter match {
    case MetricsReporter.Console =>
      ConsoleReporter
        .forRegistry(registry)
        .build()
    case MetricsReporter.Csv(directory) =>
      CsvReporter
        .forRegistry(registry)
        .build(directory.toFile)
    case MetricsReporter.Graphite(address) =>
      GraphiteReporter
        .forRegistry(registry)
        .build(new Graphite(address))
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

  private def acquire[T <: Reporter](reporter: => T)(
      implicit executionContext: ExecutionContext
  ): Resource[T] =
    ResourceOwner
      .forCloseable(() => reporter)
      .acquire()
}
