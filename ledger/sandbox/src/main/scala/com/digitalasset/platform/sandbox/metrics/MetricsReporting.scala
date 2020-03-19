// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{ConsoleReporter, CsvReporter, MetricRegistry, Slf4jReporter}
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

/** Manages metrics and reporters.
  *
  * Creates at least two reporters:
  *
  *   - A JmxReporter, which exposes metrics over JMX
  *   - An Slf4jReporter, which logs metrics on shutdown at DEBUG level
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
    for {
      slf4JReporter <- ResourceOwner.forCloseable(() => newSlf4jReporter(registry)).acquire()
      _ <- ResourceOwner
        .forCloseable(() => newJmxReporter(registry))
        .acquire()
        .map(_.start())
      _ <- extraMetricsReporter.fold(Resource.unit) {
        case MetricsReporter.ConsoleReporter =>
          ResourceOwner
            .forCloseable(() => newConsoleReporter(registry))
            .acquire()
            .map(_.start(extraMetricsReportingInterval.getSeconds, TimeUnit.SECONDS))
        case MetricsReporter.CsvReporter(directory) =>
          ResourceOwner
            .forCloseable(() => newCsvReporter(registry, directory))
            .acquire()
            .map(_.start(extraMetricsReportingInterval.getSeconds, TimeUnit.SECONDS))
      }
      // Trigger a report to the SLF4J logger on shutdown.
      _ <- Resource(Future.successful(slf4JReporter))(reporter =>
        Future.successful(reporter.report()))
    } yield {
      registry
    }
  }

  private def newConsoleReporter(registry: MetricRegistry): ConsoleReporter =
    ConsoleReporter
      .forRegistry(registry)
      .build()

  private def newCsvReporter(registry: MetricRegistry, directory: Path): CsvReporter =
    CsvReporter
      .forRegistry(registry)
      .build(directory.toFile)

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
}
