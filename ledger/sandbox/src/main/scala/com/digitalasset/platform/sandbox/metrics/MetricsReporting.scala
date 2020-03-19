// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricRegistry, Slf4jReporter}
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

/** Manages metrics and reporters.
  *
  * Creates two reporters:
  *
  *   - A JmxReporter, which exposes metrics over JMX
  *   - An Slf4jReporter, which logs metrics on shutdown at DEBUG level
  *
  * Note that metrics are in general light-weight and add negligible overhead.
  * They are not visible to everyday users so they can be safely enabled all the time.
  */
final class MetricsReporting(jmxDomain: String) extends ResourceOwner[MetricRegistry] {
  def acquire()(implicit executionContext: ExecutionContext): Resource[MetricRegistry] = {
    val registry = new MetricRegistry
    for {
      jmxReporter <- ResourceOwner.forCloseable(() => newJmxReporter(registry)).acquire()
      slf4JReporter <- ResourceOwner.forCloseable(() => newSlf4jReporter(registry)).acquire()
      // Trigger a report to the SLF4J logger on shutdown.
      _ <- Resource(Future.successful(slf4JReporter))(reporter =>
        Future.successful(reporter.report()))
    } yield {
      jmxReporter.start()
      registry
    }
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
}
