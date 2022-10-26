// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.Logger

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.codahale.metrics.{MetricRegistry, ScheduledReporter, Slf4jReporter}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.api.reporters.MetricsReporter

import scala.concurrent.Future

class MetricRegistryOwner(
    reporter: MetricsReporter,
    reportingInterval: Duration,
    logger: Logger,
) extends ResourceOwner[MetricRegistry] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[MetricRegistry] =
    for {
      registry <- ResourceOwner.forValue(() => new MetricRegistry).acquire()
      _ <- acquireSlfjReporter(registry)
      metricsReporter <- acquireMetricsReporter(registry)
      _ = metricsReporter.start(reportingInterval.toMillis, TimeUnit.MILLISECONDS)
    } yield registry

  private def acquireSlfjReporter(
      registry: MetricRegistry
  )(implicit context: ResourceContext): Resource[Slf4jReporter] =
    Resource {
      Future.successful(newSlf4jReporter(registry))
    } { reporter =>
      Future(reporter.report()) // Trigger a report to the SLF4J logger on shutdown.
        .andThen { case _ => reporter.close() } // Gracefully shut down
    }

  private def acquireMetricsReporter(registry: MetricRegistry)(implicit
      context: ResourceContext
  ): Resource[ScheduledReporter] =
    ResourceOwner.forCloseable(() => reporter.register(registry)).acquire()

  private def newSlf4jReporter(registry: MetricRegistry): Slf4jReporter =
    Slf4jReporter
      .forRegistry(registry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
      .outputTo(logger)
      .build()
}
