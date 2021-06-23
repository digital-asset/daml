// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.{MetricRegistry, Reporter, Slf4jReporter}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.MetricsReporter
import com.daml.resources

import scala.concurrent.Future

class MetricRegistryOwner(
    reporter: MetricsReporter,
    reportingInterval: Duration,
) extends ResourceOwner[MetricRegistry] {
  override def acquire()(implicit
      context: ResourceContext
  ): resources.Resource[ResourceContext, MetricRegistry] = {
    val registry = new MetricRegistry
    for {
      slf4JReporter <- acquire(newSlf4jReporter(registry))
      _ <- acquire(reporter.register(registry))
        .map(_.start(reportingInterval.toSeconds, TimeUnit.SECONDS))
      // Trigger a report to the SLF4J logger on shutdown.
      _ <- Resource(Future.successful(slf4JReporter))(reporter =>
        Future.successful(reporter.report())
      )
    } yield registry
  }

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
