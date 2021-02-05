// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.codahale.metrics.{MetricRegistry, ScheduledReporter, Slf4jReporter}
import com.daml.resources._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

// We don't need access to the underlying resource, we use
// the owner only to manage the reporter's life cycle
sealed abstract class MetricsReporterOwner[Context: HasExecutionContext]
    extends AbstractResourceOwner[Context, Unit]

object MetricsReporterOwner {

  def noop[Context: HasExecutionContext](registry: MetricRegistry): MetricsReporterOwner[Context] =
    new Noop(registry)

  def slf4j[Context: HasExecutionContext](period: FiniteDuration)(
      registry: MetricRegistry
  ): MetricsReporterOwner[Context] =
    new Scheduled(period, Slf4jReporter.forRegistry(registry).build())

  private final class Noop[Context: HasExecutionContext](val _ignored: MetricRegistry)
      extends MetricsReporterOwner[Context] {
    override def acquire()(implicit context: Context): Resource[Context, Unit] =
      PureResource(Future.successful(()))
  }

  private final class Scheduled[Context: HasExecutionContext, Delegate <: ScheduledReporter](
      period: FiniteDuration,
      reporter: Delegate,
  ) extends MetricsReporterOwner[Context] {
    override def acquire()(implicit context: Context): Resource[Context, Unit] = {
      ReleasableResource(Future(reporter.start(period.length, period.unit)))(_ =>
        Future(reporter.stop())
      )
    }
  }

}
