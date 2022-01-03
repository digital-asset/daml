// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.codahale.metrics.{ScheduledReporter, Slf4jReporter}
import com.daml.resources._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

// We don't need access to the underlying resource, we use
// the owner only to manage the reporter's life cycle
sealed abstract class MetricsReporterOwner[Context: HasExecutionContext]
    extends AbstractResourceOwner[Context, Unit]

object MetricsReporterOwner {

  def slf4j[Context: HasExecutionContext](
      period: FiniteDuration
  ): MetricsReporterOwner[Context] =
    new Scheduled(period, Slf4jReporter.forRegistry(Metrics.Registry).build())

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
