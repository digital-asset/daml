// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.scheduler.JobScheduler.*

import scala.concurrent.{ExecutionContext, Future}

trait UpdatePruningMetric {

  def withUpdatePruningMetric(
      schedule: IndividualSchedule,
      updateMetric: => Future[Unit],
  )(
      code: PruningCronSchedule => Future[ScheduledRunResult]
  )(implicit
      executionContext: ExecutionContext
  ): Future[ScheduledRunResult] =
    schedule match {
      case _: IntervalSchedule => updateMetric.map(_ => Done)
      case ps: PruningCronSchedule => code(ps)
    }

  def withUpdatePruningMetric(
      schedule: IndividualSchedule,
      updateMetric: => FutureUnlessShutdown[Unit],
  )(
      code: PruningCronSchedule => FutureUnlessShutdown[ScheduledRunResult]
  )(implicit
      executionContext: ExecutionContext
  ): FutureUnlessShutdown[ScheduledRunResult] =
    schedule match {
      case _: IntervalSchedule => updateMetric.map(_ => Done)
      case ps: PruningCronSchedule => code(ps)
    }
}
