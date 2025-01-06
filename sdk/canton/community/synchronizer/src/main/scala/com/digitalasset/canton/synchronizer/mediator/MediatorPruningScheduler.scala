// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.*
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.synchronizer.mediator.Mediator.PruningError.NoDataAvailableForPruning
import com.digitalasset.canton.time.{Clock, PositiveSeconds}
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class MediatorPruningScheduler(
    clock: Clock,
    mediator: Mediator,
    storage: Storage, // storage to build the pruning scheduler store that tracks the current schedule
    config: MediatorPruningConfig,
    override val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends JobScheduler(
      "pruning",
      timeouts,
      loggerFactory,
    )
    with HasCloseContext
    with PruningScheduler
    with HasPruningSchedulerStore
    with UpdatePruningMetric {

  override protected val pruningSchedulerStore: PruningSchedulerStore =
    PruningSchedulerStore.create(
      MediatorId.Code,
      storage,
      timeouts,
      loggerFactory,
    )

  override def schedulerJob(
      schedule: IndividualSchedule
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] = withUpdatePruningMetric(
    schedule,
    mediator.stateInspection
      .locatePruningTimestamp(NonNegativeInt.zero)
      .map(mediator.stateInspection.reportMaxResponseAgeMetric),
  ) { pruningSchedule =>
    for {
      nextBatchTimestamp <- mediator.stateInspection
        .locatePruningTimestamp(
          config.maxPruningBatchSize.toNonNegative
        )

      retentionTimestamp = clock.now - pruningSchedule.retention

      minTimestamp = nextBatchTimestamp.fold(retentionTimestamp)(_.min(retentionTimestamp))

      _ = logger.debug(s"About to prune up to $minTimestamp")

      result <- mediator
        .prune(minTimestamp)
        .fold[JobScheduler.ScheduledRunResult](
          {
            case NoDataAvailableForPruning => JobScheduler.Done
            case err =>
              // Return other errors as errors in order to raise operator visibility as these are not expected
              // to happen in production scenarios with long retention values, but if they do manual pruning
              // might be required if we ever run into MissingDomainParametersForValidPruningTsComputation.
              JobScheduler.Error(err.message)
          },
          _ => {
            logger.info(s"Pruned up to $minTimestamp")
            if (minTimestamp == retentionTimestamp) JobScheduler.Done
            else
              JobScheduler.MoreWorkToPerform
          },
        )

    } yield result
  }

  override def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] =
    getSchedule().map(
      JobSchedule
        .fromPruningSchedule(
          _,
          config.pruningMetricUpdateInterval.map(PositiveSeconds.fromConfig),
          clock,
          logger,
        )
    )

  override def onClosed(): Unit = {
    stop()(TraceContext.todo)
    super.onClosed()
  }
}
