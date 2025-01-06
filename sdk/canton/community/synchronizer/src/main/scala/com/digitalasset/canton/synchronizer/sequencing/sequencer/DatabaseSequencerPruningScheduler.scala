// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.*
import com.digitalasset.canton.scheduler.JobScheduler.*
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

private[sequencing] class DatabaseSequencerPruningScheduler(
    clock: Clock,
    sequencer: Sequencer,
    storage: Storage,
    pruningConfig: DatabaseSequencerConfig.SequencerPruningConfig,
    override val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends JobScheduler(
      "pruning",
      timeouts,
      loggerFactory,
    )
    with PruningScheduler
    with HasPruningSchedulerStore
    with UpdatePruningMetric {

  override protected val pruningSchedulerStore: PruningSchedulerStore =
    PruningSchedulerStore
      .create(
        SequencerId.Code,
        storage,
        timeouts,
        loggerFactory,
      )

  override def schedulerJob(
      schedule: IndividualSchedule
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ScheduledRunResult] =
    withUpdatePruningMetric(
      schedule,
      (for {
        oldestEventTimestamp <- sequencer.locatePruningTimestamp(PositiveInt.one)
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sequencer.reportMaxEventAgeMetric(oldestEventTimestamp)
        )
      } yield ()).valueOr(_ => ()),
    ) { pruningSchedule =>
      def locateTsAt(
          index: PositiveInt,
          onLeft: PruningSupportError => Error = e => Error(e.message),
      ) = sequencer
        .locatePruningTimestamp(index)
        .leftMap(onLeft)

      (for {
        _isActive <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          storage.isActive,
          Error(
            "Exclusive storage inactive",
            DatabaseSequencerPruningScheduler.passiveExclusiveStorageBackoff,
            logAsInfo = true,
          ),
        )

        _ = logger.debug("Checking whether to prune")

        nextBatchTimestamp <- locateTsAt(pruningConfig.maxPruningBatchSize)

        retentionTimestamp = clock.now - pruningSchedule.retention

        minTimestamp = nextBatchTimestamp.fold(retentionTimestamp)(_.min(retentionTimestamp))

        oldestEventTimestampBeforePruning <- locateTsAt(
          PositiveInt.one,
          e => Error(s"Failed to look up earliest pruning timestamp: ${e.message}"),
        )

        _ = logger.debug(s"About to prune up to $minTimestamp")

        _ <- sequencer
          .prune(minTimestamp)
          .bimap(err => Error(err.message), report => logger.info(report))

        // If sequencer pruning hasn't prune anything (e.g. because of member checkpoint constraints),
        // return to the scheduler with an Error logged at info in order to trigger backoff, so we don't
        // hammer the sequencer with back-to-back prune calls. (#15702)
        result <- oldestEventTimestampBeforePruning match {
          case _ if minTimestamp == retentionTimestamp =>
            EitherT.rightT[FutureUnlessShutdown, Error](Done: ScheduledRunResult)
          case Some(oldestTsBefore) =>
            locateTsAt(
              PositiveInt.one,
              e => Error(s"Failed earliest pruning timestamp lookup: ${e.message}"),
            ).transform(
              _.flatMap[Error, ScheduledRunResult](oldestTsAfter =>
                if (oldestTsAfter.contains(oldestTsBefore))
                  Left(
                    Error(
                      s"Nothing pruned. Oldest timestamp still at $oldestTsBefore",
                      logAsInfo = true,
                    )
                  )
                else Right(MoreWorkToPerform)
              )
            )
          case _ =>
            EitherT.rightT[FutureUnlessShutdown, Error](MoreWorkToPerform: ScheduledRunResult)
        }
      } yield result).merge
    }

  override def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] =
    getSchedule().map(
      JobSchedule
        .fromPruningSchedule(
          _,
          pruningConfig.pruningMetricUpdateInterval.map(PositiveSeconds.fromConfig),
          clock,
          logger,
        )
    )

  override def onClosed(): Unit = {
    stop()(TraceContext.todo)
    super.close()
  }
}

private[sequencing] object DatabaseSequencerPruningScheduler {
  private val passiveExclusiveStorageBackoff = NonNegativeFiniteDuration.tryOfSeconds(60)
}
