// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{BatchingConfig, ClientConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.{
  ParticipantNodePersistentState,
  ParticipantPruningSchedulerStore,
}
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.*
import com.digitalasset.canton.scheduler.JobScheduler.*
import com.digitalasset.canton.time.{Clock, PositiveSeconds}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.util.chaining.scalaUtilChainingOps
import scala.util.control.NonFatal

final class ParticipantPruningScheduler(
    pruningProcessor: PruningProcessor,
    clock: Clock,
    metrics: ParticipantMetrics,
    ledgerApiClientConfig: ClientConfig,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    storage: Storage, // storage to build the pruning scheduler store that tracks the current schedule
    adminToken: CantonAdminToken, // the admin token is needed to invoke pruning via the ledger-api
    pruningConfig: ParticipantStoreConfig,
    batchingConfig: BatchingConfig,
    override val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextIdlenessExecutorService, // executor needed for ledger client netty channel
    executionSequencerFactory: ExecutionSequencerFactory,
) extends JobScheduler(
      "pruning",
      timeouts,
      loggerFactory,
    )
    with HasPruningSchedulerStore
    with UpdatePruningMetric {

  override protected val pruningSchedulerStore: ParticipantPruningSchedulerStore =
    ParticipantPruningSchedulerStore.create(
      storage,
      timeouts,
      loggerFactory,
    )

  private val ledgerClient = new AtomicReference[Option[LedgerClient]](None)

  /** Prune the next batch. */
  override def schedulerJob(schedule: IndividualSchedule)(implicit
      traceContext: TraceContext
  ): Future[ScheduledRunResult] = withUpdatePruningMetric(
    schedule,
    reportMaxEventAgeMetric(),
  ) { pruningSchedule =>
    val pruneInternallyOnly = pruningSchedule match {
      case pse: ParticipantPruningCronSchedule => pse.pruneInternallyOnly
      case _: PruningCronSchedule =>
        ErrorUtil.internalError(
          new IllegalStateException(
            "Participant are only ever initialized with a participant pruning scheduler"
          )
        )
    }
    val timestampByRetention = clock.now.minus(pruningSchedule.retention.duration)

    (for {
      offsetByRetention <- EitherT.right[ScheduledRunResult](
        participantNodePersistentState.value.ledgerApiStore
          .lastDomainOffsetBeforeOrAtPublicationTime(timestampByRetention)
          .map(_.map(_.offset).map(GlobalOffset.tryFromLedgerOffset))
      )
      _ = logger.debug(
        s"Calculating safe-to-prune offset by [offset-by-retention: $offsetByRetention, timestamp-by-retention: $timestampByRetention]"
      )
      offsetDone <- offsetByRetention match {
        case None => EitherT.pure[Future, ScheduledRunResult](None)
        case Some(offset) =>
          pruningProcessor
            .safeToPrune(timestampByRetention, offset)
            .bimap(
              err =>
                Error(
                  s"Failed to obtain safe-to-prune offset for $timestampByRetention and $offset: ${err.message}"
                ),
              { safeOffset =>
                logger.debug(
                  s"Found safe-to-prune offset $safeOffset for $timestampByRetention and $offset"
                )
                safeOffset.map(offset.min)
              },
            )
      }
      offsetByBatch <- pruningProcessor.locatePruningOffsetForOneIteration.leftMap(pruningError =>
        Error(s"Error while locating pruning offset for one iteration: ${pruningError.message}")
      )
      // If the retention offset exists, prune at the minimum of the retention offset and the maximum batch size offset
      // If the batch offset does not exist (e.g. if participant workload has stopped sometime after retention offset),
      // use retention offset.
      minOffset = offsetDone.map(_.min(offsetByBatch))
      result <-
        minOffset.fold {
          logger.info(
            s"Nothing to prune. Timestamp $timestampByRetention does not map to an offset or is unsafe to prune"
          )
          EitherT.pure[Future, ScheduledRunResult](Done: ScheduledRunResult)
        } { offsetToPruneUpTo =>
          val pruneUpTo = UpstreamOffsetConvert.toStringOffset(offsetToPruneUpTo)
          val submissionId = UUID.randomUUID().toString
          val internally = if (pruneInternallyOnly) "internally" else ""
          logger.info(
            s"About to prune $internally up to $offsetToPruneUpTo ($pruneUpTo), submission id $submissionId, [offset-by-retention: $offsetByRetention, offset-by-safe-to-prune: $offsetDone, offset-by-batch: $offsetByBatch]"
          )

          def doneOrMoreWorkToPerform: ScheduledRunResult = {
            logger.info(
              s"Pruned $internally up to offset $offsetToPruneUpTo ($pruneUpTo), submission id $submissionId"
            )
            // Done if we pruned up to the retention offset.
            if (offsetDone.forall(_ == offsetToPruneUpTo)) Done else MoreWorkToPerform
          }

          def pruneViaLedgerApi(): EitherT[Future, ScheduledRunResult, ScheduledRunResult] = {
            val future = for {
              ledgerClient <- tryEnsureLedgerClient()
              result <- ledgerClient.participantPruningManagementClient
                .prune(pruneUpTo, submissionId = Some(submissionId))
                .map(_emptyResponse => doneOrMoreWorkToPerform.asRight[ScheduledRunResult])
            } yield result
            // Turn grpc errors returned as Future.failed into a Left ScheduledRunResult.
            EitherT(
              future
                .recover { case NonFatal(t) =>
                  // At startup and shutdown the ledger api server may not yet/anymore be ready, so log at info
                  // rather than a warning (#15702).
                  Error(
                    s"Non fatal error invoking ledger api server pruning via ledger client: ${t.getMessage}",
                    logAsInfo = true,
                  ).asLeft[ScheduledRunResult]
                }
            )
          }

          def pruneInternally(): EitherT[Future, ScheduledRunResult, ScheduledRunResult] =
            EitherT(
              pruningProcessor
                .pruneLedgerEvents(offsetToPruneUpTo)
                .bimap(err => Error(err.message), _ => doneOrMoreWorkToPerform)
                .value
                .onShutdown(Left(Error("Not pruning because of shutdown")))
            )

          // Don't invoke pruning if we have since become inactive, e.g. to avoid creating another
          // ledger client.
          if (isScheduleActivated) {
            if (pruneInternallyOnly) {
              pruneInternally()
            } else {
              pruneViaLedgerApi()
            }
          } else
            EitherT.leftT[Future, ScheduledRunResult](
              Error("Pruning scheduler has since become inactive.")
            )
        }
      // Ask for the first event timestamp as a way to report the participant's max-event-age metric.
      // Because the last participant pruning step is the ledger api server index after which the
      // canton participant portion no longer has the ability to update the age metric. We could have
      // chosen to report the metric after pruning only the canton stores portion, but that would be
      // misleading in case the ledger api server index prune operation fails.
      _ <- EitherT.right[ScheduledRunResult](
        reportMaxEventAgeMetric()
      )
    } yield result).merge
  }

  def setParticipantSchedule(schedule: ParticipantPruningSchedule)(implicit
      traceContext: TraceContext
  ): Future[Unit] = updateScheduleAndReactivateIfActive(
    pruningSchedulerStore.setParticipantSchedule(schedule)
  )

  def getParticipantSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[ParticipantPruningSchedule]] = pruningSchedulerStore.getParticipantSchedule()

  override def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] =
    getParticipantSchedule().map { maybeSchedule =>
      // Always update pruning "max-event-age" metric.
      val metricsUpdateSchedule = pruningConfig.pruningMetricUpdateInterval.map(interval =>
        new IntervalSchedule(PositiveSeconds.fromConfig(interval))
      )
      // Pruning schedule may not always exist.
      val maybePruningSchedule = maybeSchedule.map { es =>
        val ps = es.schedule
        new ParticipantPruningCronSchedule(
          ps.cron,
          ps.maxDuration,
          ps.retention,
          es.pruneInternallyOnly,
          clock,
          logger,
        )
      }
      JobSchedule(
        List(
          metricsUpdateSchedule,
          maybePruningSchedule,
        ).flatten
      )
    }

  override protected def deactivate()(implicit traceContext: TraceContext): Unit = {
    super.deactivate()
    ledgerClient.getAndSet(None).foreach(_.close())
  }

  /** Make available a LedgerClient needed for participant pruning via the ledger api */
  private def tryEnsureLedgerClient()(implicit traceContext: TraceContext): Future[LedgerClient] =
    ledgerClient
      .get()
      .fold(
        buildLedgerClient().tap(_.foreach(newLc => ledgerClient.set(Some(newLc))))
      )(Future.successful)

  private def buildLedgerClient()(implicit traceContext: TraceContext): Future[LedgerClient] = {
    val clientConfig = LedgerClientConfiguration(
      applicationId = "admin-prune",
      commandClient = CommandClientConfiguration.default,
      token = Some(adminToken.secret),
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      ledgerApiClientConfig.tls.map(x => ClientChannelBuilder.sslContext(x))
    )
    val builder = clientChannelConfig
      .builderFor(
        ledgerApiClientConfig.address,
        ledgerApiClientConfig.port.unwrap,
      )
      .executor(ec)
    LedgerClient(builder.build(), clientConfig, loggerFactory)
  }

  private def reportMaxEventAgeMetric()(implicit traceContext: TraceContext): Future[Unit] =
    participantNodePersistentState.value.ledgerApiStore
      .firstDomainOffsetAfterOrAtPublicationTime(CantonTimestamp.MinValue)
      .map(domainOffset =>
        MetricsHelper.updateAgeInHoursGauge(
          clock,
          metrics.pruning.maxEventAge,
          domainOffset.map(_.publicationTime).map(CantonTimestamp.apply),
        )
      )
}
