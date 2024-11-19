// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.{EitherT, OptionT}
import cats.instances.option.*
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.admin.data.{
  SequencerAdminStatus,
  SequencerHealthStatus,
}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerWriter.ResetWatermark
import com.digitalasset.canton.domain.sequencing.sequencer.errors.*
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.SnapshotNotFound
import com.digitalasset.canton.domain.sequencing.sequencer.store.*
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerStore.SequencerPruningResult
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil.doNotAwait
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, LoggerUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

object DatabaseSequencer {

  /** Creates a single instance of a database sequencer. */
  def single(
      config: DatabaseSequencerConfig,
      initialState: Option[SequencerInitialState],
      timeouts: ProcessingTimeout,
      storage: Storage,
      sequencerStore: SequencerStore,
      clock: Clock,
      domainId: DomainId,
      topologyClientMember: Member,
      protocolVersion: ProtocolVersion,
      cryptoApi: DomainSyncCryptoClient,
      metrics: SequencerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): DatabaseSequencer = {

    val logger = TracedLogger(DatabaseSequencer.getClass, loggerFactory)

    ErrorUtil.requireArgument(
      !config.highAvailabilityEnabled,
      "Single database sequencer creation must not have HA enabled",
    )(ErrorLoggingContext.fromTracedLogger(logger)(TraceContext.empty))

    new DatabaseSequencer(
      SequencerWriterStoreFactory.singleInstance,
      sequencerStore,
      config,
      initialState,
      TotalNodeCountValues.SingleSequencerTotalNodeCount,
      new LocalSequencerStateEventSignaller(
        timeouts,
        loggerFactory,
      ),
      None,
      // Dummy config which will be ignored anyway as `config.highAvailabilityEnabled` is false
      OnlineSequencerCheckConfig(),
      timeouts,
      storage,
      None,
      None,
      clock,
      domainId,
      topologyClientMember,
      trafficConsumedStore = None,
      protocolVersion,
      cryptoApi,
      metrics,
      loggerFactory,
      blockSequencerMode = false,
    )
  }
}

class DatabaseSequencer(
    writerStorageFactory: SequencerWriterStoreFactory,
    sequencerStore: SequencerStore,
    config: DatabaseSequencerConfig,
    initialState: Option[SequencerInitialState],
    totalNodeCount: PositiveInt,
    eventSignaller: EventSignaller,
    keepAliveInterval: Option[NonNegativeFiniteDuration],
    onlineSequencerCheckConfig: OnlineSequencerCheckConfig,
    override protected val timeouts: ProcessingTimeout,
    storage: Storage,
    exclusiveStorage: Option[Storage],
    health: Option[SequencerHealthConfig],
    clock: Clock,
    domainId: DomainId,
    topologyClientMember: Member,
    trafficConsumedStore: Option[TrafficConsumedStore],
    protocolVersion: ProtocolVersion,
    cryptoApi: DomainSyncCryptoClient,
    metrics: SequencerMetrics,
    loggerFactory: NamedLoggerFactory,
    blockSequencerMode: Boolean,
)(implicit ec: ExecutionContext, tracer: Tracer, materializer: Materializer)
    extends BaseSequencer(
      loggerFactory,
      health,
      clock,
      SignatureVerifier(cryptoApi),
    )
    with FlagCloseable {

  private val writer = SequencerWriter(
    config.writer,
    writerStorageFactory,
    totalNodeCount,
    keepAliveInterval,
    timeouts,
    storage,
    sequencerStore,
    clock,
    eventSignaller,
    protocolVersion,
    loggerFactory,
    blockSequencerMode,
    metrics,
  )

  private lazy val storageForAdminChanges: Storage = exclusiveStorage.getOrElse(
    storage // no exclusive storage in non-ha setups
  )

  override val pruningScheduler: Option[PruningScheduler] =
    pruningSchedulerBuilder.map(_(storageForAdminChanges))

  override def adminStatus: SequencerAdminStatus = SequencerAdminStatus(
    storageForAdminChanges.isActive
  )

  @VisibleForTesting
  private[canton] val store = writer.generalStore

  protected val memberValidator: SequencerMemberValidator = sequencerStore

  protected def resetWatermarkTo: ResetWatermark = SequencerWriter.ResetWatermarkToClockNow

  // Only start pruning scheduler after `store` variable above has been initialized to avoid racy NPE
  withNewTraceContext { implicit traceContext =>
    timeouts.unbounded.await(s"Waiting for sequencer writer to fully start")(
      writer.startOrLogError(initialState, resetWatermarkTo).flatMap(_ => backfillCheckpoints())
    )

    pruningScheduler.foreach(ps =>
      // default wait should be enough since scheduler start only involves single database access
      timeouts.default.await(
        s"Waiting for pruning scheduler to start",
        logFailing = Level.INFO.some, // in case we're shutting down at start-up
      )(
        ps.start()
      )
    )
  }

  private def backfillCheckpoints()(implicit traceContext: TraceContext): Future[Unit] =
    for {
      latestCheckpoint <- sequencerStore.fetchLatestCheckpoint()
      watermark <- sequencerStore.safeWatermark
      _ <- (latestCheckpoint, watermark)
        .traverseN { (oldest, watermark) =>
          val interval = config.writer.checkpointInterval
          val checkpointsToWrite = LazyList
            .iterate(oldest.plus(interval.asJava))(ts => ts.plus(interval.asJava))
            .takeWhile(_ <= watermark)

          if (checkpointsToWrite.nonEmpty) {
            val start = System.nanoTime()
            logger.info(
              s"Starting to backfill checkpoints from $oldest to $watermark in intervals of $interval"
            )
            MonadUtil
              .parTraverseWithLimit(config.writer.checkpointBackfillParallelism)(
                checkpointsToWrite
              )(cp => sequencerStore.recordCounterCheckpointsAtTimestamp(cp))
              .map { _ =>
                val elapsed = (System.nanoTime() - start).nanos
                logger.info(
                  s"Finished backfilling checkpoints from $oldest to $watermark in intervals of $interval in ${LoggerUtil
                      .roundDurationForHumans(elapsed)}"
                )
              }
          } else {
            Future.successful(())
          }
        }
    } yield ()

  // periodically run the call to mark lagging sequencers as offline
  private def periodicallyMarkLaggingSequencersOffline(
      checkInterval: NonNegativeFiniteDuration,
      offlineCutoffDuration: NonNegativeFiniteDuration,
  ): Unit = {
    def schedule(): Unit = {
      val _ = clock.scheduleAfter(_ => markOffline(), checkInterval.unwrap)
    }

    def markOfflineF()(implicit traceContext: TraceContext): Future[Unit] = {
      val cutoffTime = clock.now.minus(offlineCutoffDuration.unwrap)
      logger.trace(s"Marking sequencers with watermarks earlier than [$cutoffTime] as offline")

      sequencerStore.markLaggingSequencersOffline(cutoffTime)
    }

    def markOffline(): Unit = withNewTraceContext { implicit traceContext =>
      doNotAwait(
        performUnlessClosingF(functionFullName)(markOfflineF().thereafter { _ =>
          // schedule next marking sequencers as offline regardless of outcome
          schedule()
        }).onShutdown {
          logger.debug("Skipping scheduling next offline sequencer check due to shutdown")
        },
        "Marking lagging sequencers as offline failed",
      )
    }

    schedule()
  }

  if (config.highAvailabilityEnabled)
    periodicallyMarkLaggingSequencersOffline(
      onlineSequencerCheckConfig.onlineCheckInterval.toInternal,
      onlineSequencerCheckConfig.offlineDuration.toInternal,
    )

  private val reader =
    new SequencerReader(
      config.reader,
      domainId,
      sequencerStore,
      cryptoApi,
      eventSignaller,
      topologyClientMember,
      trafficConsumedStore,
      protocolVersion,
      timeouts,
      loggerFactory,
      blockSequencerMode = blockSequencerMode,
    )

  override def isRegistered(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    sequencerStore.lookupMember(member).map(_.isDefined)

  override def isEnabled(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    sequencerStore.lookupMember(member).map {
      case Some(registeredMember) => registeredMember.enabled
      case None =>
        logger.warn(
          s"Attempted to check if member $member is enabled but they are not registered"
        )
        false
    }

  override private[sequencing] final def registerMemberInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RegisterError, Unit] =
    EitherT
      .right[RegisterError](sequencerStore.registerMember(member, timestamp))
      .map(_ => ())

  override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] =
    for {
      // TODO(#12405) Support aggregatable submissions in the DB sequencer
      _ <- EitherT.cond[FutureUnlessShutdown](
        submission.aggregationRule.isEmpty,
        (),
        SendAsyncError.RequestRefused(
          "Aggregatable submissions are not yet supported by this database sequencer"
        ),
      )
      // TODO(#12363) Support group addresses in the DB Sequencer
      _ <- EitherT.cond[FutureUnlessShutdown](
        !submission.batch.allRecipients.exists {
          case _: MemberRecipient => false
          case _ => true
        },
        (),
        SendAsyncError.RequestRefused(
          "Group addresses are not yet supported by this database sequencer"
        ),
      )
      _ <- writer.send(submission).mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  protected def blockSequencerWriteInternal(
      outcome: DeliverableSubmissionOutcome
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] =
    writer.blockSequencerWrite(outcome)

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] =
    sendAsyncInternal(signedSubmission.content)

  override def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] = reader.read(member, offset)

  /** Internal method to be used in the sequencer integration.
    */
  // TODO(#18401): Refactor ChunkUpdate and merge/remove this method with `acknowledgeSignedInternal` below
  final protected def writeAcknowledgementInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    withExpectedRegisteredMember(member, "Acknowledge") {
      sequencerStore.acknowledge(_, timestamp)
    }

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    // it is unlikely that the ack operation will be called without the member being registered
    // as the sequencer-client will need to be registered to send and subscribe.
    // rather than introduce an error to deal with this case in the database sequencer we'll just
    // fail the operation.
    val req = signedAcknowledgeRequest.content
    writeAcknowledgementInternal(req.member, req.timestamp)
  }

  protected def disableMemberInternal(
      member: Member
  )(implicit traceContext: TraceContext): Future[Unit] = sequencerStore.disableMember(member)

  // For the database sequencer, the SequencerId serves as the local sequencer identity/member
  // until the database and block sequencers are unified.
  override protected def localSequencerMember: Member = SequencerId(domainId.uid)

  /** helper for performing operations that are expected to be called with a registered member so will just throw if we
    * find the member is unregistered.
    */
  final protected def withExpectedRegisteredMember[A](member: Member, operationName: String)(
      fn: SequencerMemberId => Future[A]
  )(implicit traceContext: TraceContext): Future[A] =
    for {
      memberIdO <- sequencerStore.lookupMember(member).map(_.map(_.memberId))
      memberId = memberIdO.getOrElse {
        logger.warn(s"$operationName attempted to use member [$member] but they are not registered")
        sys.error(s"Operation requires the member to have been registered with the sequencer")
      }
      result <- fn(memberId)
    } yield result

  override def pruningStatus(implicit traceContext: TraceContext): Future[SequencerPruningStatus] =
    sequencerStore.status(clock.now)

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): Future[SequencerHealthStatus] =
    writer.healthStatus

  override def prune(
      requestedTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, PruningError, String] =
    for {
      status <- EitherT.right[PruningError](this.pruningStatus)
      // Update the max-event-age metric after pruning. Use the actually pruned timestamp as
      // the database sequencer tends to prune fewer events than asked for e.g. not wanting to
      // prune sequencer-counter checkpoints partially.
      report <- sequencerStore
        .prune(requestedTimestamp, status, config.writer.payloadToEventMargin.toInternal)
        .map { case SequencerPruningResult(tsActuallyPrunedUpTo, report) =>
          MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, tsActuallyPrunedUpTo)
          report
        }
    } yield report

  override def locatePruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningSupportError, Option[CantonTimestamp]] =
    EitherT.right[PruningSupportError](
      sequencerStore
        .locatePruningTimestamp(NonNegativeInt.tryCreate(index.value - 1))
    )

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] =
    Either.right(
      MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestEventTimestamp)
    )

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerError, SequencerSnapshot] =
    for {
      safeWatermarkO <- EitherT.right(sequencerStore.safeWatermark)
      // we check if watermark is after the requested timestamp to avoid snapshotting the sequencer
      // at a timestamp that is not yet safe to read
      _ <- {
        safeWatermarkO match {
          case Some(safeWatermark) =>
            EitherTUtil.condUnitET[Future](
              timestamp <= safeWatermark,
              SnapshotNotFound.Error(timestamp, safeWatermark),
            )
          case None =>
            EitherT.leftT[Future, Unit](SnapshotNotFound.MissingSafeWatermark(topologyClientMember))
        }
      }
      snapshot <- EitherT.right[SequencerError](sequencerStore.readStateAtTimestamp(timestamp))
    } yield snapshot

  override private[sequencing] def firstSequencerCounterServeableForSequencer(implicit
      traceContext: TraceContext
  ): Future[SequencerCounter] =
    if (blockSequencerMode) {
      val result = for {
        member <- OptionT(sequencerStore.lookupMember(topologyClientMember))
        checkpoint <- OptionT(sequencerStore.fetchEarliestCheckpointForMember(member.memberId))
      } yield checkpoint.counter + 1
      result.getOrElse(SequencerCounter.Genesis)
    } else {
      // Database sequencers are never bootstrapped
      Future.successful(SequencerCounter.Genesis)
    }

  override def onClosed(): Unit =
    Lifecycle.close(
      () => super.onClosed(),
      () => pruningScheduler foreach (Lifecycle.close(_)(logger)),
      () => exclusiveStorage foreach (Lifecycle.close(_)(logger)),
      writer,
      reader,
      eventSignaller,
      sequencerStore,
    )(logger)

  override def trafficStatus(members: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerTrafficStatus] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      domainTimeTracker: DomainTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TrafficControlErrors.TrafficControlError,
    Unit,
  ] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )
}
