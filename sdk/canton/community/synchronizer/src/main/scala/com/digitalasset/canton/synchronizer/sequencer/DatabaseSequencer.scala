// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.{EitherT, OptionT}
import cats.instances.option.*
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriter.ResetWatermark
import com.digitalasset.canton.synchronizer.sequencer.admin.data.{
  SequencerAdminStatus,
  SequencerHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.SnapshotNotFound
import com.digitalasset.canton.synchronizer.sequencer.errors.{
  CreateSubscriptionError,
  SequencerError,
}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore.SequencerPruningResult
import com.digitalasset.canton.synchronizer.sequencer.store.{
  SequencerMemberId,
  SequencerMemberValidator,
  SequencerStore,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.{Member, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.FutureUtil.doNotAwait
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.Pause
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, LoggerUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object DatabaseSequencer {

  /** Creates a single instance of a database sequencer. */
  def single(
      config: DatabaseSequencerConfig,
      initialState: Option[SequencerInitialState],
      timeouts: ProcessingTimeout,
      storage: Storage,
      sequencerStore: SequencerStore,
      clock: Clock,
      synchronizerId: SynchronizerId,
      topologyClientMember: Member,
      protocolVersion: ProtocolVersion,
      cryptoApi: SynchronizerCryptoClient,
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
      None,
      timeouts,
      storage,
      None,
      None,
      clock,
      synchronizerId,
      topologyClientMember,
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
    onlineSequencerCheckConfig: Option[OnlineSequencerCheckConfig],
    override protected val timeouts: ProcessingTimeout,
    storage: Storage,
    exclusiveStorage: Option[Storage],
    health: Option[SequencerHealthConfig],
    clock: Clock,
    synchronizerId: SynchronizerId,
    topologyClientMember: Member,
    protocolVersion: ProtocolVersion,
    cryptoApi: SynchronizerCryptoClient,
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

  require(
    blockSequencerMode || config.writer.eventWriteMaxConcurrency == 1,
    "The database sequencer must be configured with writer.event-write-max-concurrency = 1",
  )

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
      writer
        .startOrLogError(initialState, resetWatermarkTo)
        .flatMap(_ => backfillCheckpoints())
        .onShutdown(logger.info("Sequencer writer not started due to shutdown"))
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

  private def backfillCheckpoints()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
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
            FutureUnlessShutdown.pure(())
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

    def markOfflineF()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val cutoffTime = clock.now.minus(offlineCutoffDuration.unwrap)
      logger.trace(s"Marking sequencers with watermarks earlier than [$cutoffTime] as offline")

      sequencerStore.markLaggingSequencersOffline(cutoffTime)
    }

    def markOffline(): Unit = withNewTraceContext { implicit traceContext =>
      doNotAwait(
        performUnlessClosingUSF(functionFullName)(markOfflineF().thereafter { _ =>
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

  onlineSequencerCheckConfig.foreach { config =>
    periodicallyMarkLaggingSequencersOffline(
      config.onlineCheckInterval.toInternal,
      config.offlineDuration.toInternal,
    )
  }

  private val reader =
    new SequencerReader(
      config.reader,
      synchronizerId,
      sequencerStore,
      cryptoApi,
      eventSignaller,
      topologyClientMember,
      protocolVersion,
      timeouts,
      loggerFactory,
      blockSequencerMode = blockSequencerMode,
    )

  override def isRegistered(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    sequencerStore.lookupMember(member).map(_.isDefined)

  override def isEnabled(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    sequencerStore.lookupMember(member).map {
      case Some(registeredMember) => registeredMember.enabled
      case None =>
        logger.warn(
          s"Attempted to check if member $member is enabled but they are not registered"
        )
        false
    }

  override private[sequencer] final def registerMemberInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RegisterError, Unit] =
    EitherT
      .right[RegisterError](sequencerStore.registerMember(member, timestamp))
      .map(_ => ())

  override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    for {
      // TODO(#12405) Support aggregatable submissions in the DB sequencer
      _ <- EitherT.cond[FutureUnlessShutdown](
        submission.aggregationRule.isEmpty,
        (),
        SequencerErrors.UnsupportedFeature(
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
        SequencerErrors.UnsupportedFeature(
          "Group addresses are not yet supported by this database sequencer"
        ),
      )
      _ <- writer.send(submission)
    } yield ()

  protected def blockSequencerWriteInternal(
      outcome: DeliverableSubmissionOutcome
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    writer.blockSequencerWrite(outcome)

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    sendAsyncInternal(signedSubmission.content)

  override def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.EventSource] =
    reader.read(member, offset)

  override def readInternalV2(member: Member, timestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.EventSource] =
    reader.readV2(member, timestamp)

  /** Internal method to be used in the sequencer integration.
    */
  // TODO(#18401): Refactor ChunkUpdate and merge/remove this method with `acknowledgeSignedInternal` below
  final protected def writeAcknowledgementInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    withExpectedRegisteredMember(member, "Acknowledge") {
      sequencerStore.acknowledge(_, timestamp)
    }

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // it is unlikely that the ack operation will be called without the member being registered
    // as the sequencer-client will need to be registered to send and subscribe.
    // rather than introduce an error to deal with this case in the database sequencer we'll just
    // fail the operation.
    val req = signedAcknowledgeRequest.content
    writeAcknowledgementInternal(req.member, req.timestamp)
  }

  protected def disableMemberInternal(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    sequencerStore.disableMember(member)

  // For the database sequencer, the SequencerId serves as the local sequencer identity/member
  // until the database and block sequencers are unified.
  override protected def localSequencerMember: Member = SequencerId(synchronizerId.uid)

  /** helper for performing operations that are expected to be called with a registered member so
    * will just throw if we find the member is unregistered.
    */
  final protected def withExpectedRegisteredMember[A](member: Member, operationName: String)(
      fn: SequencerMemberId => FutureUnlessShutdown[A]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[A] =
    for {
      memberIdO <- sequencerStore.lookupMember(member).map(_.map(_.memberId))
      memberId = memberIdO.getOrElse {
        logger.warn(s"$operationName attempted to use member [$member] but they are not registered")
        sys.error(s"Operation requires the member to have been registered with the sequencer")
      }
      result <- fn(memberId)
    } yield result

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerPruningStatus] =
    sequencerStore.status(clock.now)

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus] =
    writer.healthStatus

  override def prune(
      requestedTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PruningError, String] =
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
  ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] =
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

  override def awaitContainingBlockLastTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, CantonTimestamp] =
    EitherT.right(
      FutureUnlessShutdown.pure(timestamp)
    ) // DatabaseSequencer doesn't have a concept of blocks

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    for {
      safeWatermarkO <- EitherT.right(sequencerStore.safeWatermark)
      // we check if watermark is after the requested timestamp to avoid snapshotting the sequencer
      // at a timestamp that is not yet safe to read
      _ <- checkWatermarkIsAfterTimestamp(safeWatermarkO, timestamp)
      snapshot <- EitherT.right[SequencerError](sequencerStore.readStateAtTimestamp(timestamp))
    } yield snapshot

  private def checkWatermarkIsAfterTimestamp(
      safeWatermarkO: Option[CantonTimestamp],
      timestamp: CantonTimestamp,
  ): EitherT[FutureUnlessShutdown, SequencerError, Unit] =
    safeWatermarkO match {
      case Some(safeWatermark) =>
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          timestamp <= safeWatermark,
          SnapshotNotFound.Error(timestamp, safeWatermark),
        )
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          SnapshotNotFound.MissingSafeWatermark(topologyClientMember)
        )
    }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] = {
    val delay = 1.second
    val waitForWatermarkToPassTimestamp = EitherT(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      )
        .unlessShutdown(
          sequencerStore.safeWatermark.flatMap(checkWatermarkIsAfterTimestamp(_, timestamp).value),
          DbExceptionRetryPolicy,
        )
    )
    waitForWatermarkToPassTimestamp
      .flatMap { _ =>
        snapshot(timestamp)
      }
  }

  override private[sequencer] def firstSequencerCounterServeableForSequencer(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerCounter] =
    if (blockSequencerMode) {
      val result = for {
        member <- OptionT(sequencerStore.lookupMember(topologyClientMember))
        checkpoint <- OptionT(sequencerStore.fetchEarliestCheckpointForMember(member.memberId))
      } yield checkpoint.counter + 1
      result.getOrElse(SequencerCounter.Genesis)
    } else {
      // Database sequencers are never bootstrapped
      FutureUnlessShutdown.pure(SequencerCounter.Genesis)
    }

  override def onClosed(): Unit =
    LifeCycle.close(
      () => super.onClosed(),
      () => pruningScheduler foreach (LifeCycle.close(_)(logger)),
      () => exclusiveStorage foreach (LifeCycle.close(_)(logger)),
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
      synchronizerTimeTracker: SynchronizerTimeTracker,
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
