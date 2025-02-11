// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.{NonEmptyList, ValidatedNec}
import cats.syntax.contravariantSemigroupal.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.validated.*
import com.daml.error.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.admin.participant.v30.{ReceivedCommitmentState, SentCommitmentState}
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  PositiveInt,
  PositiveNumeric,
}
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AcsCommitmentErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.event.{
  AcsChange,
  AcsChangeListener,
  ContractStakeholdersAndReassignmentCounter,
  RecordTime,
}
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.DegradationError
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.AcsCommitmentAlarm
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.RunningCommitments
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{
  AcsCommitmentsCatchUpParameters,
  LfContractId,
  SerializableContract,
}
import com.digitalasset.canton.pruning.{
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
  PruningStatus,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.RequestRefused
import com.digitalasset.canton.sequencing.client.{SendResult, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients, SendAsyncError}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPartyId,
  ProtoDeserializationError,
  ReassignmentCounter,
  RequestCounter,
}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{Map, SortedSet}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, blocking}
import scala.math.Ordering.Implicits.*

/** Computes, sends, receives and compares ACS commitments
  *
  *  In more detail:
  *
  *  <ol>
  *   <li>The class computes the participant's ACS commitments (for each of the participant's "counter-participants", i.e.,
  *     participants who host a stakeholder of some contract in participant's ACS). The commitments are computed at
  *     specified (sequencer) times that are configured by the synchronizer and are uniform for all participants connected to
  *     the synchronizer. We refer to them as "commitment ticks". The commitments must be computed "online", i.e., after
  *     the state of the ACS at a commitment tick becomes known.
  *
  *   <li>After the commitments for a tick are computed, they should be distributed to the counter-participants; but
  *     this is best-effort.
  *   </li>
  *
  *   <li>The class processes the ACS commitments from counter-participants (method `processBatch`):
  *
  *     <ol>
  *      <li>it checks that the commitments are properly signed
  *      </li>
  *      <li>it checks that they match the locally computed ACS commitments
  *      </li>
  *     </ol>
  *   </li>
  *
  *   <li>The class must define crash recovery points, such that the class itself combined with startup procedures of
  *      the node jointly ensure that the participant doesn't neglect to send its ACS commitments or process the remote
  *      ones. We allow the participant to send the same commitments multiple times in case of a crash, and we do allow
  *      the participant to not send some commitments in some edge cases due to crashes.
  *   </li>
  *
  *   <li>Finally, the class supports pruning: it computes the safe timestamps for participant pruning, such
  *     that, after pruning, non-repudiation still holds for any contract in the ACS
  *   </li>
  *  </ol>
  *
  *  The first four pieces of class functionality must be appropriately synchronized:
  *
  *  <ol>
  *   <li>ACS commitments for a tick cannot be completely processed before the local commitment for that tick is computed.
  *      Note that the class cannot make many assumptions on the received commitments: the counter-participants can send
  *      them in any order, and they can either precede or lag behind the local commitment computations.
  *   </li>
  *
  *   <li>The recovery points must be chosen such that the participant computes its local commitments correctly, and
  *     never misses to compute a local commitment for every tick. Otherwise, the participant will start raising false
  *     alarms when remote commitments are received (either because it computes the wrong thing, or because it doesn't
  *     compute anything at all and thus doesn't expect to receive anything).
  *   </li>
  *  </ol>
  *
  *  Additionally, the startup procedure must ensure that:
  *
  *  <ol>
  *    <li> [[processBatch]] is called for every sequencer message that contains commitment messages and whose handling
  *    hasn't yet completed successfully
  *    <li> publish is called for every change to the ACS after
  *    [[com.digitalasset.canton.participant.store.IncrementalCommitmentStore.watermark]]. where the request counter
  *    is to be used as a tie-breaker.
  *    </li>
  *  </ol>
  *
  * On first time startup (no entries in database) a single outstanding row will be generated for a previous fictive period, of length one reconciliation interval;
  * this is to generate a starting point going forward.
  *
  *  Finally, the class requires the reconciliation interval to be a multiple of 1 second.
  *
  * The ``commitmentPeriodObserver`` is called whenever a commitment is computed for a period, except if the participant crashes.
  * If publish is called multiple times for the same timestamp (once before a crash and once after the recovery),
  * the observer may also be called twice for the same period.
  *
  * When a participant's ACS commitment processor falls behind some counter participants' processors, the participant
  * has the option to enter a so-called "catch-up mode". In catch-up mode, the participant skips sending and
  * checking commitments for some reconciliation intervals. The parameter governing catch-up mode is:
  *
  * @param maxCommitmentSendDelayMillis Optional parameter to specify the maximum delay in milliseconds for sending out
  *                                  commitments. To avoid a spike in network activity at the end of each reconciliation
  *                                  period, commitment sending is delayed by default with a random amount uniformly
  *                                  distributed between 0 and max.  Commitment sending should not be delayed by more
  *                                  than a reconciliation interval, for two reasons: (1) it'll overlap with the
  *                                  next round of commitment sends, defeating somewhat the purpose of delaying commitment
  *                                  sends; and (2) and might not interact well with the catch-up mode, depending on the
  *                                  parameters there.
  *                                  If this is not specified, the maximum delay is testingConfig.maxCommitmentSendDelayMillis
  *                                  if specified, otherwise the maximum delay is 2/3 of the reconciliation interval.
  *
  *   The constructor of this class is private. Instances of this class can only be created using
  *   [[AcsCommitmentProcessor.apply]], which in turn uses the Factory method in the companion object to ensure that
  *   the class is properly initialized.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class AcsCommitmentProcessor private (
    synchronizerId: SynchronizerId,
    participantId: ParticipantId,
    sequencerClient: SequencerClientSend,
    synchronizerCrypto: SyncCryptoClient[SyncCryptoApi],
    sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
    store: AcsCommitmentStore,
    pruningObserver: TraceContext => Unit,
    metrics: CommitmentMetrics,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    activeContractStore: ActiveContractStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    /* An in-memory, mutable running ACS snapshot, updated on every call to [[publish]]  */
    val runningCommitments: RunningCommitments,
    endLastProcessedPeriod: Option[CantonTimestampSecond],
    enableAdditionalConsistencyChecks: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    exitOnFatalFailures: Boolean,
    maxCommitmentSendDelayMillis: Option[NonNegativeInt],
)(implicit ec: ExecutionContext)
    extends AcsChangeListener
    with FlagCloseable
    with NamedLogging {

  import AcsCommitmentProcessor.{errorLoggingContextFromNamedLoggingContext as _, *}

  private[canton] val healthComponent = new AcsCommitmentProcessorHealth(
    AcsCommitmentProcessor.healthName,
    this,
    logger,
  )

  // As the commitment computation is in the worst case expected to last the same order of magnitude as the
  // reconciliation interval, wait for at least that long
  override protected def closingTimeout: FiniteDuration = {
    // If we don't have any, nothing around commitment processing happened, so we take 0
    val latestReconciliationInterval =
      sortedReconciliationIntervalsProvider.getApproximateLatestReconciliationInterval
        .map(_.intervalLength.toScala)
        .getOrElse(Duration.Zero)

    super.closingTimeout.max(latestReconciliationInterval)
  }

  /** The parallelism to use when computing commitments */
  private val threadCount: PositiveNumeric[Int] = {
    val count = Threading.detectNumberOfThreads(noTracingLogger)
    noTracingLogger.info(s"Will use parallelism $count when computing ACS commitments")
    PositiveNumeric.tryCreate(count)
  }

  // used to generate randomized commitment sending delays
  private val rand = new scala.util.Random

  /** The sequencer timestamp for which we are ready to process remote commitments.
    *     Continuously updated as new local commitments are computed.
    *     All received remote commitments with the timestamp lower than this one will either have been processed or queued.
    *     Note that we don't guarantee that every remote commitment will be processed once this moves. However, such
    *     commitments will not be lost, as they will be put in the persistent buffer and get picked up by `processBuffered`
    *     eventually.
    */
  private val readyForRemote: AtomicReference[Option[CantonTimestampSecond]] =
    new AtomicReference[Option[CantonTimestampSecond]](endLastProcessedPeriod)

  /**  Denotes the last period queued for processing.
    *  Always increasing because we schedule periods for processing in monotonically order of their timestamps.
    */
  @volatile private[this] var endOfLastPeriod: Option[CantonTimestampSecond] =
    endLastProcessedPeriod

  /** End of the last period until which we have processed, sent and persisted all local and remote commitments.
    *     It's accessed only through chained futures, such that all accesses are synchronized
    */
  @volatile private[this] var endOfLastProcessedPeriod: Option[CantonTimestampSecond] =
    endLastProcessedPeriod

  /** In contrast to `endOfLastProcessedPeriod`, during catch-up, a period is considered processed when its commitment
    *     is computed, but not necessarily sent. Thus, we use a new variable `endOfLastProcessedPeriodDuringCatchUp`.
    *     Used in `processCompletedPeriod` to compute the correct reconciliation interval to be processed.
    */
  @volatile private[this] var endOfLastProcessedPeriodDuringCatchUp: Option[CantonTimestampSecond] =
    None

  /** During a coarse-grained catch-up interval, [[runningCmtSnapshotsForCatchUp]] stores in memory the snapshots for the
    * fine-grained reconciliation periods. In case of a commitment mismatch at the end of a catch-up interval, the
    * participant uses these snapshots in the function [[sendCommitmentMessagesInCatchUpInterval]] to compute
    * fine-grained commitments and send them to the counter-participant, which enables more precise detection of the
    * interval when ACS divergence happened.
    */
  private val runningCmtSnapshotsForCatchUp =
    scala.collection.mutable.Map
      .empty[CommitmentPeriod, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType]]

  /** A list containing the last timestamp for a received message per counter participant per synchronizer. */
  private val counterParticipantLastMessage =
    TrieMap
      .empty[ParticipantId, CantonTimestamp]

  /** a separate class that keeps track of multi hosted parties and if a period can be considered done */
  @VisibleForTesting
  private[pruning] val multiHostedPartyTracker =
    new AcsCommitmentMultiHostedPartyTracker(participantId, timeouts, loggerFactory)

  /** A future checking whether the node should enter catch-up mode by computing the catch-up timestamp.
    * At most one future runs computing this
    */
  private var computingCatchUpTimestamp: FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(None)

  private val cachedCommitments: Option[CachedCommitments] =
    if (testingConfig.doNotUseCommitmentCachingFor.contains(participantId.identifier.str))
      None
    else Some(new CachedCommitments())

  private val cachedCommitmentsForRetroactiveSends: CachedCommitments = new CachedCommitments()

  private val timestampsWithPotentialTopologyChanges =
    new AtomicReference[List[Traced[EffectiveTime]]](List())

  /** Queue to serialize the access to the DB, to avoid serialization failures at SERIALIZABLE level */
  private val dbQueue: SimpleExecutionQueue =
    new SimpleExecutionQueue(
      "acs-commitment-processor-queue",
      futureSupervisor,
      timeouts,
      loggerFactory,
      logTaskTiming = true,
      crashOnFailure = exitOnFatalFailures,
    )

  /** Queue to serialize the publication of ACS changes */
  private val publishQueue: SimpleExecutionQueue =
    new SimpleExecutionQueue(
      "acs-commitment-processor-publish-queue",
      futureSupervisor,
      timeouts,
      loggerFactory,
      logTaskTiming = true,
      crashOnFailure = exitOnFatalFailures,
    )

  private def processBufferedAtInit(
      timestamp: Option[CantonTimestampSecond]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    dbQueue.executeUS(
      timestamp.traverse_(ts => processBuffered(ts, endExclusive = false)),
      "processBufferedAtInit",
    )

  private def getReconciliationIntervals(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedReconciliationIntervals] =
    performUnlessClosingUSF(functionFullName)(
      sortedReconciliationIntervalsProvider.reconciliationIntervals(validAt)
    )

  @volatile private[this] var lastPublished: Option[RecordTime] = None

  /** Indicates what timestamp the participant catches up to. */
  @volatile private[this] var catchUpToTimestamp = CantonTimestamp.MinValue

  private[pruning] def catchUpConfig(
      cantonTimestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsCommitmentsCatchUpParameters]] =
    for {
      snapshot <- synchronizerCrypto.ipsSnapshot(cantonTimestamp)
      config <-
        snapshot.findDynamicSynchronizerParametersOrDefault(
          protocolVersion,
          warnOnUsingDefault = false,
        )

    } yield { config.acsCommitmentsCatchUp }

  private def catchUpEnabled(cfg: Option[AcsCommitmentsCatchUpParameters]): Boolean =
    cfg.exists(_.isCatchUpEnabled())

  private def catchUpInProgress(crtTimestamp: CantonTimestamp, catchUpToTs: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    for {
      config <- catchUpConfig(crtTimestamp)
    } yield catchUpEnabled(config) && catchUpToTs >= crtTimestamp

  private def caughtUpToBoundary(timestamp: CantonTimestamp, catchUpToTs: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    for {
      config <- catchUpConfig(timestamp)
      sortedReconciliationIntervals <- sortedReconciliationIntervalsProvider
        .reconciliationIntervals(timestamp)
    } yield config.exists(cfg =>
      sortedReconciliationIntervals.intervals.headOption match {
        case Some(interval) =>
          (timestamp.getEpochSecond % (cfg.catchUpIntervalSkip.value * interval.intervalLength.duration.getSeconds) == 0) && timestamp <= catchUpToTs
        case None =>
          throw new IllegalStateException(
            s"Cannot determine catch-up boundary: No valid reconciliation interval at time $timestamp"
          )
      }
    )

  /** Detects whether the participant is lagging too far behind (w.r.t. the catchUp config) in commitment computation.
    * If lagging behind, the method returns a new catch-up timestamp, otherwise it returns None.
    * It is up to the caller to update the [[catchUpToTimestamp]] accordingly.
    *
    * Note that, even if the participant is not lagging too far behind, it does not mean it "caught up".
    * In particular, if the participant's current timestamp is behind [[catchUpToTimestamp]], then the participant is
    * still catching up. Please use the method [[catchUpInProgress]] to determine whether the participant has caught up.
    */
  private def computeCatchUpTimestamp(
      completedPeriodTimestamp: CantonTimestamp,
      config: Option[AcsCommitmentsCatchUpParameters],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    for {
      catchUpBoundaryTimestamp <- laggingTooFarBehind(completedPeriodTimestamp, config)
    } yield {
      if (catchUpEnabled(config) && catchUpBoundaryTimestamp.nonEmpty) {
        logger.debug(
          s"Computed catch up boundary when processing end of period $completedPeriodTimestamp: computed catch-up timestamp is $catchUpBoundaryTimestamp"
        )
        catchUpBoundaryTimestamp
      } else None
    }

  def initializeTicksOnStartup(
      timestamps: List[EffectiveTime]
  )(implicit traceContext: TraceContext): Unit = {
    // assuming timestamps to be ordered
    val cur = timestampsWithPotentialTopologyChanges.getAndSet(timestamps.map(Traced(_)))
    ErrorUtil.requireArgument(
      cur.isEmpty,
      s"Bad initialization attempt of timestamps with ticks, as we've already scheduled ${cur.length} ",
    )
  }

  def scheduleTopologyTick(effectiveTime: Traced[EffectiveTime]): Unit =
    timestampsWithPotentialTopologyChanges.updateAndGet { cur =>
      // only append if this timestamp is higher than the last one (relevant during init)
      if (cur.lastOption.forall(_.value < effectiveTime.value)) cur :+ effectiveTime
      else cur
    }.discard

  override def publish(toc: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Unit =
    publishInternal(
      toc,
      () => FutureUnlessShutdown.pure(acsChange),
    )

  override def publish(
      sequencerTimestamp: CantonTimestamp,
      requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)],
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val toc = RecordTime(
      sequencerTimestamp,
      requestCounterCommitSetPairO.map(_._1.unwrap).getOrElse(RecordTime.lowestTiebreaker),
    )
    publishInternal(
      toc,
      () => computeAcsChange(requestCounterCommitSetPairO),
    )
  }

  private def computeAcsChange(requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[AcsChange] = {
    // If the requestCounterCommitSetPairO is not set, then by default the commit set is empty, and
    // the request counter is the smallest possible value that does not throw an exception in
    // ActiveContractStore.bulkContractsReassignmentCounterSnapshot, i.e., Genesis
    val (requestCounter, commitSet) =
      requestCounterCommitSetPairO.getOrElse((RequestCounter.Genesis, CommitSet.empty))
    // Augments the commit set with the updated reassignment counters for archive events,
    // computes the acs change and publishes it
    logger.trace(
      show"The received commit set contains creations ${commitSet.creations}" +
        show"assignments ${commitSet.assignments}" +
        show"archivals ${commitSet.archivals} unassignments ${commitSet.unassignments}"
    )

    val transientArchivals = reassignmentCountersForArchivedTransient(commitSet)

    val acsChangePublish =
      for {
        // Retrieves the reassignment counters of the archived contracts from the latest state in the active contract store
        archivalsWithReassignmentCountersOnly <- activeContractStore
          .bulkContractsReassignmentCounterSnapshot(
            commitSet.archivals.keySet -- transientArchivals.keySet,
            requestCounter,
          )

      } yield {
        // Computes the ACS change by decorating the archive events in the commit set with their reassignment counters
        val acsChange = AcsChange.tryFromCommitSet(
          commitSet,
          archivalsWithReassignmentCountersOnly,
          transientArchivals,
        )
        // we only log the full list of changes on trace level
        logger.trace(
          s"Computed ACS change activations ${acsChange.activations} deactivations ${acsChange.deactivations}"
        )
        acsChange
      }
    acsChangePublish
  }

  private def publishInternal(toc: RecordTime, acsChangeF: () => FutureUnlessShutdown[AcsChange])(
      implicit traceContext: TraceContext
  ): Unit = {
    @tailrec
    def go(): Unit =
      timestampsWithPotentialTopologyChanges.get().headOption match {
        // no upcoming topology change queued
        case None => publishTick(toc, acsChangeF)
        // pre-insert topology change queued
        case Some(traced @ Traced(effectiveTime)) if effectiveTime.value <= toc.timestamp =>
          // remove the tick from our update
          timestampsWithPotentialTopologyChanges.updateAndGet(_.drop(1))
          // only update if this is a separate timestamp
          if (
            effectiveTime.value < toc.timestamp && lastPublished.exists(
              _.timestamp < effectiveTime.value
            )
          ) {
            publishTick(
              RecordTime(timestamp = effectiveTime, tieBreaker = 0),
              () => FutureUnlessShutdown.pure(AcsChange.empty),
            )(traced.traceContext)
          }
          // now, iterate (there might have been several effective time updates)
          go()
        case Some(_) =>
          publishTick(toc, acsChangeF)
      }
    go()
  }

  /** Event processing consists of two steps: one (primarily) for computing local commitments, and one for handling remote ones.
    * This is the "local" processing, however, it does also process remote commitments in one case: when they arrive before the corresponding
    * local ones have been computed (in which case they are buffered).
    *
    * The caller(s) must jointly ensure that:
    * 1. [[publish]] is called with a strictly lexicographically increasing combination of timestamp/tiebreaker within
    *    a "crash-epoch". I.e., the timestamp/tiebreaker combination may only decrease across participant crashes/restarts.
    *    Note that the tie-breaker can change non-monotonically between two calls to publish. The tie-breaker is introduced
    *    to handle repair requests, as these may cause several changes to have the same timestamp.
    *    Actual ACS changes (e.g., due to transactions) use their request counter as the tie-breaker, while other
    *    updates (e.g., heartbeats) that only update the current time can set the tie-breaker to 0
    * 2. after publish is first called within a participant's "crash-epoch" with timestamp `ts` and tie-breaker `tb`, all subsequent changes
    *    to the ACS are also published (no gaps), and in the record order
    * 3. on startup, [[publish]] is called for all changes that are later than the watermark returned by
    *    [[com.digitalasset.canton.participant.store.IncrementalCommitmentStore.watermark]]. It may also be called for
    *    changes that are earlier than this timestamp (these calls will be ignored).
    *
    * Processing is implemented as a [[com.digitalasset.canton.util.SimpleExecutionQueue]] and driven by publish calls
    * made by the RecordOrderPublisher.
    *
    * ACS commitments at a tick become computable once an event with a timestamp larger than the tick appears
    *
    * *** Catch-up logic ***
    *
    * The participant maintains a [[catchUpToTimestamp]], which records the timestamp to which the participant is performing
    * a catch-up. `catchUpToTimestamp` is strictly increasing, and never decreases. If the participant's timestamp is
    * past `catchUpToTimestamp`, then no catch-up is in progress.
    *
    * In the beginning of the processing, the participant computes a catch-up timestamp in [[computeCatchUpTimestamp]].
    * [[computeCatchUpTimestamp]] checks whether the incoming commitments queue has commitments with timestamps that
    * are ahead the participant's end of period timestamp more than a threshold.
    * The participant correspondingly updates `catchUpToTimestamp` if the computation finishes before processing this
    * period. This means, either all period processing considers the catch-up active, or not.  as part of the main
    * processing in [[publish]]. [[checkAndTriggerCatchUpMode]] checks whether the participant received commitments
    * from a period that's significantly ahead the participant's current period.
    *
    * During catch-up mode, the participant computes, stores and sends commitments to counter-participants only at
    * catch-up interval boundaries, instead of at each reconciliation tick. In case of a commitment mismatch during
    * catch-up at a catch-up interval boundary, the participant sends out commitments for each reconciliation
    * interval covered by the catch-up period to those counter-participants (configurable)
    *        (a) whose catch-up interval boundary commitments do not match
    *        (b) *** default *** whose catch-up interval boundary commitments do not match or who haven't sent a
    *        catch-up interval boundary commitment yet
    */
  private def publishTick(toc: RecordTime, acsChangeF: () => FutureUnlessShutdown[AcsChange])(
      implicit traceContext: TraceContext
  ): Unit = {
    if (!lastPublished.forall(_ < toc))
      throw new IllegalStateException(
        s"Publish called with non-increasing record time, $toc (old was $lastPublished)"
      )
    lastPublished = Some(toc)

    def processCompletedPeriod(
        snapshotRes: CommitmentSnapshot
    )(
        completedPeriod: CommitmentPeriod
    ): FutureUnlessShutdown[Unit] = {
      for {
        config <- catchUpConfig(completedPeriod.toInclusive.forgetRefinement)

        // Evaluate in the beginning the catch-up conditions for simplicity
        catchingUpInProgress <- catchUpInProgress(
          completedPeriod.toInclusive.forgetRefinement,
          catchUpToTimestamp,
        )
        hasCaughtUpToBoundaryRes <- caughtUpToBoundary(
          completedPeriod.toInclusive.forgetRefinement,
          catchUpToTimestamp,
        )

        _ = if (catchingUpInProgress && healthComponent.isOk) {
          metrics.catchupModeEnabled.mark()
          logger.debug(s"Entered catch-up mode with config ${config.toString}")
          if (config.exists(cfg => cfg.catchUpIntervalSkip.value == 1))
            healthComponent.degradationOccurred(
              DegradationError.AcsCommitmentDegradationWithIneffectiveConfig.Report()
            )
          else {
            // we warn on degradation if specifically enables in tests, or if we are not running a test,
            // which is true if enableAdditionalConsistencyChecks is false
            if (testingConfig.warnOnAcsCommitmentDegradation) {
              healthComponent.degradationOccurred(
                DegradationError.AcsCommitmentDegradation.Report()
              )
            } else {
              val logMsg =
                DegradationError.AcsCommitmentDegradation.id + ": The participant has activated ACS catchup mode to combat computation problem."
              healthComponent.degradationOccurred(logMsg)
              logger.debug(logMsg)
            }
          }
        }

        _ = logger.debug(
          show"Processing completed period $completedPeriod. Modes: in catch-up mode = $catchingUpInProgress, " +
            show"and if yes, caught up to catch-up boundary $hasCaughtUpToBoundaryRes"
        )

        // If there is a commitment mismatch at the end of the catch-up period, we need to send fine-grained commitments
        // starting at `endOfLastProcessedPeriod` and ending at `endOfLastProcessedPeriodDuringCatchUp` for all
        // reconciliation intervals covered by the catch-up period.
        // However, `endOfLastProcessedPeriod` and `endOfLastProcessedPeriodDuringCatchUp` are updated when marking
        // the period as processed during catch-up, and when processing a catch-up, respectively, therefore
        // we save their prior values.
        lastSentCatchUpCommitmentTimestamp = endOfLastProcessedPeriod
        lastProcessedCatchUpCommitmentTimestamp = endOfLastProcessedPeriodDuringCatchUp

        _ = logger.debug(
          show"Commitment snapshot for completed period $completedPeriod: $snapshotRes"
        )

        // Detect possible inconsistencies of the running commitments and the ACS state
        // Runs only when enableAdditionalConsistencyChecks is true
        // *** Should not be enabled in production ***
        _ <-
          checkRunningCommitmentsAgainstACS(
            snapshotRes.active,
            activeContractStore,
            contractStore,
            enableAdditionalConsistencyChecks,
            completedPeriod.toInclusive.forgetRefinement,
          )

        _ <-
          if (!catchingUpInProgress || hasCaughtUpToBoundaryRes) {
            for {
              msgs <- commitmentMessages(completedPeriod, snapshotRes.active)
              _ = logger.debug(
                show"Commitment messages for $completedPeriod: ${msgs.fmap(_.commitment)}"
              )
              _ <- storeCommitments(msgs)
              _ = logger.debug(
                s"Computed and stored ${msgs.size} commitment messages for period $completedPeriod"
              )
              _ <- MarkOutstandingIfNonEmpty(completedPeriod, msgs.keySet)
              _ <- persistRunningCommitments(snapshotRes)
            } yield {
              sendCommitmentMessages(completedPeriod, msgs)
            }
          } else FutureUnlessShutdown.unit

        _ <-
          if (catchingUpInProgress) {
            for {
              _ <-
                if (!hasCaughtUpToBoundaryRes) {
                  for {
                    // persist running commitments for crash recovery
                    _ <- persistRunningCommitments(snapshotRes)
                    _ <- persistCatchUpPeriod(completedPeriod)
                  } yield {
                    // store running commitments in memory, in order to compute and send fine-grained commitments
                    // if a mismatch appears after catch-up
                    runningCmtSnapshotsForCatchUp += completedPeriod -> snapshotRes.active
                  }
                } else FutureUnlessShutdown.unit
              // mark the period as processed during catch-up
              _ = endOfLastProcessedPeriodDuringCatchUp = Some(completedPeriod.toInclusive)
            } yield ()
          } else FutureUnlessShutdown.unit

        _ <-
          if (!catchingUpInProgress) {
            healthComponent.resolveUnhealthy()
            for {
              noWait <- acsCounterParticipantConfigStore.getAllActiveNoWaitCounterParticipants(
                Seq(synchronizerId),
                Seq.empty,
              )
              topoSnapshot <- synchronizerCrypto.ipsSnapshot(
                completedPeriod.fromExclusive.forgetRefinement
              )
              _ <- multiHostedPartyTracker.trackPeriod(
                completedPeriod,
                topoSnapshot,
                snapshotRes,
                noWait.map(_.participantId).toSet,
              )
              _ = indicateReadyForRemote(completedPeriod.toInclusive)
              _ <- processBuffered(completedPeriod.toInclusive, endExclusive = false)
              _ <- indicateLocallyProcessed(completedPeriod)
            } yield ()
          } else FutureUnlessShutdown.unit

        // we only send commitments when no catch-up is in progress or at coarse-grain catch-up interval limit.
        _ <-
          if (hasCaughtUpToBoundaryRes) {
            for {
              _ <- checkMatchAndMarkSafeOrFixDuringCatchUp(
                lastSentCatchUpCommitmentTimestamp,
                lastProcessedCatchUpCommitmentTimestamp,
                completedPeriod,
              )
              // The ordering here is important; we shouldn't move `readyForRemote` before we mark the periods as outstanding,
              // as otherwise we can get a race where an incoming commitment doesn't "clear" the outstanding period
              _ = indicateReadyForRemote(completedPeriod.toInclusive)
              // Processes buffered counter-commitments for the catch-up period and compares them with local commitments,
              // which are available if there was a mismatch at the catch-up boundary
              // Ignore the buffered commitment at the boundary
              _ <- processBuffered(completedPeriod.toInclusive, endExclusive = true)
              // *After the above check* (the order matters), mark all reconciliation intervals as locally processed.
              _ <- indicateLocallyProcessed(completedPeriod)
              // clear the commitment snapshot in memory once we caught up
              _ = runningCmtSnapshotsForCatchUp.clear()
              _ = cachedCommitmentsForRetroactiveSends.clear()
            } yield ()
          } else FutureUnlessShutdown.unit

      } yield {
        // Inform the commitment period observer that we have completed the commitment period.
        // The invocation is quick and will schedule the processing in the background
        // It only kicks of journal pruning
        pruningObserver(traceContext)
      }
    }

    // On the `publishQueue`, obtain the running commitment, the reconciliation parameters, and topology snapshot,
    // and check whether this is a replay of something we've already seen. If not, then do publish the change,
    // which runs on the `dbQueue`.
    val fut = publishQueue
      .executeUS(
        for {
          // During crash recovery, it should be that only in tests could we have the situation where we replay
          // ACS changes while the ledger end lags behind the replayed change timestamp. In normal processing,
          // we publish ACS changes only after the ledger end has moved, which should mean that all topology events
          // for a given timestamp have been processed before processing the ACS change for the same timestamp.
          //
          // In that former case, when the ledger end lags behind, the topology client may be able to serve
          // the intervals and snapshots for re-published ACS changes only after some messages have been processed,
          // which may include ACS commitments that go through the same queue. Therefore, we serialize the access to
          // the DB only after having obtained the reconciliation intervals and topology snapshot.
          _ <-
            if (runningCommitments.watermark >= toc) {
              logger.debug(s"ACS change at $toc is a replay, treating it as a no-op")
              // This is a replay of an already processed ACS change, ignore
              FutureUnlessShutdown.unit
            } else {

              for {
                acsChange <- acsChangeF()
                reconciliationIntervals <- getReconciliationIntervals(toc.timestamp)
                periodEndO = reconciliationIntervals.tickBefore(toc.timestamp)

                processPeriodEnd <- periodEndO.traverse_ { crtPeriodEnd =>
                  // This is merely the last period end that we have queued to process, but it does not necessarily mean that
                  // we have computed and stored the local commitments at endOfLastPeriod.
                  // However, [[processCompletedPeriod]] has an immutable snapshot at time endOfLastPeriod, so it is safe
                  // to update the running commitments irrespective of the status of the local commitments.
                  val prevPeriodEnd = endOfLastPeriod
                  // Check whether this ACS change timestamp pushes us to a new commitment period; if so, the previous one is completed
                  val completedPeriod = reconciliationIntervals
                    .commitmentPeriodPrecedingFixedLowerBound(crtPeriodEnd, prevPeriodEnd)
                  for {
                    config <- catchUpConfig(crtPeriodEnd.forgetRefinement)
                    // We update `catchUpTimestamp` only if the future computing `computedNewCatchUpTimestamp` has returned by
                    // this point. If `catchUpToTimestamp` is greater or equal to the participant's end of period, then the
                    // participant enters catch up mode up to `catchUpTimestamp`.
                    // Important: `catchUpToTimestamp` is not updated concurrently, because the code below runs
                    // sequentially on the `publishQueue`. Moreover, the `publishQueue` inserts `periodEnd`
                    // sequentially in the order of the timestamps, which is the key to ensuring that the catch-up timestamp
                    // grows monotonically and that catch-ups are towards the future.
                    _ = if (config.exists(_.isCatchUpEnabled())) {
                      computingCatchUpTimestamp.unwrap.value.foreach { v =>
                        v.fold(
                          exc => logger.error(s"Error when computing the catch up timestamp", exc),
                          {
                            case Outcome(res) => res.foreach(ts => catchUpToTimestamp = ts)
                            case AbortedDueToShutdown =>
                              logger.debug(
                                "Could not compute catch up timestamp because shutting down"
                              )
                          },
                        )
                        computingCatchUpTimestamp = computeCatchUpTimestamp(
                          crtPeriodEnd.forgetRefinement,
                          config,
                        )
                      }
                    }

                    res <- completedPeriod match {
                      case Some(commitmentPeriod) =>
                        endOfLastPeriod = Some(commitmentPeriod.toInclusive)
                        // do not chain: make sure it gets scheduled, but don't wait for it to be executed

                        // Important: the running commitments must have been updated with the changes up to but not including
                        // the current ACS change at toc.timestamp, because updating commitments happens on the "publish queue" in
                        // increasing order of changes
                        // If the current ACS change moves us past a reconciliation boundary t, it is critical for correctness
                        // to first snapshot the running commitments before updating them. Otherwise, we lose the
                        // ability to compute the commitments at the reconciliation boundary t.
                        // Also, we need to ensure that we take the snapshot before we schedule the processing of the completed
                        // period, otherwise the snapshot for the period might not have all updates.
                        val dbQueueRes = dbQueue.executeUS(
                          processCompletedPeriod(runningCommitments.snapshot())(
                            commitmentPeriod
                          ),
                          s"process completed period as a result of time of change $toc",
                        )
                        dbQueueRes
                      case None =>
                        logger.debug(
                          s"The change $acsChange at $toc does not lead to a new commitment period."
                        )
                        FutureUnlessShutdown.unit
                    }
                  } yield res
                }
                // Add the changes to the running commitments regardless of whether the change leads to a new period
                _ = updateRunningCommitments(toc, acsChange)
              } yield processPeriodEnd
            }
        } yield (),
        s"publish ACS change at $toc",
      )

    FutureUtil.doNotAwait(
      fut.onShutdown(
        logger.info("Giving up on producing ACS commitment due to shutdown")
      ),
      // If this happens, then the failure is fatal or there is some bug in the queuing or retrying.
      // Unfortunately, we can't do anything anymore to reliably prevent corruption of the running snapshot in the DB,
      // as the data may already be corrupted by now.
      failureMessage = s"Producing ACS commitments failed.",
      // If the failure is due to the DB instance transitioning to pasive, then failing to produce commitments is
      // expected, and we shouldn't log it as an error.
      logPassiveInstanceAtInfo = true,
    )
  }

  def processBatch(
      timestamp: CantonTimestamp,
      batch: Traced[Seq[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]],
  ): FutureUnlessShutdown[Unit] =
    batch.withTraceContext(implicit traceContext => processBatchInternal(timestamp, _))

  /** Process incoming commitments.
    *
    * The caller(s) must jointly ensure that all incoming commitments are passed to this method, in their order
    * of arrival. Upon startup, the method must be called on all incoming commitments whose processing hasn't
    * finished yet, including those whose processing has been aborted due to shutdown.
    *
    *    There is no special catch-up logic on the incoming queue, because processing was never a bottleneck here.
    *    However, the incoming queue is important because it gives us the condition to initiate catch-up by allowing us
    *    to look at the timestamp of received commitments.
    *    Should processing of incoming commitments become a bottleneck, we can do the following:
    *    - to quickly detect a possible catch-up condition, we validate incoming commitments (including signature) as they
    *    come and store them; the catch-up condition looks at the timestamp of incoming commitments in the queue
    *    - to enable match checks of local and remote commitments, in a separate thread continue processing the commitments
    *    by checking matches and buffering them if needed.
    *      - during catch-up, the processing order is first commitments at catch-up boundaries in increasing timestamp order,
    *      then other commitments in increasing timestamp order
    *      - outside catch-up, process commitments as they come
    */
  def processBatchInternal(
      timestamp: CantonTimestamp,
      batch: Seq[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    if (batch.lengthCompare(1) != 0) {
      Errors.InternalError
        .MultipleCommitmentsInBatch(synchronizerId, timestamp, batch.length)
        .discard
    }

    val future = for {
      allConfigs <- acsCounterParticipantConfigStore.fetchAllSlowCounterParticipantConfig()
      (configsForSlowCounterParticipants, configsForSynchronizerThreshold) = allConfigs
      allNoWaits <- acsCounterParticipantConfigStore.getAllActiveNoWaitCounterParticipants(
        Seq(synchronizerId),
        Seq.empty,
      )
      _ <- batch.parTraverse_ { envelope =>
        getReconciliationIntervals(
          envelope.protocolMessage.message.period.toInclusive.forgetRefinement
        )
          // TODO(#10790) Investigate whether we can validate and process the commitments asynchronously.
          .flatMap { reconciliationIntervals =>
            validateEnvelope(timestamp, envelope, reconciliationIntervals) match {
              case Right(()) =>
                checkSignedMessage(timestamp, envelope)

              case Left(errors) =>
                errors.toList.foreach(logger.error(_))
                FutureUnlessShutdown.unit
            }
          }
      }
      reconInterval <- getReconciliationIntervals(timestamp)
      _ = calculateParticipantLatencies(
        configsForSlowCounterParticipants,
        configsForSynchronizerThreshold,
        allNoWaits,
        reconInterval,
      )
    } yield ()

    FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
      future,
      failureMessage = s"Failed to process incoming commitment.",
      onFailure = _ => {
        // Close ourselves so that we don't process any more messages
        close()
      },
      logPassiveInstanceAtInfo = true,
    )
  }

  private def updateParticipantLatency(
      timestamp: CantonTimestamp,
      payload: AcsCommitment,
  ): Unit =
    if (counterParticipantLastMessage.get(payload.sender).fold(true)(_ < timestamp))
      counterParticipantLastMessage.addOne(payload.sender -> timestamp)

  private def calculateParticipantLatencies(
      slowConfigs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForSynchronizerThresholds],
      noWaits: Seq[ConfigForNoWaitCounterParticipants],
      reconIntervals: SortedReconciliationIntervals,
  ): Unit = {
    val newest = endOfLastProcessedPeriod.getOrElse(CantonTimestampSecond.MinValue).forgetRefinement
    val reconIntervalLength =
      reconIntervals.intervals.headOption.fold(1L)(_.intervalLength.duration.toMillis * 1000)

    val threshold = thresholds
      .find(cfg => cfg.synchronizerId == synchronizerId)
      .getOrElse(
        // we use a default value of 0 for threshold if no config have been provided
        ConfigForSynchronizerThresholds(synchronizerId, NonNegativeLong.zero, NonNegativeLong.zero)
      )

    val defaultMax = threshold.thresholdDefault.value * reconIntervalLength
    val distinguishedMax = threshold.thresholdDistinguished.value * reconIntervalLength

    val configuredParticipants = counterParticipantLastMessage
      .map { case (participantId, cts) =>
        val cfg = slowConfigs
          .find(cfg => cfg.participantId == participantId)
          .fold(
            slowConfigs
              .find(cfg => cfg.synchronizerId == synchronizerId)
              .map(default => default.copy(isDistinguished = false, isAddedToMetrics = false))
          )(Some(_))
        (
          participantId,
          // we floor it to 0, negative numbers here indicate we are lacking behind everybody else.
          math.max(0, newest.toMicros - cts.toMicros),
          cfg.fold(false)(_.isDistinguished),
          cfg.fold(false)(_.isAddedToMetrics),
        )
      }

    // Update the Non-Distinguished
    checkAndUpdateMetric(
      configuredParticipants
        .filter { case (participantId, timeDiff, isDistinguished, isAddedToMetrics) =>
          !isDistinguished && !isAddedToMetrics && !noWaits.exists(noWait =>
            noWait.participantId == participantId
          ) && timeDiff > defaultMax
        }
        .maxByOption { case (_, timeDiff, _, _) => timeDiff },
      metrics.largestCounterParticipantLatency.getValue,
      metrics.largestCounterParticipantLatency.updateValue,
    )

    // Update the Distinguished
    checkAndUpdateMetric(
      configuredParticipants
        .filter { case (_, timeDiff, isDistinguished, isAddedToMetrics) =>
          isDistinguished && !isAddedToMetrics && timeDiff > distinguishedMax
        }
        .maxByOption { case (_, timeDiff, _, _) => timeDiff },
      metrics.largestDistinguishedCounterParticipantLatency.getValue,
      metrics.largestDistinguishedCounterParticipantLatency.updateValue,
    )

    // update all the added to metric
    configuredParticipants
      .filter { case (_, _, _, isAddedToMetrics) => isAddedToMetrics }
      .foreach { case (participantId, timeDiff, _, _) =>
        metrics.counterParticipantLatency(participantId).updateValue(timeDiff)
      }
  }

  private def checkAndUpdateMetric(
      maxByTimeDiff: Option[(ParticipantId, Long, Boolean, Boolean)],
      currentValue: Long,
      update: Long => Unit,
  ): Unit =
    maxByTimeDiff match {
      case Some((_, timeDiff, _, _)) =>
        if (
          // this means that either, the slowest participant is falling further behind, or he is catching up and somebody new is the slowest
          timeDiff != currentValue
        ) {
          update(timeDiff)
        }
      case None =>
        // this means all participants have caught up
        if (currentValue != 0)
          update(0)
    }

  private def validateEnvelope(
      timestamp: CantonTimestamp,
      envelope: OpenEnvelope[SignedProtocolMessage[AcsCommitment]],
      reconciliationIntervals: SortedReconciliationIntervals,
  ): Either[NonEmptyList[String], Unit] = {
    val payload = envelope.protocolMessage.message

    def validate(valid: Boolean, error: => String): ValidatedNec[String, Unit] =
      if (valid) ().validNec else error.invalidNec

    val validRecipients = validate(
      envelope.recipients == Recipients.cc(participantId),
      s"At $timestamp, (purportedly) ${payload.sender} sent an ACS commitment to me, but addressed the message to ${envelope.recipients}",
    )

    val validCounterParticipant = validate(
      payload.counterParticipant == participantId,
      s"At $timestamp, (purportedly) ${payload.sender} sent an ACS commitment to me, but the commitment lists ${payload.counterParticipant} as the counterparticipant",
    )

    val commitmentPeriodEndsInPast = validate(
      payload.period.toInclusive <= timestamp,
      s"Received an ACS commitment with a future beforeAndAt timestamp. (Purported) sender: ${payload.sender}. Timestamp: ${payload.period.toInclusive}, receive timestamp: $timestamp",
    )

    val commitmentPeriodEndsAtTick =
      reconciliationIntervals.isAtTick(payload.period.toInclusive) match {
        case Some(true) => ().validNec
        case Some(false) =>
          s"finish time of received commitment period is not on a tick: ${payload.period}".invalidNec
        case None =>
          s"Unable to determine whether finish time of received commitment period is on a tick: ${payload.period}".invalidNec
      }

    (
      validRecipients,
      validCounterParticipant,
      commitmentPeriodEndsInPast,
      commitmentPeriodEndsAtTick,
    ).mapN((_, _, _, _) => ()).toEither.left.map(_.toNonEmptyList)
  }

  private def persistRunningCommitments(
      res: CommitmentSnapshot
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.runningCommitments
      .update(res.recordTime, res.delta, res.deleted)
      .map(_ => logger.debug(s"Persisted ACS commitments at ${res.recordTime} with $res"))

  /** Store special empty commitment to remember we were in catch-up mode,
    *  with the current participant as the counter-participant.
    *  Because the special commitment have the current participant as counter-participant, they do not conflict
    *  with "normal operation" commitments, which have other participants as counter-participants.
    */
  private def persistCatchUpPeriod(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val catchUpCmt = mkCommitment(participantId, AcsCommitmentProcessor.emptyCommitment, period)
    storeCommitments(Map(participantId -> catchUpCmt))
  }

  private def isCatchUpPeriod(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    for {
      possibleCatchUpCmts <- store.getComputed(period, participantId)
    } yield {
      val response = possibleCatchUpCmts.nonEmpty &&
        possibleCatchUpCmts.forall { case (_period, commitment) =>
          commitment == AcsCommitmentProcessor.emptyCommitment
        }
      logger.debug(
        s"Period $period is a catch-up period $response with the computed catch-up commitments $possibleCatchUpCmts"
      )
      response
    }

  private def updateRunningCommitments(rt: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      s"Applying ACS change at $rt: ${acsChange.activations.size} activated, ${acsChange.deactivations.size} archived"
    )
    runningCommitments.update(rt, acsChange)
  }

  @VisibleForTesting
  private[pruning] def indicateLocallyProcessed(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    endOfLastProcessedPeriod = Some(period.toInclusive)
    for {
      // mark that we're done with processing this period; safe to do at any point after the commitment has been sent
      // and the outstanding commitments stored
      _ <- store.markComputedAndSent(period)

      // delete the processed buffered commitments (safe to do after `processBuffered` completes)
      // In addition, for a correct catch-up after crash, we can delete processed buffered commitments only after
      // `markComputedAndSent`. Otherwise, it can happen that, upon recovery, we don't observe the catch-up condition
      // because we deleted the commitments in the queue, yet we did not mark the period as complete.
      // This means we process the period again as a non-catch-up period, which might have unexpected behavior.
      _ <- store.queue.deleteThrough(period.toInclusive.forgetRefinement)
    } yield {
      logger.debug(
        s"Deleted buffered commitments and set last computed and sent timestamp set to ${period.toInclusive}"
      )
    }
  }

  private def checkSignedMessage(
      timestamp: CantonTimestamp,
      envelope: OpenEnvelope[SignedProtocolMessage[AcsCommitment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val message = envelope.protocolMessage
    logger.debug(
      s"Checking commitment (purportedly by) ${message.message.sender} for period ${message.message.period}"
    )
    for {
      validSig <- checkCommitmentSignature(message)

      commitment = message.message

      // If signature passes, store such that we can prove Byzantine behavior if necessary
      _ <-
        if (validSig) for {
          _ <- store.storeReceived(message)
          _ <- checkCommitment(commitment)
        } yield ()
        else FutureUnlessShutdown.unit
    } yield {
      if (!validSig) {
        AcsCommitmentAlarm
          .Warn(
            s"""Received wrong signature for ACS commitment at timestamp $timestamp; purported sender: ${commitment.sender}; commitment: $commitment"""
          )
          .report()
      } else {
        updateParticipantLatency(timestamp, envelope.protocolMessage.message)
      }
    }
  }

  private def checkCommitmentSignature(
      message: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = {
    val cryptoSnapshot = synchronizerCrypto.currentSnapshotApproximation

    for {
      result <- message.verifySignature(cryptoSnapshot, message.typedMessage.content.sender).value
    } yield result
      .tapLeft(err => logger.error(s"Commitment signature verification failed with $err"))
      .isRight
  }

  private def checkCommitment(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    dbQueue
      .executeUS(
        {
          // Make sure that the ready-for-remote check is atomic with buffering the commitment
          val readyToCheck = readyForRemote.get().exists(_ >= commitment.period.toInclusive)
          if (readyToCheck) {
            // Do not sequentialize the checking
            FutureUnlessShutdown.pure(checkMatchAndMarkSafe(List(commitment)))
          } else {
            logger.debug(s"Buffering $commitment for later processing")
            store.queue.enqueue(commitment).map((_: Unit) => FutureUnlessShutdown.unit)
          }
        },
        s"check commitment readiness at ${commitment.period} by ${commitment.sender}",
      )
      .flatten

  private def indicateReadyForRemote(timestamp: CantonTimestampSecond)(implicit
      traceContext: TraceContext
  ): Unit = {
    val updated = readyForRemote.updateAndGet { oldTs =>
      oldTs.fold(Some(timestamp))(oldReadyForRemote =>
        if (oldReadyForRemote > timestamp) Some(oldReadyForRemote) else Some(timestamp)
      )
    }

    updated match {
      case Some(newTimestamp) =>
        if (newTimestamp != timestamp) {
          logger.debug(
            s"out of order timestamps when updating ready for remote in the commitment processor: $newTimestamp and $timestamp. Ready-for-remote remains at $newTimestamp"
          )
        }
      case None =>
    }
  }

  private def processBuffered(
      timestamp: CantonTimestampSecond,
      endExclusive: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Processing buffered commitments until $timestamp ${if (endExclusive) "exclusive"
      else "inclusive"}")
    for {
      toProcessInclusive <- store.queue.peekThrough(timestamp.forgetRefinement)
      toProcess =
        if (endExclusive) toProcessInclusive.filterNot(c => c.period.toInclusive == timestamp)
        else toProcessInclusive
      _ <- checkMatchAndMarkSafe(toProcess)
    } yield {
      logger.debug(
        s"Checked buffered remote commitments up to $timestamp ${if (endExclusive) "exclusive"
          else "inclusive"} and ready to check further ones without buffering"
      )
    }
  }

  /* Logs all necessary messages and returns whether the remote commitment matches the local ones */
  private def matches(
      remote: AcsCommitment,
      local: Iterable[(CommitmentPeriod, AcsCommitment.CommitmentType)],
      lastPruningTime: Option[CantonTimestamp],
      possibleCatchUp: Boolean,
  )(implicit traceContext: TraceContext): Boolean =
    if (local.isEmpty) {
      if (
        !possibleCatchUp && lastPruningTime.forall(_ < remote.period.toInclusive.forgetRefinement)
      ) {
        // We do not run in an infinite loop of sending empty commitments to each other:
        // If we receive an empty commitment from a counter-participant, it is because the current participant
        // sent a commitment to them when they didn't have a shared contract with the current participant
        // so normally local would not be empty for the current participant, and we would not be on this branch
        // It could, however, happen that a counter-participant, perhaps maliciously, sends a commitment despite
        // not having received a commitment from us; in this case, we simply reply with an empty commitment, but we
        // issue a mismatch only if the counter-commitment was not empty
        if (remote.commitment != LtHash16().getByteString())
          Errors.MismatchError.NoSharedContracts.Mismatch(synchronizerId, remote).report()

        // Due to the condition of this branch, in catch-up mode we don't reply with an empty commitment in between
        // catch-up boundaries. If the counter-participant thinks that there is still a shared contract at the
        // end of the catch-up boundary, we then reply with an empty commitment.
        val msg = mkCommitment(remote.sender, AcsCommitmentProcessor.emptyCommitment, remote.period)

        sendCommitmentMessages(remote.period, Map(remote.sender -> msg))
        logger.debug(
          s" ${remote.sender} send a non-empty ACS, but local ACS was empty. returned an empty ACS counter-commitment."
        )

      } else {
        if (lastPruningTime.exists(_ >= remote.period.toInclusive.forgetRefinement))
          logger.info(s"Ignoring incoming commitment for a pruned period: $remote")
        if (possibleCatchUp)
          logger.info(
            s"Ignoring incoming commitment for a skipped period due to catch-up: $remote"
          )
      }
      false
    } else {
      local.filter(_._2 != remote.commitment) match {
        case Nil =>
          lazy val logMsg: String =
            s"Commitment correct for sender ${remote.sender} and period ${remote.period}"
          logger.debug(logMsg)
          true
        case mismatches =>
          Errors.MismatchError.CommitmentsMismatch
            .Mismatch(synchronizerId, remote, mismatches.toSeq)
            .report()
          false
      }
    }

  private def checkMatchAndMarkSafe(
      remote: List[AcsCommitment]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Processing ${remote.size} remote commitments")
    remote.parTraverse_ { cmt =>
      for {
        commitments <- store.getComputed(cmt.period, cmt.sender)
        // check if we were in a catch-up phase
        possibleCatchUp <- isCatchUpPeriod(cmt.period)
        lastPruningTime <- store.pruningStatus
        _ <- markPeriods(cmt, commitments, lastPruningTime, possibleCatchUp)
      } yield ()
    }
  }

  /** Checks whether at the end of `completedPeriod` the commitments with the counter-participants match. In case of
    * mismatch, sends fine-grained commitments to enable fine-grained mismatch detection.
    *
    * @param filterInJustMismatches If true, send fine-grained commitments only to counter-participants from whom we
    *                               have mismatching cmts at the catch-up boundary. If false, send fine-grained
    *                               commitments to all counter-participants from whom we don't have cmts, or cmts do
    *                               not match at the catch-up boundary.
    */
  private def checkMatchAndMarkSafeOrFixDuringCatchUp(
      lastSentCatchUpCommitmentTimestamp: Option[CantonTimestampSecond],
      lastProcessedCatchUpCommitmentTimestamp: Option[CantonTimestampSecond],
      completedPeriod: CommitmentPeriod,
      filterInJustMismatches: Boolean = false,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"$participantId checkMatchAndMarkSafeOrFixDuringCatchUp for period $completedPeriod"
    )
    for {
      // retrieve commitments computed at catch-up boundary
      computed <- store.searchComputedBetween(
        completedPeriod.fromExclusive.forgetRefinement,
        completedPeriod.toInclusive.forgetRefinement,
      )

      intervals <- sortedReconciliationIntervalsProvider.computeReconciliationIntervalsCovering(
        completedPeriod.fromExclusive.forgetRefinement,
        completedPeriod.toInclusive.forgetRefinement,
      )

      _ <- MonadUtil.parTraverseWithLimit_(threadCount.value)(computed.toList) {
        case (period, counterParticipant, cmt) =>
          logger.debug(
            s"Processing own commitment $cmt for period $period and counter-participant $counterParticipant"
          )
          for {
            counterCommitmentList <- store.queue.peekOverlapsForCounterParticipant(
              period,
              counterParticipant,
            )(traceContext)

            lastPruningTime <- store.pruningStatus

            _ = if (counterCommitmentList.sizeIs > intervals.size) {
              AcsCommitmentAlarm
                .Warn(
                  s"""There should be at most ${intervals.size} commitments from counter-participant
                   |$counterParticipant covering the period ${completedPeriod.fromExclusive} to ${completedPeriod.toInclusive}),
                   |but we have the following ${counterCommitmentList.size}""".stripMargin
                )
                .report()
            }

            // check if we were in a catch-up phase
            possibleCatchUp <- isCatchUpPeriod(period)

            // get lists of counter-commitments that match and, respectively, do not match locally computed commitments
            (matching, mismatches) = counterCommitmentList.partition(counterCommitment =>
              matches(
                counterCommitment,
                List((period, cmt)),
                lastPruningTime.map(_.timestamp),
                possibleCatchUp,
              )
            )

            // we mark safe all matching counter-commitments
            _ <- matching.parTraverse_ { counterCommitment =>
              logger.debug(
                s"Marked as safe commitment $cmt against counterComm $counterCommitment"
              )
              val cmtPeriodsNE = NonEmptyUtil.fromElement(counterCommitment.period)
              for {
                _ <- multiHostMark(counterCommitment.sender, cmtPeriodsNE)
                _ <- store.markSafe(
                  counterCommitment.sender,
                  cmtPeriodsNE.toSet,
                )
              } yield ()

            }

            // if there is a mismatch, send all fine-grained commitments between `lastSentCatchUpCommitmentTimestamp`
            // and `lastProcessedCatchUpCommitmentTimestamp`
            _ <-
              if (mismatches.nonEmpty) {
                for {
                  res <-
                    if (!filterInJustMismatches) {
                      // send to all counter-participants from whom either I don't have cmts or I have cmts, but they don't match
                      sendCommitmentMessagesInCatchUpInterval(
                        lastSentCatchUpCommitmentTimestamp,
                        lastProcessedCatchUpCommitmentTimestamp,
                        filterOutParticipantId = matching.map(c => c.counterParticipant),
                      )
                    } else {
                      // send to all counter-participants from whom I have cmts, but they don't match
                      sendCommitmentMessagesInCatchUpInterval(
                        lastSentCatchUpCommitmentTimestamp,
                        lastProcessedCatchUpCommitmentTimestamp,
                        filterInParticipantId = mismatches.map(c => c.counterParticipant),
                        filterOutParticipantId = matching.map(c => c.counterParticipant),
                      )
                    }
                } yield res
              } else FutureUnlessShutdown.unit
          } yield ()

      }
    } yield ()
  }

  private def mkCommitment(
      counterParticipant: ParticipantId,
      cmt: CommitmentType,
      period: CommitmentPeriod,
  ) =
    AcsCommitment.create(
      synchronizerId,
      participantId,
      counterParticipant,
      period,
      cmt,
      protocolVersion,
    )

  /* Compute commitment messages to be sent for the ACS at the given timestamp. The snapshot is assumed to be ordered
   * by contract IDs (ascending or descending both work, but must be the same at all participants) */
  @VisibleForTesting
  private[pruning] def commitmentMessages(
      period: CommitmentPeriod,
      commitmentSnapshot: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, AcsCommitment]] = {
    logger.debug(
      s"Computing commitments for $period, number of stakeholder sets: ${commitmentSnapshot.keySet.size}"
    )
    for {
      cmts <-
        commitments(
          participantId,
          commitmentSnapshot,
          synchronizerCrypto,
          period.toInclusive,
          Some(metrics),
          threadCount,
          cachedCommitments.getOrElse(new CachedCommitments()),
        )

    } yield cmts.collect {
      case (counterParticipant, cmt) if LtHash16.isNonEmptyCommitment(cmt) =>
        counterParticipant -> mkCommitment(counterParticipant, cmt, period)
    }
  }

  /** Checks whether a participant whose processing timestamp is the end of the given period lags too far behind
    * a counter participant.
    * Lagging "too far behind" means that a received counter-commitment has a timestamp ahead of the participant's
    * current timestamp by at least reconciliation interval len * [[catchIpIntervalSkip]] * [[laggingBehindCatchUpTrigger]].
    * If reconciliation intervals are dynamic, the reconciliation interval len represents the interval len at the time
    * when the catch-up decision is taken.
    * @return The catch-up timestamp, if the node needs to catch-up, otherwise None.
    */
  private def laggingTooFarBehind(
      completedPeriodTimestamp: CantonTimestamp,
      config: Option[AcsCommitmentsCatchUpParameters],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    config.parFlatTraverse(cfg =>
      for {
        sortedReconciliationIntervals <- sortedReconciliationIntervalsProvider
          .reconciliationIntervals(completedPeriodTimestamp)
        catchUpTimestamp = sortedReconciliationIntervals.intervals.headOption.flatMap(interval =>
          try {
            val catchUpDelta =
              Math.multiplyExact(
                interval.intervalLength.duration.getSeconds,
                cfg.catchUpIntervalSkip.value * cfg.nrIntervalsToTriggerCatchUp.value,
              )
            Some(
              CantonTimestamp.ofEpochSecond(
                Math.addExact(
                  completedPeriodTimestamp.getEpochSecond,
                  // this is if the reconciliation interval changed
                  catchUpDelta - completedPeriodTimestamp.getEpochSecond % catchUpDelta,
                )
              )
            )
          } catch {
            case _: ArithmeticException =>
              throw new IllegalArgumentException(
                s"Overflow when computing the catchUp timestamp with catch up" +
                  s"parameters (${cfg.catchUpIntervalSkip}, ${cfg.nrIntervalsToTriggerCatchUp}) and" +
                  s"reconciliation interval ${interval.intervalLength.duration.getSeconds} seconds"
              )
          }
        )

        comm <- catchUpTimestamp.fold(FutureUnlessShutdown.pure(Seq.empty[AcsCommitment]))(ts =>
          store.queue.peekThroughAtOrAfter(ts)
        )

      } yield {
        if (comm.nonEmpty) catchUpTimestamp
        else None
      }
    )

  /** Store the computed commitments of the commitment messages */
  private def storeCommitments(
      msgs: Map[ParticipantId, AcsCommitment]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val items = msgs.map { case (pid, msg) =>
      AcsCommitmentStore.CommitmentData(pid, msg.period, msg.commitment)
    }
    NonEmpty.from(items.toList).fold(FutureUnlessShutdown.unit)(store.storeComputed(_))
  }

  /** Send the computed commitment messages */
  private def sendCommitmentMessages(
      period: CommitmentPeriod,
      msgs: Map[ParticipantId, AcsCommitment],
  )(implicit traceContext: TraceContext): Unit = {

    // delay sending out commitments by at most (in this order): maxCommitmentSendDelayMillis, or
    // testingConfig.maxCommitmentSendDelayMillis, or 2/3 of the reconciliation interval
    val maxDelayMillis =
      maxCommitmentSendDelayMillis.fold(testingConfig.maxCommitmentSendDelayMillis.fold({
        val latestReconciliationInterval =
          sortedReconciliationIntervalsProvider.getApproximateLatestReconciliationInterval
            .map(_.intervalLength.toScala)
            .getOrElse(Duration.Zero)
        2 * latestReconciliationInterval.toMillis.toInt / 3
      })(_.value))(_.value)
    val delayMillis = if (maxDelayMillis > 0) rand.nextInt(maxDelayMillis) else 0

    def sendUnlessClosing() = {
      implicit val metricsContext: MetricsContext = MetricsContext("type" -> "send-commitment")
      performUnlessClosingUSF(functionFullName) {
        def message = s"Failed to send commitment message batch for period $period"
        val cryptoSnapshot = synchronizerCrypto.currentSnapshotApproximation
        FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
          for {
            batchForm <- msgs.toSeq.parTraverse { case (participant, commitment) =>
              SignedProtocolMessage
                .trySignAndCreate(commitment, cryptoSnapshot, protocolVersion)
                .map(_ -> Recipients.cc(participant))
            }
            batch = Batch.of(protocolVersion, batchForm*)
            _ <- sequencerClient
              .sendAsync(
                batch,
                None,
                // ACS commitments are "best effort", so no need to amplify them
                amplify = false,
                callback = {
                  case UnlessShutdown.Outcome(SendResult.Success(deliver)) =>
                    val difference = deliver.timestamp.toMicros - period.toInclusive.toMicros
                    metrics.sequencingTime.updateValue(difference)
                  case _ => // Failed to sequence the message, so no update to the metric
                },
              )
              .leftMap {
                case RequestRefused(SendAsyncError.ShuttingDown(_)) =>
                  logger.info(
                    s"$message as the sequencer is shutting down. Once the sequencer is back, we'll recover."
                  )
                case other =>
                  logger.warn(s"$message: $other")
              }
              .value
          } yield (),
          message,
          logPassiveInstanceAtInfo = true,
        )
      }
    }

    if (msgs.nonEmpty) {
      FutureUnlessShutdownUtil
        .logOnFailureUnlessShutdown(
          clock
            .scheduleAfter(
              _ => sendUnlessClosing(),
              java.time.Duration.ofMillis(delayMillis.toLong),
            ),
          s"Failed to schedule sending commitment message batch for period $period at time ${clock.now
              .add(java.time.Duration.ofMillis(delayMillis.toLong))}",
          logPassiveInstanceAtInfo = true,
        )
        .discard
    }
  }

  /** Computes and sends commitment messages to counter-participants for all (period,snapshot) pairs in
    * [[runningCmtSnapshotsForCatchUp]] for the last catch-up interval.
    * The counter-participants are by default all counter-participant at the end of each interval, to which we apply the
    * filters `filterInParticipantId` and `filterOutParticipantId`.
    * These snapshots should cover the last catch-up interval, namely between `fromExclusive` to `toInclusive`,
    * otherwise we throw an [[IllegalStateException]].
    * The caller should ensure that `fromExclusive` and `toInclusive` represent valid reconciliation ticks.
    * If `fromExclusive` < `toInclusive`, we throw an [[IllegalStateException]].
    * If `fromExclusive` and/or `toInclusive` are None, they get the value CantonTimestampSecond.MinValue.
    */
  private def sendCommitmentMessagesInCatchUpInterval(
      fromExclusive: Option[CantonTimestampSecond],
      toInclusive: Option[CantonTimestampSecond],
      filterInParticipantId: Seq[ParticipantId] = Seq.empty,
      filterOutParticipantId: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val fromExclusiveSeconds = fromExclusive.getOrElse(CantonTimestampSecond.MinValue)
    val toInclusiveSeconds = toInclusive.getOrElse(CantonTimestampSecond.MinValue)

    if (fromExclusiveSeconds > toInclusiveSeconds)
      throw new IllegalStateException(
        s"$fromExclusive needs to be <= $toInclusive"
      )

    if (runningCmtSnapshotsForCatchUp.isEmpty && fromExclusiveSeconds < toInclusiveSeconds)
      throw new IllegalStateException(
        s"No active snapshots for catchupInterval [$fromExclusive, $toInclusive], but there should be"
      )

    if (runningCmtSnapshotsForCatchUp.nonEmpty && fromExclusiveSeconds == toInclusiveSeconds)
      throw new IllegalStateException(
        s"Active snapshots $runningCmtSnapshotsForCatchUp for catchupInterval [$fromExclusive, $toInclusive], but there shouldn't be"
      )

    val sortedPeriods = runningCmtSnapshotsForCatchUp.keySet.toList.sortBy(c => c.toInclusive)
    sortedPeriods.headOption.fold(()) { c =>
      if (c.fromExclusive != fromExclusiveSeconds)
        throw new IllegalStateException(
          s"Wrong fromExclusive ${c.fromExclusive} in the active commitment snapshots, should be $fromExclusiveSeconds"
        )
      else ()
    }

    sortedPeriods.lastOption.fold(()) { c =>
      if (c.toInclusive != toInclusiveSeconds)
        throw new IllegalStateException(
          s"Wrong toInclusive ${c.toInclusive} in the active commitment snapshots, should be $toInclusiveSeconds"
        )
      else ()
    }

    sortedPeriods.sliding(2).foreach {
      case Seq(prev, next) =>
        if (prev.toInclusive != next.fromExclusive)
          throw new IllegalStateException(
            s"Periods in active snapshots $runningCmtSnapshotsForCatchUp are not consecutive"
          )
      case _ =>
    }

    runningCmtSnapshotsForCatchUp
      .map { case (period, snapshot) =>
        if (period.fromExclusive < fromExclusiveSeconds)
          throw new IllegalStateException(
            s"Wrong fromExclusive ${period.fromExclusive} in the active commitment snapshots, min is $fromExclusiveSeconds"
          )
        if (period.toInclusive > toInclusiveSeconds)
          throw new IllegalStateException(
            s"Wrong toInclusive ${period.toInclusive} in the active commitment snapshots, max is $toInclusiveSeconds"
          )

        for {
          cmts <-
            commitments(
              participantId,
              snapshot,
              synchronizerCrypto,
              period.toInclusive,
              Some(metrics),
              threadCount,
              cachedCommitmentsForRetroactiveSends,
              filterInParticipantId,
              filterOutParticipantId,
            )

          _ = logger.debug(
            s"Due to mismatch, sending commitment for period $period to counterP ${cmts.keySet}"
          )

          msgs = cmts
            .collect {
              case (counterParticipant, cmt) if LtHash16.isNonEmptyCommitment(cmt) =>
                counterParticipant -> mkCommitment(counterParticipant, cmt, period)
            }
          _ <- storeCommitments(msgs)
          // TODO(i15333) batch more commitments and handle the case when we reach the maximum message limit.
          _ = sendCommitmentMessages(period, msgs)
        } yield ()
      }
      .toSeq
      .sequence_
  }

  /** takes a period and set of participants, handles splitting and conversion to NonEmpty
    * does nothing if the period is non-valid or the participant set is empty.
    */
  private def MarkOutstandingIfNonEmpty(
      completedPeriod: CommitmentPeriod,
      participants: Set[ParticipantId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      splitPeriod <- sortedReconciliationIntervalsProvider.splitCommitmentPeriod(
        completedPeriod
      )
      counterParticipantsNE = NonEmpty.from(participants)
      _ <- counterParticipantsNE.fold(FutureUnlessShutdown.unit)(counterParticipants =>
        splitPeriod.fold(FutureUnlessShutdown.unit)(period =>
          store.markOutstanding(
            period,
            counterParticipants,
          )
        )
      )
    } yield ()
  private def markPeriods(
      cmt: AcsCommitment,
      commitments: Iterable[(CommitmentPeriod, CommitmentType)],
      lastPruningTime: Option[PruningStatus],
      possibleCatchUp: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      splitPeriods <- sortedReconciliationIntervalsProvider.splitCommitmentPeriod(cmt.period)
      _ <- splitPeriods match {
        case Some(periods) =>
          if (matches(cmt, commitments, lastPruningTime.map(_.timestamp), possibleCatchUp)) {
            for {
              _ <- multiHostMark(cmt.sender, periods)
              _ <-
                store.markSafe(
                  cmt.sender,
                  periods,
                )
            } yield ()
          } else {
            store.markUnsafe(
              cmt.sender,
              periods,
            )
          }
        case None => FutureUnlessShutdown.unit
      }
    } yield ()

  private def multiHostMark(sender: ParticipantId, periods: NonEmpty[Set[CommitmentPeriod]])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown
      .sequence(
        multiHostedPartyTracker
          .newCommit(sender, periods)
          .map { case (period, state) =>
            state match {
              case TrackedPeriodState.Cleared => store.markMultiHostedCleared(period)
              case _ => FutureUnlessShutdown.unit
            }
          }
          .filter(_ =>
            true
          ) // this is to remove the NonEmpty, since the sequence does not support it.
      )
      .map(_ => ()) // maps FutureUnlessShutdown[Seq[Unit]] to FutureUnlessShutdown[Unit]

  override protected def onClosed(): Unit =
    LifeCycle.close(dbQueue, publishQueue)(logger)

  @VisibleForTesting
  private[pruning] def flush(): FutureUnlessShutdown[Unit] =
    // flatMap instead of zip because the `publishQueue` pushes tasks into the `queue`,
    // so we must call `queue.flush()` only after everything in the `publishQueue` has been flushed.
    FutureUnlessShutdown.outcomeF(publishQueue.flush().flatMap(_ => dbQueue.flush()))

  private[canton] class AcsCommitmentProcessorHealth(
      override val name: String,
      override protected val associatedOnShutdownRunner: OnShutdownRunner,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
    override def closingState: ComponentHealthState =
      ComponentHealthState.failed(s"Disconnected from synchronizer")
  }
}

object AcsCommitmentProcessor extends HasLoggerName {

  val healthName: String = "acs-commitment-processor"

  type ProcessorType =
    (
        CantonTimestamp,
        Traced[Seq[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]],
    ) => FutureUnlessShutdown[Unit]

  val emptyCommitment: AcsCommitment.CommitmentType = LtHash16().getByteString()

  def apply(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
      sequencerClient: SequencerClientSend,
      synchronizerCrypto: SyncCryptoClient[SyncCryptoApi],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      store: AcsCommitmentStore,
      pruningObserver: TraceContext => Unit,
      metrics: CommitmentMetrics,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      activeContractStore: ActiveContractStore,
      acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
      contractStore: ContractStore,
      enableAdditionalConsistencyChecks: Boolean,
      loggerFactory: NamedLoggerFactory,
      testingConfig: TestingConfigInternal,
      clock: Clock,
      exitOnFatalFailures: Boolean,
      maxCommitmentSendDelayMillis: Option[NonNegativeInt] = None,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[AcsCommitmentProcessor] = {

    implicit val loggingContext: NamedLoggingContext =
      NamedLoggingContext(loggerFactory, traceContext)

    def initCommitmentProcessor(store: AcsCommitmentStore)(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): FutureUnlessShutdown[(Option[CantonTimestampSecond], RunningCommitments)] = {

      val executed = for {
        lastComputed <- store.lastComputedAndSent
        _ = lastComputed.foreach { ts =>
          loggingContext.info(s"Last computed and sent timestamp: $ts")
        }
        runningCommitments <- initRunningCommitments(store)
        // we have no cached commitments for the first computation after recovery
        snapshot = runningCommitments.snapshot()
        _ = loggingContext.info(
          s"Initialized from stored snapshot at ${runningCommitments.watermark} (might be incomplete) with $snapshot"
        )
      } yield (lastComputed, runningCommitments)
      FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
        executed,
        "Failed to initialize the ACS commitment processor.",
        logPassiveInstanceAtInfo = true,
      )
    }
    // Ensure that the initialization runs first. We don't care about initialization having
    // completed by the time we return - only that it runs first.
    for {
      init <- initCommitmentProcessor(store)
      (endOfLastProcessedPeriod, runningCommitments) = init
      processor = new AcsCommitmentProcessor(
        synchronizerId,
        participantId,
        sequencerClient,
        synchronizerCrypto,
        sortedReconciliationIntervalsProvider,
        store,
        pruningObserver,
        metrics,
        protocolVersion,
        timeouts,
        futureSupervisor,
        activeContractStore,
        contractStore,
        acsCounterParticipantConfigStore,
        runningCommitments,
        endOfLastProcessedPeriod,
        enableAdditionalConsistencyChecks,
        loggerFactory,
        testingConfig,
        clock,
        exitOnFatalFailures,
        maxCommitmentSendDelayMillis,
      )
      // We trigger the processing of the buffered commitments, but we do not wait for it to complete here,
      // because, if processing buffered required topology updates that go through the same queue, we'd create a deadlock.
      // It should be that only in tests could we have the situation where we replay ACS changes while the ledger end
      // lags behind the replayed change timestamp. In normal processing, we publish ACS changes only after the ledger
      // end has moved, which should mean that all topology events for a given timestamp have been processed before
      // processing the ACS change for the same timestamp
      _ = processor.processBufferedAtInit(endOfLastProcessedPeriod)
      _ = loggingContext.info(
        s"Initialized the ACS commitment processor DB queue and started processing buffered commitments until $endOfLastProcessedPeriod"
      )
    } yield processor
  }

  /* Extracted as a pure function for testing */
  @VisibleForTesting
  private[pruning] def initRunningCommitments(
      store: AcsCommitmentStore
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[RunningCommitments] =
    store.runningCommitments.get()(TraceContext.empty).map { case (rt, snapshot) =>
      new RunningCommitments(
        rt,
        TrieMap(snapshot.toSeq.map { case (parties, h) =>
          parties -> LtHash16.tryCreate(h)
        }*),
      )
    }

  /** A snapshot of ACS commitments per set of stakeholders
    *
    * @param recordTime The timestamp and tie-breaker of the snapshot
    * @param active     Maps stakeholders to the commitment to their shared ACS, if the shared ACS is not empty
    * @param delta      A sub-map of active with those stakeholders whose commitments have changed since the last snapshot
    * @param deleted    Stakeholder sets whose ACS has gone to empty since the last snapshot (no longer active)
    */
  final case class CommitmentSnapshot(
      recordTime: RecordTime,
      active: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      delta: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deleted: Set[SortedSet[LfPartyId]],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[CommitmentSnapshot] = prettyOfClass(
      param("record time", _.recordTime),
      param("active", _.active),
      param("delta (parties)", _.delta.keySet),
      param("deleted", _.deleted),
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class RunningCommitments(
      initRt: RecordTime,
      commitments: TrieMap[SortedSet[LfPartyId], LtHash16],
  ) extends HasLoggerName {

    private val lock = new Object
    @volatile private var rt: RecordTime = initRt
    private val deltaB = Map.newBuilder[SortedSet[LfPartyId], LtHash16]

    /** The latest (immutable) snapshot. Taking the snapshot also garbage collects empty commitments.
      */
    def snapshot(): CommitmentSnapshot = {

      /* Delete all hashes that have gone empty since the last snapshot and return the corresponding stakeholder sets */
      def garbageCollect(
          candidates: Map[SortedSet[LfPartyId], LtHash16]
      ): Set[SortedSet[LfPartyId]] = {
        val deletedB = Set.newBuilder[SortedSet[LfPartyId]]
        candidates.foreach { case (stkhs, h) =>
          if (h.isEmpty) {
            deletedB += stkhs
            commitments -= stkhs
          }
        }
        deletedB.result()
      }

      blocking {
        lock.synchronized {
          val delta = deltaB.result()
          deltaB.clear()
          val deleted = garbageCollect(delta)
          val activeDelta = (delta -- deleted).fmap(_.getByteString())
          // Note that it's crucial to eagerly (via fmap, as opposed to, say mapValues) snapshot the LtHash16 values,
          // since they're mutable
          CommitmentSnapshot(
            rt,
            commitments.readOnlySnapshot().toMap.fmap(_.getByteString()),
            activeDelta,
            deleted,
          )
        }
      }
    }

    def update(rt: RecordTime, change: AcsChange)(implicit
        loggingContext: NamedLoggingContext
    ): Unit = {
      /*
      The concatenate function is guaranteed to be safe when contract IDs always have the same length.
      Otherwise, a longer contract ID without a reassignment counter might collide with a
      shorter contract ID with a reassignment counter.
      In the current implementation collisions cannot happen, because either all contracts in a commitment
      have a reassignment counter or none, depending on the protocol version.
       */
      def concatenate(
          contractId: LfContractId,
          reassignmentCounter: ReassignmentCounter,
      ): Array[Byte] =
        (
          contractId.encodeDeterministically
            concat ReassignmentCounter.encodeDeterministically(reassignmentCounter)
        ).toByteArray
      import com.digitalasset.canton.lfPartyOrdering
      blocking {
        lock.synchronized {
          this.rt = rt
          change.activations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
            val sortedStakeholders =
              SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
            val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
            h.add(concatenate(cid, stakeholdersAndReassignmentCounter.reassignmentCounter))
            loggingContext.debug(
              s"Adding to commitment activation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
            )
            deltaB += sortedStakeholders -> h
          }
          change.deactivations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
            val sortedStakeholders =
              SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
            val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
            h.remove(
              concatenate(
                cid,
                stakeholdersAndReassignmentCounter.reassignmentCounter,
              )
            )
            loggingContext.debug(
              s"Removing from commitment deactivation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
            )
            deltaB += sortedStakeholders -> h
          }
        }
      }
    }

    def watermark: RecordTime = rt
  }

  /** Caches the commitments per participant and the commitments per stakeholder group in a period, in order to optimize
    * the computation of commitments for the subsequent period.
    * It optimizes the computation of a counter-participant commitments when at most half of the stakeholder commitments
    * shared with that participant change in the next period.
    *
    * The class is thread-safe w.r.t. calling [[setCachedCommitments]] and [[computeCmtFromCached]]. However,
    * for correct commitment computation, the caller needs to call [[setCachedCommitments]] before
    * [[computeCmtFromCached]], because [[computeCmtFromCached]] uses the state set by [[setCachedCommitments]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class CachedCommitments(
      private var prevParticipantCmts: Map[ParticipantId, AcsCommitment.CommitmentType] =
        Map.empty[ParticipantId, AcsCommitment.CommitmentType],
      private var prevStkhdCmts: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType] = Map
        .empty[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      private var prevParticipantToStkhd: Map[ParticipantId, Set[SortedSet[LfPartyId]]] =
        Map.empty[ParticipantId, Set[SortedSet[LfPartyId]]],
  ) {
    private val lock = new Object

    def setCachedCommitments(
        cmts: Map[ParticipantId, AcsCommitment.CommitmentType],
        stkhdCmts: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
        participantToStkhd: Map[ParticipantId, Set[SortedSet[LfPartyId]]],
    ): Unit =
      blocking {
        lock.synchronized {
          // cache participant commitments
          prevParticipantCmts = cmts
          // cache stakeholder group commitments
          prevStkhdCmts = stkhdCmts
          prevParticipantToStkhd = participantToStkhd
        }
      }

    def computeCmtFromCached(
        participant: ParticipantId,
        newStkhdCmts: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
    ): Option[AcsCommitment.CommitmentType] =
      blocking {
        lock.synchronized {
          // a commitment is cached when we have the participant commitment, and
          // all commitments for all its stakeholder groups are cached, and exist
          // in the new stakeholder commitments (a delete exists as an empty commitment)
          val commitmentIsCached =
            prevParticipantCmts.contains(participant) &&
              prevParticipantToStkhd
                .get(participant)
                .exists(set =>
                  set.forall(stkhds =>
                    prevStkhdCmts.contains(stkhds) && newStkhdCmts.contains(stkhds)
                  )
                )
          if (commitmentIsCached) {
            // remove from old commitment all stakeholder commitments that have changed
            val changedKeys = newStkhdCmts.filter { case (stkhd, newCmt) =>
              prevStkhdCmts
                .get(stkhd)
                .fold(false)(_ != newCmt && prevParticipantToStkhd(participant).contains(stkhd))
            }
            if (changedKeys.sizeIs > prevParticipantToStkhd(participant).size / 2) None
            else {
              val c = LtHash16.tryCreate(prevParticipantCmts(participant))
              changedKeys.foreach { case (stkhd, cmt) =>
                c.remove(LtHash16.tryCreate(prevStkhdCmts(stkhd)).get())
                // if the stakeholder group is still active, add its commitment
                if (cmt != emptyCommitment) c.add(cmt.toByteArray)
              }
              // add new stakeholder group commitments for groups that were not active before
              newStkhdCmts.foreach { case (stkhds, cmt) =>
                if (!prevParticipantToStkhd(participant).contains(stkhds) && cmt != emptyCommitment)
                  c.add(cmt.toByteArray)
              }
              Some(c.getByteString())
            }
          } else None
        }
      }

    def clear(): Unit = setCachedCommitments(
      Map.empty[ParticipantId, AcsCommitment.CommitmentType],
      Map.empty[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      Map.empty[ParticipantId, Set[SortedSet[LfPartyId]]],
    )
  }

  /** Compute the ACS commitments at the given timestamp.
    *
    * Extracted as a pure function to be able to test.
    */
  @VisibleForTesting
  private[pruning] def commitments(
      participantId: ParticipantId,
      runningCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      synchronizerCrypto: SyncCryptoClient[SyncCryptoApi],
      timestamp: CantonTimestampSecond,
      pruningMetrics: Option[CommitmentMetrics],
      parallelism: PositiveNumeric[Int],
      cachedCommitments: CachedCommitments,
      // compute commitments just for includeCounterParticipantIds, if non-empty, otherwise for all counter-participants
      filterInParticipantIds: Seq[ParticipantId] = Seq.empty,
      // exclude from computing commitments excludeCounterParticipantIds, if non-empty, otherwise do not exclude anyone
      filterOutParticipantIds: Seq[ParticipantId] = Seq.empty,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[ParticipantId, AcsCommitment.CommitmentType]] = {
    val commitmentTimer = pruningMetrics.map(_.compute.startAsync())

    for {
      byParticipant <- stakeholderCommitmentsPerParticipant(
        participantId,
        runningCommitments,
        synchronizerCrypto,
        timestamp,
        parallelism,
      )
    } yield {
      // compute commitments just for counterParticipantId, if defined, otherwise for all counter-participants
      val includeCPs =
        if (filterInParticipantIds.isEmpty) byParticipant.keys
        else filterInParticipantIds
      val finalCPs = includeCPs.toSet.diff(filterOutParticipantIds.toSet)
      val res = computeCommitmentsPerParticipant(
        byParticipant.filter { case (pid, _) =>
          finalCPs.contains(pid)
        },
        cachedCommitments,
      )
      // update cached commitments
      cachedCommitments.setCachedCommitments(
        res,
        runningCommitments,
        byParticipant.fmap(m => m.map { case (stkhd, _cmt) => stkhd }.toSet),
      )
      commitmentTimer.foreach(_.stop())
      res
    }
  }

  @VisibleForTesting
  private[pruning] def stakeholderCommitmentsPerParticipant(
      participantId: ParticipantId,
      runningCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      synchronizerCrypto: SyncCryptoClient[SyncCryptoApi],
      timestamp: CantonTimestampSecond,
      parallelism: PositiveNumeric[Int],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    Map[ParticipantId, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType]]
  ] =
    for {
      ipsSnapshot <- synchronizerCrypto.awaitIpsSnapshot("acs-stakeholder-commitments")(
        timestamp.forgetRefinement
      )
      // Important: use the keys of the timestamp
      isActiveParticipant <-
        ipsSnapshot.isParticipantActive(participantId)

      byParticipant <-
        if (isActiveParticipant) {
          val allParties = runningCommitments.keySet.flatten
          ipsSnapshot
            .activeParticipantsOfParties(allParties.toSeq)
            .flatMap { participantsOf =>
              FutureUnlessShutdown.outcomeF(
                IterableUtil
                  .mapReducePar[(SortedSet[LfPartyId], AcsCommitment.CommitmentType), Map[
                    ParticipantId,
                    Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
                  ]](parallelism, runningCommitments.toSeq) { case (parties, commitment) =>
                    val participants = parties.flatMap(participantsOf.getOrElse(_, Set.empty))
                    // Check that we're hosting at least one stakeholder; it can happen that the stakeholder used to be
                    // hosted on this participant, but is now disabled
                    val pSet =
                      if (participants.contains(participantId)) participants - participantId
                      else Set.empty
                    val commitmentS =
                      if (participants.contains(participantId)) Map(parties -> commitment)
                      // Signal with an empty commitment that our participant does no longer host any
                      // party in the stakeholder group
                      else Map(parties -> emptyCommitment)
                    pSet.map(_ -> commitmentS).toMap
                  }(MapsUtil.mergeWith(_, _)(_ ++ _))
                  .map(
                    _.getOrElse(
                      Map
                        .empty[ParticipantId, Map[SortedSet[
                          LfPartyId
                        ], AcsCommitment.CommitmentType]]
                    )
                  )
              )
            }
        } else
          FutureUnlessShutdown.pure(
            Map.empty[ParticipantId, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType]]
          )
    } yield {
      byParticipant
    }

  @VisibleForTesting
  private[pruning] def computeCommitmentsPerParticipant(
      cmts: Map[ParticipantId, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType]],
      cachedCommitments: CachedCommitments,
  ): Map[ParticipantId, AcsCommitment.CommitmentType] =
    cmts.map { case (p, hashes) =>
      (
        p,
        cachedCommitments
          .computeCmtFromCached(p, hashes)
          .getOrElse(
            commitmentsFromStkhdCmts(
              hashes.map { case (_stakeholders, cmt) => cmt }.filter(_ != emptyCommitment).toSeq
            )
          ),
      )
    }

  @VisibleForTesting
  private[pruning] def commitmentsFromStkhdCmts(
      commitments: Seq[AcsCommitment.CommitmentType]
  ): AcsCommitment.CommitmentType = {
    val sumHash = LtHash16()
    commitments.foreach(h => sumHash.add(h.toByteArray))
    sumHash.getByteString()
  }

  /*
    Describe how ACS commitments are taken into account for the safeToPrune computation:
   */
  sealed trait CommitmentsPruningBound extends Product with Serializable

  object CommitmentsPruningBound {
    // Not before any outstanding commitment
    final case class Outstanding(
        noOutstandingCommitmentsF: CantonTimestamp => FutureUnlessShutdown[Option[CantonTimestamp]]
    ) extends CommitmentsPruningBound

    // Not before any computed and sent commitment
    final case class LastComputedAndSent(
        lastComputedAndSentF: FutureUnlessShutdown[Option[CantonTimestamp]]
    ) extends CommitmentsPruningBound
  }

  private def checkRunningCommitmentsAgainstACS(
      runningCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      activeContractStore: ActiveContractStore,
      contractStore: ContractStore,
      enableAdditionalConsistencyChecks: Boolean,
      toInclusive: CantonTimestamp, // end of interval used to snapshot the ACS
  )(implicit
      ec: ExecutionContext,
      namedLoggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Unit] = {

    def withMetadataSeq(cids: Seq[LfContractId]): FutureUnlessShutdown[Seq[SerializableContract]] =
      contractStore
        .lookupManyExistingUncached(cids)(namedLoggingContext.traceContext)
        .valueOr { missingContractId =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Contract $missingContractId is in the active contract store but not in the contract store"
            )
          )
        }

    def lookupChangeMetadata(
        activations: Map[LfContractId, ReassignmentCounter]
    ): FutureUnlessShutdown[AcsChange] =
      for {
        // TODO(i9270) extract magic numbers
        storedActivatedContracts <- MonadUtil.batchedSequentialTraverse(
          parallelism = PositiveInt.tryCreate(20),
          chunkSize = PositiveInt.tryCreate(500),
        )(activations.keySet.toSeq)(withMetadataSeq)
      } yield {
        AcsChange(
          activations = storedActivatedContracts
            .map(c =>
              c.contractId ->
                ContractStakeholdersAndReassignmentCounter(
                  c.metadata.stakeholders,
                  activations(c.contractId),
                )
            )
            .toMap,
          deactivations = Map.empty,
        )
      }

    if (enableAdditionalConsistencyChecks) {
      for {
        activeContracts <- activeContractStore.snapshot(toInclusive)(
          namedLoggingContext.traceContext
        )
        activations = activeContracts.map { case (cid, (_toc, reassignmentCounter)) =>
          (
            cid,
            reassignmentCounter,
          )
        }
        change <- lookupChangeMetadata(activations)
      } yield {
        val emptyRunningCommitments =
          new RunningCommitments(RecordTime.MinValue, TrieMap.empty[SortedSet[LfPartyId], LtHash16])
        val toc = new RecordTime(toInclusive, 0)
        emptyRunningCommitments.update(toc, change)
        val acsCommitments = emptyRunningCommitments.snapshot().active
        if (acsCommitments != runningCommitments) {
          Errors.InternalError
            .InconsistentRunningCommitmentAndACS(toc, acsCommitments, runningCommitments)
            .discard
        }
      }
    } else FutureUnlessShutdown.unit
  }

  object Errors extends AcsCommitmentErrorGroup {
    @Explanation(
      """This error indicates that there was an internal error within the ACS commitment processing."""
    )
    @Resolution("Inspect error message for details.")
    object InternalError
        extends ErrorCode(
          id = "ACS_COMMITMENT_INTERNAL_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {

      override protected def exposedViaApi: Boolean = false

      final case class MultipleCommitmentsInBatch(
          synchronizerId: SynchronizerId,
          timestamp: CantonTimestamp,
          num: Int,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Received multiple batched ACS commitments over synchronizer"
          )

      @Explanation(
        """This error indicates that the running commitments at the participant at the tick time do not match
          the state found in the active contract store at the same tick time.
          This error indicates a bug in computing the commitments."""
      )
      @Resolution("Contact customer support.")
      final case class InconsistentRunningCommitmentAndACS(
          toc: RecordTime,
          acsCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
          runningCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      )(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(
            cause = "Detected an inconsistency between the running commitment and the ACS"
          )
    }

    object MismatchError extends ErrorGroup {
      @Explanation("""This error indicates that a remote participant has sent a commitment over
            |an ACS for a period, while this participant does not think that there is a shared contract state.
            |This error occurs if a remote participant has manually changed contracts using repair,
            |or due to byzantine behavior, or due to malfunction of the system. The consequence is that
            |the ledger is forked, and some commands that should pass will not.""")
      @Resolution(
        """Please contact the other participant in order to check the cause of the mismatch. Either repair
            |the store of this participant or of the counterparty."""
      )
      object NoSharedContracts extends AlarmErrorCode(id = "ACS_MISMATCH_NO_SHARED_CONTRACTS") {
        final case class Mismatch(synchronizerId: SynchronizerId, remote: AcsCommitment)
            extends Alarm(
              cause = "Received a commitment where we have no shared contract with the sender"
            )
      }

      @Explanation("""This error indicates that a remote participant has sent a commitment over
            |an ACS for a period which does not match the local commitment.
            |This error occurs if a remote participant has manually changed contracts using repair,
            |or due to byzantine behavior, or due to malfunction of the system. The consequence is that the ledger is forked,
            |and some commands that should pass will not.""")
      @Resolution(
        """Please contact the other participant in order to check the cause of the mismatch. Either repair
            |the store of this participant or of the counterparty."""
      )
      object CommitmentsMismatch extends AlarmErrorCode(id = "ACS_COMMITMENT_MISMATCH") {
        final case class Mismatch(
            synchronizerId: SynchronizerId,
            remote: AcsCommitment,
            local: Seq[(CommitmentPeriod, AcsCommitment.CommitmentType)],
        ) extends Alarm(cause = "The local commitment does not match the remote commitment")
      }

      @Explanation("The participant has detected that another node is behaving maliciously.")
      @Resolution("Contact support.")
      object AcsCommitmentAlarm extends AlarmErrorCode(id = "ACS_COMMITMENT_ALARM") {
        final case class Warn(override val cause: String) extends Alarm(cause)
      }
    }

    trait AcsCommitmentDegradation extends CantonError
    object DegradationError extends ErrorGroup {

      @Explanation(
        "The participant is configured to engage catchup mode, however configuration is invalid to have any effect"
      )
      @Resolution("Please update catchup mode to have a catchUpIntervalSkip higher than 1")
      object AcsCommitmentDegradationWithIneffectiveConfig
          extends ErrorCode(
            id = "ACS_COMMITMENT_DEGRADATION_WITH_INEFFECTIVE_CONFIG",
            ErrorCategory.BackgroundProcessDegradationWarning,
          ) {
        final case class Report()(implicit
            val loggingContext: ErrorLoggingContext
        ) extends CantonError.Impl(
              cause =
                "The participant has activated catchup mode, however catchUpIntervalSkip is set to 1, so it will have no improvement."
            )
            with AcsCommitmentDegradation
      }

      @Explanation(
        "The participant has detected that ACS computation is taking to long and trying to catch up."
      )
      @Resolution("Catch up mode is enabled and the participant should recover on its own.")
      object AcsCommitmentDegradation
          extends ErrorCode(
            id = "ACS_COMMITMENT_DEGRADATION",
            ErrorCategory.BackgroundProcessDegradationWarning,
          ) {
        final case class Report()(implicit
            val loggingContext: ErrorLoggingContext
        ) extends CantonError.Impl(
              cause =
                "The participant has activated ACS catchup mode to combat computation problem."
            )
            with AcsCommitmentDegradation
      }
    }
  }

  sealed trait ReceivedCmtState {
    def toProtoV30: ReceivedCommitmentState
  }

  object ReceivedCmtState {
    object Match extends ReceivedCmtState {
      override val toProtoV30: ReceivedCommitmentState =
        ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MATCH
    }

    object Mismatch extends ReceivedCmtState {
      override val toProtoV30: ReceivedCommitmentState =
        ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MISMATCH
    }

    object Buffered extends ReceivedCmtState {
      override val toProtoV30: ReceivedCommitmentState =
        ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_BUFFERED
    }

    object Outstanding extends ReceivedCmtState {
      override val toProtoV30: ReceivedCommitmentState =
        ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_OUTSTANDING
    }

    def fromProtoV30(
        proto: ReceivedCommitmentState
    ): ParsingResult[ReceivedCmtState] =
      proto match {
        case ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MATCH => Right(Match)
        case ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MISMATCH => Right(Mismatch)
        case ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_BUFFERED => Right(Buffered)
        case ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_OUTSTANDING => Right(Outstanding)
        case _ =>
          Left(
            ProtoDeserializationError.ValueConversionError(
              "received commitment state",
              s"Unknown value: $proto",
            )
          )
      }
  }

  sealed trait SentCmtState {
    def toProtoV30: SentCommitmentState
  }

  object SentCmtState {
    object Match extends SentCmtState {
      override val toProtoV30: SentCommitmentState =
        SentCommitmentState.SENT_COMMITMENT_STATE_MATCH
    }

    object Mismatch extends SentCmtState {
      override val toProtoV30: SentCommitmentState =
        SentCommitmentState.SENT_COMMITMENT_STATE_MISMATCH
    }

    object NotCompared extends SentCmtState {
      override val toProtoV30: SentCommitmentState =
        SentCommitmentState.SENT_COMMITMENT_STATE_NOT_COMPARED
    }

    def fromProtoV30(
        proto: SentCommitmentState
    ): ParsingResult[SentCmtState] =
      proto match {
        case SentCommitmentState.SENT_COMMITMENT_STATE_MATCH => Right(Match)
        case SentCommitmentState.SENT_COMMITMENT_STATE_MISMATCH => Right(Mismatch)
        case SentCommitmentState.SENT_COMMITMENT_STATE_NOT_COMPARED => Right(NotCompared)
        case _ =>
          Left(
            ProtoDeserializationError.ValueConversionError(
              "sent commitment state",
              s"Unknown value: $proto",
            )
          )
      }
  }

  @VisibleForTesting
  def reassignmentCountersForArchivedTransient(
      commitSet: CommitSet
  ): Map[LfContractId, ReassignmentCounter] = {

    // We first search in assignments, because they would have the most recent reassignment counter.
    val transientCidsAssigned = commitSet.assignments.collect {
      case (contractId, tcAndContractHash) if commitSet.archivals.keySet.contains(contractId) =>
        (contractId, tcAndContractHash.reassignmentCounter)
    }

    // Then we search in creations
    val transientCidsCreated = commitSet.creations.collect {
      case (contractId, tcAndContractHash)
          if commitSet.archivals.keySet.contains(contractId) && !transientCidsAssigned.keySet
            .contains(contractId) =>
        (contractId, tcAndContractHash.reassignmentCounter)
    }

    transientCidsAssigned ++ transientCidsCreated
  }
}
