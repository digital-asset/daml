// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.{EitherT, Nested}
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.{Eval, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.ledger.participant.state.SyncService.SubmissionCostEstimation
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  SubmitterInfo,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmissionResult,
}
import com.digitalasset.canton.participant.protocol.reassignment.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  SynchronizerNotReady,
}
import com.digitalasset.canton.participant.protocol.submission.{
  SeedGenerator,
  TransactionConfirmationRequestFactory,
}
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  JournalGarbageCollector,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerHandle,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyDispatcher,
  SequencerConnectionSuccessorListener,
}
import com.digitalasset.canton.participant.traffic.{
  ParticipantTrafficControlSubscriber,
  TrafficCostEstimator,
}
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.command.interactive.CostEstimationHints
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, Envelope, TrafficState}
import com.digitalasset.canton.sequencing.traffic.{TrafficControlProcessor, TrafficStateController}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag
import com.digitalasset.canton.topology.transaction.{
  SynchronizerTrustCertificate,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{
  ContractHasher,
  ContractValidator,
  EitherTUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
}
import com.digitalasset.canton.version.ParticipantProtocolFeatureFlags
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/** A connected synchronizer from the synchronization service.
  *
  * @param synchronizerHandle
  *   A synchronizer handle providing sequencer clients.
  * @param participantId
  *   The participant node id hosting this sync service.
  * @param persistent
  *   The persistent state of the connected synchronizer.
  * @param ephemeral
  *   The ephemeral state of the connected synchronizer.
  * @param packageService
  *   Underlying package management service.
  * @param synchronizerCrypto
  *   Synchronisation crypto utility combining IPS and Crypto operations for a single synchronizer.
  */
class ConnectedSynchronizer(
    val synchronizerHandle: SynchronizerHandle,
    participantId: ParticipantId,
    engine: Engine,
    parameters: ParticipantNodeParameters,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    private[sync] val persistent: SyncPersistentState,
    val ephemeral: SyncEphemeralState,
    val packageService: PackageService,
    val synchronizerCrypto: SynchronizerCryptoClient,
    contractValidator: ContractValidator,
    identityPusher: ParticipantTopologyDispatcher,
    topologyProcessor: TopologyTransactionProcessor,
    missingKeysAlerter: MissingKeysAlerter,
    val sequencerConnectionListener: SequencerConnectionSuccessorListener,
    reassignmentCoordination: ReassignmentCoordination,
    commandProgressTracker: CommandProgressTracker,
    messageDispatcherFactory: MessageDispatcher.Factory[MessageDispatcher],
    journalGarbageCollector: JournalGarbageCollector,
    val acsCommitmentProcessor: AcsCommitmentProcessor,
    clock: Clock,
    promiseUSFactory: DefaultPromiseUnlessShutdownFactory,
    metrics: ConnectedSynchronizerMetrics,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
    testingConfig: TestingConfigInternal,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with FlagCloseableAsync
    with ReassignmentSubmissionHandle
    with CloseableHealthComponent
    with AtomicHealthComponent {

  val psid: PhysicalSynchronizerId = synchronizerHandle.psid

  val topologyClient: SynchronizerTopologyClientWithInit = synchronizerHandle.topologyClient

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  override val name: String = ConnectedSynchronizer.healthName
  override def initialHealthState: ComponentHealthState = ComponentHealthState.NotInitializedState
  override def closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from synchronizer")

  private[canton] val sequencerClient: RichSequencerClient = synchronizerHandle.sequencerClient
  private[canton] val sequencerChannelClientO: Option[SequencerChannelClient] =
    synchronizerHandle.sequencerChannelClientO
  val timeTracker: SynchronizerTimeTracker = ephemeral.timeTracker
  val staticSynchronizerParameters: StaticSynchronizerParameters =
    synchronizerHandle.staticParameters

  private val seedGenerator =
    new SeedGenerator(synchronizerCrypto.crypto.pureCrypto)

  private val packageResolver: PackageResolver = pkgId =>
    traceContext => packageService.getPackage(pkgId)(traceContext)

  private val contractHasher = ContractHasher(engine, packageResolver)

  private[canton] val requestGenerator =
    TransactionConfirmationRequestFactory(
      participantId,
      psid,
    )(
      synchronizerCrypto.crypto.pureCrypto,
      contractHasher,
      seedGenerator,
      parameters.loggingConfig,
      loggerFactory,
    )

  private val trafficCostEstimation = {
    val trafficStateController: TrafficStateController =
      sequencerClient.trafficStateController.getOrElse {
        ErrorUtil.invalidState(
          s"Sequencer client of the participant node $participantId does not have a traffic state controller"
        )(ErrorLoggingContext.fromTracedLogger(logger)(TraceContext.empty))
      }

    new TrafficCostEstimator(
      requestGenerator,
      topologyClient,
      synchronizerCrypto,
      ephemeral.contractStore,
      ephemeral.sessionKeyStore,
      psid,
      participantId,
      trafficStateController,
      loggerFactory,
    )
  }

  def estimateTrafficCost(
      transaction: LfVersionedTransaction,
      transactionMeta: TransactionMeta,
      submitterInfo: SubmitterInfo,
      keyResolver: LfKeyResolver,
      disclosedContracts: Map[LfContractId, LfFatContractInst],
      costHints: CostEstimationHints,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SubmissionCostEstimation] =
    trafficCostEstimation.estimateTrafficCost(
      transaction,
      transactionMeta,
      submitterInfo,
      keyResolver,
      disclosedContracts,
      costHints,
    )

  private val damle =
    new DAMLe(
      packageResolver,
      engine,
      parameters.engine.validationPhaseLogging,
      loggerFactory,
    )

  private val transactionProcessor: TransactionProcessor = new TransactionProcessor(
    participantId,
    requestGenerator,
    psid,
    damle,
    staticSynchronizerParameters,
    synchronizerCrypto,
    contractValidator,
    sequencerClient,
    ephemeral.inFlightSubmissionSynchronizerTracker,
    ephemeral,
    commandProgressTracker,
    metrics.transactionProcessing,
    timeouts,
    loggerFactory,
    futureSupervisor,
    packageResolver = packageResolver,
    testingConfig = testingConfig,
    promiseUSFactory,
    parameters.loggingConfig.api.messagePayloads,
  )

  private val unassignmentProcessor: UnassignmentProcessor = new UnassignmentProcessor(
    Source(psid),
    participantId,
    Source(staticSynchronizerParameters),
    reassignmentCoordination,
    ephemeral.inFlightSubmissionSynchronizerTracker,
    ephemeral,
    synchronizerCrypto,
    contractValidator,
    seedGenerator,
    sequencerClient,
    timeouts,
    Source(staticSynchronizerParameters.protocolVersion),
    loggerFactory,
    futureSupervisor,
    testingConfig = testingConfig,
    promiseUSFactory,
  )

  private val assignmentProcessor: AssignmentProcessor = new AssignmentProcessor(
    Target(psid),
    participantId,
    Target(staticSynchronizerParameters),
    reassignmentCoordination,
    ephemeral.inFlightSubmissionSynchronizerTracker,
    ephemeral,
    synchronizerCrypto,
    contractValidator,
    seedGenerator,
    sequencerClient,
    timeouts,
    Target(staticSynchronizerParameters.protocolVersion),
    loggerFactory,
    futureSupervisor,
    testingConfig = testingConfig,
    promiseUSFactory,
  )

  private val trafficProcessor =
    new TrafficControlProcessor(
      synchronizerCrypto,
      psid,
      Option.empty[CantonTimestamp],
      loggerFactory,
    )

  sequencerClient.trafficStateController.foreach { tsc =>
    trafficProcessor.subscribe(
      new ParticipantTrafficControlSubscriber(
        tsc,
        participantId,
        loggerFactory,
      )
    )
  }

  private val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor =
    new BadRootHashMessagesRequestProcessor(
      ephemeral,
      synchronizerCrypto,
      sequencerClient,
      participantId,
      timeouts,
      loggerFactory,
    )

  private val registerIdentityTransactionHandle = identityPusher.createHandler(
    synchronizerHandle.synchronizerAlias,
    synchronizerHandle.topologyClient,
    sequencerClient,
    ephemeral.timeTracker,
  )

  private val messageDispatcher: MessageDispatcher =
    messageDispatcherFactory.create(
      psid,
      participantId,
      ephemeral.requestTracker,
      transactionProcessor,
      unassignmentProcessor,
      assignmentProcessor,
      topologyProcessor,
      trafficProcessor,
      acsCommitmentProcessor.processBatch,
      ephemeral.requestCounterAllocator,
      ephemeral.recordOrderPublisher,
      badRootHashMessagesRequestProcessor,
      ephemeral.inFlightSubmissionSynchronizerTracker,
      loggerFactory,
      metrics,
    )

  def addJournalGarageCollectionLock()(implicit
      traceContext: TraceContext
  ): Future[Unit] = journalGarbageCollector.addOneLock()

  def removeJournalGarageCollectionLock()(implicit
      traceContext: TraceContext
  ): Unit = journalGarbageCollector.removeOneLock()

  def getTrafficControlState(implicit traceContext: TraceContext): Future[TrafficState] =
    sequencerClient.trafficStateController
      .map { tsc =>
        Future.successful(tsc.getState)
      }
      .getOrElse {
        ErrorUtil.invalidState(
          "Participant's sequencer client should have a traffic state controller"
        )
      }

  private def initialize(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConnectedSynchronizerInitializationError, Unit] = {
    def liftF[A](
        f: FutureUnlessShutdown[A]
    ): EitherT[FutureUnlessShutdown, ConnectedSynchronizerInitializationError, A] = EitherT.right(f)

    def withMetadataSeq(cids: Seq[LfContractId]): FutureUnlessShutdown[Seq[ContractInstance]] =
      participantNodePersistentState.value.contractStore
        .lookupManyExistingUncached(cids)
        .valueOr { missingContractId =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Contract $missingContractId is in the active contract store but not in the contract store"
            )
          )
        }

    def lookupChangeMetadata(change: ActiveContractIdsChange): FutureUnlessShutdown[AcsChange] =
      for {
        storedActivatedContracts <- MonadUtil.batchedSequentialTraverse(
          parallelism = parameters.batchingConfig.parallelism,
          chunkSize = parameters.batchingConfig.maxItemsInBatch,
        )(change.activations.keySet.toSeq)(withMetadataSeq)
        storedDeactivatedContracts <- MonadUtil
          .batchedSequentialTraverse(
            parallelism = PositiveInt.tryCreate(20),
            chunkSize = PositiveInt.tryCreate(500),
          )(
            change.deactivations.keySet.toSeq
          )(
            withMetadataSeq
          )
      } yield {
        AcsChange(
          activations = storedActivatedContracts
            .map(c =>
              c.contractId -> ContractStakeholdersAndReassignmentCounter(
                c.stakeholders,
                change.activations(c.contractId).reassignmentCounter,
              )
            )
            .toMap,
          deactivations = storedDeactivatedContracts
            .map(c =>
              c.contractId ->
                ContractStakeholdersAndReassignmentCounter(
                  c.stakeholders,
                  change.deactivations(c.contractId).reassignmentCounter,
                )
            )
            .toMap,
        )
      }

    // pre-inform the ACS commitment processor about upcoming topology changes.
    // as we future date the topology changes, they happen at an effective time
    // and not necessarily at a timestamp triggered by a sequencer message
    // therefore, we need to pre-register them with the acs commitment processor
    // who will then consume them once a tick with a higher timestamp is observed.
    // on a restart, we can just load the relevant effective times from the database
    def loadPendingEffectiveTimesFromTopologyStore(
        timestamp: CantonTimestamp
    ): EitherT[FutureUnlessShutdown, ConnectedSynchronizerInitializationError, Unit] = {
      val store = synchronizerHandle.syncPersistentState.topologyStore
      for {
        _ <- EitherT
          .right(store.findUpcomingEffectiveChanges(timestamp).map { changes =>
            changes.headOption.foreach { head =>
              logger.debug(
                s"Initializing the acs commitment processor with ${changes.length} effective times starting from: ${head.validFrom}"
              )
              acsCommitmentProcessor.initializeTicksOnStartup(changes.map(_.validFrom).toList)
            }
          })
      } yield ()
    }

    def replayAcsChanges(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ConnectedSynchronizerInitializationError, LazyList[
      (RecordTime, AcsChange)
    ]] =
      liftF(for {
        contractIdChanges <- persistent.activeContractStore
          .changesBetween(fromExclusive, toInclusive)
        changes <- contractIdChanges.parTraverse { case (toc, change) =>
          val changeWithAdjustedReassignmentCountersForUnassignments = ActiveContractIdsChange(
            change.activations,
            change.deactivations.fmap {
              case StateChangeType(ContractChange.Unassigned, reassignmentCounter) =>
                StateChangeType(ContractChange.Unassigned, reassignmentCounter - 1)
              case change => change
            },
          )
          lookupChangeMetadata(changeWithAdjustedReassignmentCountersForUnassignments).map(ch =>
            (RecordTime.fromTimeOfChange(toc), ch)
          )
        }
      } yield {
        logger.info(
          s"Replaying ${changes.size} ACS changes between $fromExclusive (exclusive) and $toInclusive to the commitment processor"
        )
        logger.trace(
          s"Retrieved contract ID changes from changesBetween " +
            s"${contractIdChanges
                .map { case (toc, activeContractsChange) =>
                  s"at time $toc activations ${activeContractsChange.activations} deactivations ${activeContractsChange.deactivations} "
                }
                .force
                .mkString(", ")}"
        )
        changes
      })

    def initializeClientAtCleanHead(): Unit = {
      // generally, the topology client will be initialised by the topology processor. however,
      // if there is nothing to be replayed, then the topology processor will only be initialised
      // once the first event is dispatched.
      // however, this is bad for reassignment processing as we need to be able to access the topology state
      // across synchronizers and this requires that the clients are separately initialised on the participants
      val resubscriptionTs = ephemeral.startingPoints.processing.lastSequencerTimestamp
      logger.debug(s"Initializing topology client at clean head=$resubscriptionTs")
      // startup with the resubscription-ts
      topologyClient.updateHead(
        SequencedTime(resubscriptionTs),
        EffectiveTime(resubscriptionTs),
        ApproximateTime(resubscriptionTs),
      )
      // now, compute epsilon at resubscriptionTs and update client
      topologyClient.updateHead(
        SequencedTime(resubscriptionTs),
        EffectiveTime(
          resubscriptionTs.plus(staticSynchronizerParameters.topologyChangeDelay.duration)
        ),
        ApproximateTime(resubscriptionTs),
      )
    }

    val startingPoints = ephemeral.startingPoints
    val nextRepairCounter = startingPoints.processing.nextRepairCounter
    val lastSequencerTimestamp = startingPoints.processing.lastSequencerTimestamp

    for {
      // Prepare missing key alerter
      _ <- EitherT.right(missingKeysAlerter.init())
      _ <- EitherT.right(sequencerConnectionListener.init())

      // Phase 0: Initialise topology client at current clean head
      _ = initializeClientAtCleanHead()

      // Phase 2: Log so we know if any repairs have been applied.
      _ = logger.info(s"The next repair counter would be $nextRepairCounter")

      // Phase 3: publish ACS changes from some suitable point up to clean head timestamp to the commitment processor.
      // The "suitable point" must ensure that the [[com.digitalasset.canton.participant.store.AcsSnapshotStore]]
      // receives any partially-applied changes; choosing the timestamp returned by the store is sufficient and optimal
      // in terms of performance, but any earlier timestamp is also correct
      acsChangesReplayStartRt <- EitherT.right(
        persistent.acsCommitmentStore.runningCommitments.watermark
      )

      _ <- loadPendingEffectiveTimesFromTopologyStore(acsChangesReplayStartRt.timestamp)
      acsChangesToReplay <-
        if (lastSequencerTimestamp >= acsChangesReplayStartRt.timestamp) {
          logger.info(
            s"Looking for ACS changes to replay between ${acsChangesReplayStartRt.timestamp} and $lastSequencerTimestamp"
          )
          replayAcsChanges(
            acsChangesReplayStartRt.toTimeOfChange,
            TimeOfChange(lastSequencerTimestamp, Some(nextRepairCounter)),
          )
        } else
          EitherT.pure[FutureUnlessShutdown, ConnectedSynchronizerInitializationError](Seq.empty)
      _ = acsChangesToReplay.foreach { case (toc, change) =>
        acsCommitmentProcessor.publish(toc, change)
      }
    } yield ()
  }

  /** Starts the connected synchronizer. NOTE: Must only be called at most once on a synchronizer
    * instance.
    */
  private[sync] def start()(implicit
      initializationTraceContext: TraceContext
  ): FutureUnlessShutdown[Either[ConnectedSynchronizerInitializationError, Unit]] =
    synchronizeWithClosing("start") {

      val delayLogger = new DelayLogger(
        clock,
        logger,
        parameters.delayLoggingThreshold,
        metrics.sequencerClient.handler.sequencingTimeMetrics,
      )

      def firstUnpersistedEventScF: FutureUnlessShutdown[SequencerCounter] =
        persistent.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))(
            initializationTraceContext
          )
          .fold(_ => SequencerCounter.Genesis, _.counter + 1)

      val cleanProcessingTs =
        ephemeral.startingPoints.processing.lastSequencerTimestamp
      // note: we need the optional here, since it changes the behavior inside (subscribing from Some(CantonTimestamp.MinValue) would result in timeouts at requesting topology snapshot ... has not completed after...)
      val cleanProcessingTsO = Some(cleanProcessingTs)
        .filterNot(_ == CantonTimestamp.MinValue)
      val subscriptionPriorTs = {
        val cleanReplayTs = ephemeral.startingPoints.cleanReplay.prenextTimestamp
        Ordering[CantonTimestamp].min(cleanReplayTs, cleanProcessingTs)
      }

      def waitForParticipantToBeInTopology(implicit
          initializationTraceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ConnectedSynchronizerInitializationError, Unit] =
        EitherT(
          synchronizerHandle.topologyClient
            .awaitUS(_.isParticipantActive(participantId), timeouts.verifyActive.duration)
            .map(isActive =>
              Either.cond(
                isActive,
                (),
                ParticipantDidNotBecomeActive(
                  s"Participant did not become active after ${timeouts.verifyActive.duration}"
                ),
              )
            )
        )

      // Update the synchronizer trust certificate if the feature flags are out of sync
      def synchronizeFeatureFlagsAsync(implicit initializationTraceContext: TraceContext): Unit = {
        val synchronizerId = synchronizerHandle.psid.logical
        val protocolVersion =
          synchronizerHandle.staticParameters.protocolVersion

        val featureFlagsForPV: Set[ParticipantTopologyFeatureFlag] =
          ParticipantProtocolFeatureFlags.supportedFeatureFlagsByPV.getOrElse(
            protocolVersion,
            Set.empty,
          )

        val topologyManager =
          synchronizerHandle.syncPersistentState.topologyManager

        def updateSTCWithFeatureFlags(
            existingSynchronizerTrustCertificate: TopologyTransaction[
              TopologyChangeOp.Replace,
              SynchronizerTrustCertificate,
            ]
        ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransaction] =
          synchronizeWithClosing("updating STC for feature flags auto sync")(
            topologyManager.proposeAndAuthorize(
              op = TopologyChangeOp.Replace,
              mapping = existingSynchronizerTrustCertificate.mapping.copy(
                featureFlags = featureFlagsForPV.toSeq
              ),
              serial = Some(existingSynchronizerTrustCertificate.serial.increment),
              signingKeys = Seq.empty,
              protocolVersion = protocolVersion,
              expectFullAuthorization = false,
              forceChanges = ForceFlags.none,
              waitToBecomeEffective = None,
            )
          )

        val result = for {
          // Lookup the existing STCs for the node
          stcTransactions <- EitherT.liftF[
            FutureUnlessShutdown,
            TopologyManagerError,
            PositiveStoredTopologyTransactions,
          ](
            topologyManager.store
              .findPositiveTransactions(
                // We want the current effective STC
                asOf = CantonTimestamp.MaxValue,
                asOfInclusive = true,
                isProposal = false,
                types = Seq(TopologyMapping.Code.SynchronizerTrustCertificate),
                filterUid = Some(NonEmpty.mk(Seq, participantId.uid)),
                filterNamespace = None,
              )
          )
          currentStcO = stcTransactions.result
            .maxByOption(_.validFrom)
            .flatMap(
              _.transaction
                .select[TopologyChangeOp.Replace, SynchronizerTrustCertificate]
            )
          _ <- currentStcO match {
            // There should already be an STC present, so we only update if the feature flags differ
            case Some(currentStc) if currentStc.mapping.featureFlags.toSet != featureFlagsForPV =>
              logger.info(
                s"Synchronizer Trust Certificate in the synchronizer store $synchronizerId for $participantId does not have the expected set of feature flags, they will be updated. " +
                  s"Current: ${currentStc.mapping.featureFlags.toSet}. Expected: $featureFlagsForPV"
              )
              updateSTCWithFeatureFlags(currentStc.transaction)
            case Some(_) =>
              EitherT.pure[FutureUnlessShutdown, TopologyManagerError](
                logger.debug(
                  s"Synchronizer Trust Certificate in the synchronizer store $synchronizerId for $participantId already has the correct feature flags $featureFlagsForPV. No update necessary."
                )
              )
            // This is unexpected as the node is connected to the synchronizer, so it should have an STC in the synchronizer store
            // (unless it was revoked concurrently but that's very unlikely and still worth logging a warning)
            case None =>
              EitherT.pure[FutureUnlessShutdown, TopologyManagerError](
                logger.warn(
                  s"Expected a Synchronizer Trust Certificate to be present in the synchronizer store $synchronizerId for $participantId but none was found. Feature flags will not be updated"
                )
              )
          }
        } yield ()

        EitherTUtil.doNotAwaitUS(result, "Feature flag synchronization")
      }

      // Initialize, replay and process stored events, then subscribe to new events
      (for {
        _ <- initialize(initializationTraceContext)
        firstUnpersistedEventSc <- EitherT.liftF(firstUnpersistedEventScF)

        monitor = new ConnectedSynchronizer.EventProcessingMonitor(
          ephemeral.startingPoints,
          firstUnpersistedEventSc,
          delayLogger,
          loggerFactory,
        )
        messageHandler =
          new ApplicationHandler[
            Lambda[`+X <: Envelope[_]` => Traced[Seq[PossiblyIgnoredSequencedEvent[X]]]],
            ClosedEnvelope,
          ] {
            override def name: String = s"connected-synchronizer-$psid"

            override def subscriptionStartsAt(
                start: SubscriptionStart,
                synchronizerTimeTracker: SynchronizerTimeTracker,
            )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
              Seq(
                topologyProcessor.subscriptionStartsAt(start, synchronizerTimeTracker)(
                  traceContext
                ),
                trafficProcessor.subscriptionStartsAt(start, synchronizerTimeTracker)(traceContext),
              ).parSequence_

            override def apply(
                tracedEvents: BoxedEnvelope[Lambda[
                  `+X <: Envelope[_]` => Traced[Seq[PossiblyIgnoredSequencedEvent[X]]]
                ], ClosedEnvelope]
            ): HandlerResult =
              tracedEvents.withTraceContext { traceContext => closedEvents =>
                val openEvents = closedEvents.map { event =>
                  val openedEvent = PossiblyIgnoredSequencedEvent.openEnvelopes(event)(
                    staticSynchronizerParameters.protocolVersion,
                    synchronizerCrypto.crypto.pureCrypto,
                  )

                  // Raise alarms
                  // TODO(i11804): Send a rejection
                  openedEvent.openingErrors.foreach { error =>
                    val cause =
                      s"Received an envelope at ${openedEvent.event.timestamp} that cannot be opened. " +
                        s"Discarding envelope... Reason: $error"
                    SyncServiceAlarm.Warn(cause).report()
                  }

                  openedEvent
                }

                messageDispatcher.handleAll(Traced(openEvents)(traceContext))
              }
          }
        _ <- EitherT
          .right[ConnectedSynchronizerInitializationError](
            sequencerClient.subscribeAfter(
              subscriptionPriorTs,
              cleanProcessingTsO,
              monitor(messageHandler),
              ephemeral.timeTracker,
              tc =>
                participantNodePersistentState.value.ledgerApiStore
                  .cleanSynchronizerIndex(psid.logical)(tc, ec)
                  .map(_.flatMap(_.sequencerIndex).map(_.sequencerTimestamp)),
            )(initializationTraceContext)
          )

        // Notify the listeners to the upcoming upgrade, if any
        _ = ephemeral.recordOrderPublisher.getSynchronizerSuccessor.foreach(
          topologyProcessor.terminateProcessing.notifyUpgradeAnnouncement
        )

        // wait for initial topology transactions to be sequenced and received before we start computing pending
        // topology transactions to push for IDM approval
        _ <- waitForParticipantToBeInTopology(initializationTraceContext)
        _ = if (parameters.autoSyncProtocolFeatureFlags)
          synchronizeFeatureFlagsAsync(initializationTraceContext)
        _ <-
          registerIdentityTransactionHandle
            .synchronizerConnected()(initializationTraceContext)
            .leftMap[ConnectedSynchronizerInitializationError](
              ParticipantTopologyHandshakeError.apply
            )
      } yield {
        logger.debug(s"Started synchronizer for $psid")(initializationTraceContext)
        ephemeral.markAsRecovered()
        logger.debug("Sync synchronizer is ready.")(initializationTraceContext)
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          completeAssignment,
          "Failed to complete outstanding assignments on startup. " +
            "You may have to complete the assignments manually.",
        )
        ()
      }).value
    }

  private def completeAssignment(implicit tc: TraceContext): FutureUnlessShutdown[Unit] = {

    val fetchLimit = 1000

    def completeReassignments(
        previous: Option[(CantonTimestamp, Source[SynchronizerId])]
    ): FutureUnlessShutdown[Either[Option[(CantonTimestamp, Source[SynchronizerId])], Unit]] = {
      logger.debug(s"Fetch $fetchLimit pending reassignments")
      val resF = for {
        pendingReassignments <- synchronizeWithClosing(functionFullName)(
          persistent.reassignmentStore.findAfter(
            requestAfter = previous,
            limit = fetchLimit,
          )
        )
        // TODO(i9500): Here, assignments are completed sequentially. Consider running several in parallel to speed
        // this up. It may be helpful to use the `RateLimiter`
        eithers <- MonadUtil
          .sequentialTraverse(pendingReassignments) { data =>
            logger.debug(s"Complete ${data.reassignmentId} after startup")
            val eitherF = synchronizeWithClosing(functionFullName)(
              AutomaticAssignment.perform(
                data.reassignmentId,
                Target(psid),
                Target(staticSynchronizerParameters),
                reassignmentCoordination,
                data.contractsBatch.stakeholders.all,
                data.submitterMetadata,
                participantId,
                data.targetTimestamp,
              )
            )
            eitherF.leftMap(err => data.reassignmentId -> err).value
          }

      } yield {
        // Log any errors, then discard the errors and continue to complete pending reassignments
        eithers.foreach {
          case Left((reassignmentId, error)) =>
            logger.debug(
              s"Failed to complete pending reassignment $reassignmentId. The error was $error."
            )
          case Right(()) => ()
        }

        pendingReassignments.lastOption.map(t => t.unassignmentTs -> t.sourcePSId.map(_.logical))
      }

      resF.map {
        // Continue completing reassignments that are after the last completed reassignment
        case Some(value) => Left(Some(value))
        // We didn't find any uncompleted reassignments, so stop
        case None => Either.unit
      }
    }

    logger.debug(s"Wait for replay to complete")
    for {
      // Wait to see a timestamp >= now from the synchronizer -- when we see such a timestamp, it means that the participant
      // has "caught up" on messages from the synchronizer (and so should have seen all the assignments)
      // TODO(i9009): This assumes the participant and synchronizer clocks are synchronized, which may not be the case
      _waitForReplay <- FutureUnlessShutdown.outcomeF(
        timeTracker.awaitTick(clock.now).getOrElse(Future.unit)
      )

      _params <- synchronizeWithClosing(functionFullName)(
        topologyClient.currentSnapshotApproximation.findDynamicSynchronizerParametersOrDefault(
          staticSynchronizerParameters.protocolVersion
        )
      )

      _bool <- Monad[FutureUnlessShutdown].tailRecM(
        None: Option[(CantonTimestamp, Source[SynchronizerId])]
      )(ts => completeReassignments(ts))
    } yield {
      logger.debug(s"Assignment completion has finished")
    }

  }

  /** A [[ConnectedSynchronizer]] is ready when it has resubscribed to the sequencer client. */
  def ready: Boolean = !ephemeral.isFailed

  def readyForSubmission: SubmissionReady =
    SubmissionReady(ready && !isFailed && !sequencerClient.healthComponent.isFailed)

  /** Helper method to perform the submission (unless shutting down), but track the inner FUS
    * completion as well: on shutdown wait for the inner FUS to complete before closing the
    * child-services.
    */
  private def synchronizeSubmissionWithClosing[ERROR, RESULT](name: String, onClosing: => ERROR)(
      f: => EitherT[FutureUnlessShutdown, ERROR, FutureUnlessShutdown[RESULT]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ERROR, FutureUnlessShutdown[RESULT]] =
    synchronizeWithClosing(name)(Nested(f)).value.onShutdown(Left(onClosing))

  /** @return
    *   The outer future completes after the submission has been registered as in-flight. The inner
    *   future completes after the submission has been sequenced or if it will never be sequenced.
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, ContractInstance],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionSubmissionError, FutureUnlessShutdown[
    TransactionSubmissionResult
  ]] =
    synchronizeSubmissionWithClosing[
      TransactionSubmissionError,
      TransactionSubmissionResult,
    ](functionFullName, SubmissionDuringShutdown.Rejection()) {
      ErrorUtil.requireState(ready, "Cannot submit transaction before recovery")
      transactionProcessor
        .submit(
          submitterInfo,
          transactionMeta,
          keyResolver,
          transaction,
          disclosedContracts,
          topologySnapshot,
        )
    }

  override def submitUnassignments(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractIds: Seq[LfContractId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      sourceTopology: Source[TopologySnapshot],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    UnassignmentProcessingSteps.SubmissionResult
  ]] =
    synchronizeSubmissionWithClosing[
      ReassignmentProcessorError,
      UnassignmentProcessingSteps.SubmissionResult,
    ](
      functionFullName,
      SynchronizerNotReady(psid, "The synchronizer is shutting down."),
    ) {
      logger.debug(
        s"Submitting unassignment of `$contractIds` from `$psid` to `$targetSynchronizer`"
      )

      if (!ready)
        EitherT.leftT(
          SynchronizerNotReady(psid, "Cannot submit unassignment before recovery")
        )
      else
        unassignmentProcessor
          .submit(
            UnassignmentProcessingSteps
              .SubmissionParam(
                submitterMetadata,
                contractIds,
                targetSynchronizer,
              ),
            sourceTopology.unwrap,
          )
    }

  override def submitAssignments(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
      targetTopology: Target[TopologySnapshot],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    AssignmentProcessingSteps.SubmissionResult
  ]] =
    synchronizeSubmissionWithClosing[
      ReassignmentProcessorError,
      AssignmentProcessingSteps.SubmissionResult,
    ](
      functionFullName,
      SynchronizerNotReady(psid, "The synchronizer is shutting down."),
    ) {
      logger.debug(s"Submitting assignment of `$reassignmentId` to `$psid`")

      if (!ready)
        EitherT.leftT(
          SynchronizerNotReady(psid, "Cannot submit unassignment before recovery")
        )
      else
        assignmentProcessor
          .submit(
            AssignmentProcessingSteps
              .SubmissionParam(submitterMetadata, reassignmentId),
            targetTopology.unwrap,
          )
    }

  def numberOfDirtyRequests(): Int = ephemeral.requestJournal.numberOfDirtyRequests

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    sequencerClient.logout()

  // We must run this even before the invocation `closeAsync`,
  // because it will abort tasks that need to complete
  // before `closeAsync` is invoked.
  runOnOrAfterClose_(new RunOnClosing {
    override def name: String = "Cancel promises of ConnectedSynchronizer.promiseUSFactory"
    override def done: Boolean = promiseUSFactory.isClosing
    override def run()(implicit traceContext: TraceContext): Unit = promiseUSFactory.close()
  })(TraceContext.empty)

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    // As the commitment and protocol processors use the sequencer client to send messages, close
    // them before closing the synchronizerHandle. Both of them will ignore the requests from the message dispatcher
    // after they get closed.
    Seq(
      SyncCloseable(
        "connected-synchronizer",
        LifeCycle.close(
          journalGarbageCollector,
          acsCommitmentProcessor,
          transactionProcessor,
          unassignmentProcessor,
          assignmentProcessor,
          badRootHashMessagesRequestProcessor,
          topologyProcessor,
          ephemeral.timeTracker, // need to close time tracker before synchronizer handle, as it might otherwise send messages
          synchronizerHandle,
          ephemeral,
        )(logger),
      )
    )

  override def toString: String =
    s"ConnectedSynchronizer(synchronizerId=$psid, participantId=$participantId)"
}

object ConnectedSynchronizer {
  val healthName: String = "connected-synchronizer"

  // Whether the synchronizer is ready for submission
  final case class SubmissionReady(v: Boolean) extends AnyVal {
    def unwrap: Boolean = v
  }

  private class EventProcessingMonitor(
      startingPoints: ProcessingStartingPoints,
      firstUnpersistedSc: SequencerCounter,
      delayLogger: DelayLogger,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    def apply[Env <: Envelope[?]](
        handler: PossiblyIgnoredApplicationHandler[Env]
    ): PossiblyIgnoredApplicationHandler[Env] = handler.replace { tracedBatch =>
      tracedBatch.withTraceContext { implicit batchTraceContext => tracedEvents =>
        tracedEvents.lastOption.fold(HandlerResult.done) { lastEvent =>
          if (lastEvent.counter >= firstUnpersistedSc) {
            delayLogger.checkForDelay(lastEvent)
          }
          val firstEvent = tracedEvents.headOption.getOrElse(
            throw new RuntimeException("A sequence with a last element must also have a head")
          )

          def batchIncludesCounter(counter: SequencerCounter): Boolean =
            firstEvent.counter <= counter && lastEvent.counter >= counter

          if (batchIncludesCounter(startingPoints.cleanReplay.nextSequencerCounter)) {
            logger.info(
              s"Replaying requests ${startingPoints.cleanReplay.nextRequestCounter} up to clean request index ${startingPoints.processing.nextRequestCounter - 1L}"
            )
          }
          if (batchIncludesCounter(startingPoints.processing.nextSequencerCounter)) {
            logger.info(
              s"Replaying or processing locally stored events with sequencer counters ${startingPoints.processing.nextSequencerCounter} to ${firstUnpersistedSc - 1L}"
            )
          }
          handler(tracedBatch)
        }
      }
    }
  }

  trait Factory[+T <: ConnectedSynchronizer] {

    def create(
        synchronizerHandle: SynchronizerHandle,
        participantId: ParticipantId,
        engine: Engine,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        persistentState: SyncPersistentState,
        ephemeralState: SyncEphemeralState,
        packageService: PackageService,
        synchronizerCrypto: SynchronizerCryptoClient,
        identityPusher: ParticipantTopologyDispatcher,
        topologyProcessorFactory: TopologyTransactionProcessor.Factory,
        missingKeysAlerter: MissingKeysAlerter,
        sequencerConnectionSuccessorListener: SequencerConnectionSuccessorListener,
        reassignmentCoordination: ReassignmentCoordination,
        commandProgressTracker: CommandProgressTracker,
        clock: Clock,
        promiseUSFactory: DefaultPromiseUnlessShutdownFactory,
        connectedSynchronizerMetrics: ConnectedSynchronizerMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): FutureUnlessShutdown[T]
  }

  object DefaultFactory extends Factory[ConnectedSynchronizer] {
    override def create(
        synchronizerHandle: SynchronizerHandle,
        participantId: ParticipantId,
        engine: Engine,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        persistentState: SyncPersistentState,
        ephemeralState: SyncEphemeralState,
        packageService: PackageService,
        synchronizerCrypto: SynchronizerCryptoClient,
        identityPusher: ParticipantTopologyDispatcher,
        topologyProcessorFactory: TopologyTransactionProcessor.Factory,
        missingKeysAlerter: MissingKeysAlerter,
        sequencerConnectionSuccessorListener: SequencerConnectionSuccessorListener,
        reassignmentCoordination: ReassignmentCoordination,
        commandProgressTracker: CommandProgressTracker,
        clock: Clock,
        promiseUSFactory: DefaultPromiseUnlessShutdownFactory,
        connectedSynchronizerMetrics: ConnectedSynchronizerMetrics,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
    )(implicit
        ec: ExecutionContext,
        mat: Materializer,
        tracer: Tracer,
    ): FutureUnlessShutdown[ConnectedSynchronizer] = {
      import TraceContext.Implicits.Empty.*
      val sortedReconciliationIntervalsProvider = new SortedReconciliationIntervalsProvider(
        synchronizerHandle.topologyClient,
        futureSupervisor,
        loggerFactory,
      )
      val journalGarbageCollector = new JournalGarbageCollector(
        persistentState.requestJournalStore,
        tc =>
          ephemeralState.ledgerApiIndexer.ledgerApiStore.value
            .cleanSynchronizerIndex(synchronizerHandle.psid.logical)(tc, ec),
        sortedReconciliationIntervalsProvider,
        persistentState.acsCommitmentStore,
        persistentState.activeContractStore,
        persistentState.submissionTrackerStore,
        participantNodePersistentState.map(_.inFlightSubmissionStore),
        synchronizerHandle.psid,
        parameters.journalGarbageCollectionDelay,
        parameters.processingTimeouts,
        loggerFactory,
      )
      for {
        acsCommitmentProcessor <- AcsCommitmentProcessor(
          participantId,
          synchronizerHandle.sequencerClient,
          synchronizerCrypto,
          sortedReconciliationIntervalsProvider,
          persistentState.acsCommitmentStore,
          journalGarbageCollector.observer,
          connectedSynchronizerMetrics.commitments,
          parameters.processingTimeouts,
          futureSupervisor,
          persistentState.activeContractStore,
          participantNodePersistentState.value.acsCounterParticipantConfigStore,
          participantNodePersistentState.value.contractStore,
          persistentState.enableAdditionalConsistencyChecks,
          loggerFactory,
          testingConfig,
          clock,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          parameters.batchingConfig,
          doNotAwaitOnCheckingIncomingCommitments =
            parameters.doNotAwaitOnCheckingIncomingCommitments,
          commitmentCheckpointInterval = parameters.commitmentCheckpointInterval,
        )
        topologyProcessor <- topologyProcessorFactory.create(
          acsCommitmentProcessor.scheduleTopologyTick
        )
      } yield {
        val contractValidator = ContractValidator(
          synchronizerCrypto.pureCrypto,
          engine,
          packageId => traceContext => packageService.getPackage(packageId)(traceContext),
        )
        new ConnectedSynchronizer(
          synchronizerHandle,
          participantId,
          engine,
          parameters,
          participantNodePersistentState,
          persistentState,
          ephemeralState,
          packageService,
          synchronizerCrypto,
          contractValidator,
          identityPusher,
          topologyProcessor,
          missingKeysAlerter,
          sequencerConnectionSuccessorListener,
          reassignmentCoordination,
          commandProgressTracker,
          ParallelMessageDispatcherFactory,
          journalGarbageCollector,
          acsCommitmentProcessor,
          clock,
          promiseUSFactory,
          connectedSynchronizerMetrics,
          futureSupervisor,
          loggerFactory,
          testingConfig,
        )
      }
    }
  }
}

sealed trait ConnectedSynchronizerInitializationError

final case class AbortedDueToShutdownError(msg: String)
    extends ConnectedSynchronizerInitializationError

final case class SequencedEventStoreError(err: store.SequencedEventStoreError)
    extends ConnectedSynchronizerInitializationError

final case class ParticipantTopologyHandshakeError(err: SynchronizerRegistryError)
    extends ConnectedSynchronizerInitializationError

final case class ParticipantDidNotBecomeActive(msg: String)
    extends ConnectedSynchronizerInitializationError
