// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.{Eval, Monad}
import com.daml.lf.engine.Engine
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.domain.{DomainHandle, DomainRegistryError}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.event.{
  AcsChange,
  ContractMetadataAndTransferCounter,
  ContractStakeholdersAndTransferCounter,
  RecordTime,
}
import com.digitalasset.canton.participant.metrics.{PruningMetrics, SyncDomainMetrics}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.submission.{
  ConfirmationRequestFactory,
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  DomainNotReady,
  TransferProcessorError,
}
import com.digitalasset.canton.participant.protocol.transfer.*
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  PruneObserver,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.{
  ContractChange,
  ParticipantNodePersistentState,
  StateChangeType,
  StoredContract,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.topology.ParticipantTopologyDispatcherCommon
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.traffic.TrafficStateController
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.PeriodicAcknowledgements
import com.digitalasset.canton.sequencing.handlers.CleanSequencerCounterTracker
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, Envelope, EventWithErrors}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.AuthorityOfResponse
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  TopologyTransactionProcessorCommon,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.traffic.MemberTrafficStatus
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/** A connected domain from the synchronization service.
  *
  * @param domainId          The identifier of the connected domain.
  * @param domainHandle      A domain handle providing sequencer clients.
  * @param participantId     The participant node id hosting this sync service.
  * @param persistent        The persistent state of the sync domain.
  * @param ephemeral         The ephemeral state of the sync domain.
  * @param packageService    Underlying package management service.
  * @param domainCrypto      Synchronisation crypto utility combining IPS and Crypto operations for a single domain.
  */
class SyncDomain(
    val domainId: DomainId,
    domainHandle: DomainHandle,
    participantId: ParticipantId,
    engine: Engine,
    authorityResolver: AuthorityResolver,
    parameters: ParticipantNodeParameters,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    private[sync] val persistent: SyncDomainPersistentState,
    val ephemeral: SyncDomainEphemeralState,
    val packageService: PackageService,
    domainCrypto: DomainSyncCryptoClient,
    identityPusher: ParticipantTopologyDispatcherCommon,
    topologyProcessorFactory: TopologyTransactionProcessorCommon.Factory,
    missingKeysAlerter: MissingKeysAlerter,
    transferCoordination: TransferCoordination,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    messageDispatcherFactory: MessageDispatcher.Factory[MessageDispatcher],
    clock: Clock,
    pruningMetrics: PruningMetrics,
    metrics: SyncDomainMetrics,
    trafficStateController: TrafficStateController,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
    skipRecipientsCheck: Boolean,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with StartAndCloseable[Either[SyncDomainInitializationError, Unit]]
    with TransferSubmissionHandle
    with CloseableHealthComponent
    with AtomicHealthComponent
    with HasCloseContext {

  val topologyClient: DomainTopologyClientWithInit = domainHandle.topologyClient

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  override val name: String = SyncDomain.healthName
  override def initialHealthState: ComponentHealthState = ComponentHealthState.NotInitializedState
  override def closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from domain")

  private[canton] val sequencerClient = domainHandle.sequencerClient
  val timeTracker: DomainTimeTracker = ephemeral.timeTracker
  val staticDomainParameters: StaticDomainParameters = domainHandle.staticParameters

  private val seedGenerator =
    new SeedGenerator(domainCrypto.crypto.pureCrypto)

  private[canton] val requestGenerator =
    ConfirmationRequestFactory(participantId, domainId, staticDomainParameters.protocolVersion)(
      domainCrypto.crypto.pureCrypto,
      seedGenerator,
      parameters.loggingConfig,
      staticDomainParameters.uniqueContractKeys,
      loggerFactory,
    )

  private val damle =
    new DAMLe(
      pkgId => traceContext => packageService.getPackage(pkgId)(traceContext),
      authorityResolver,
      Some(domainId),
      engine,
      loggerFactory,
    )

  private val transactionProcessor: TransactionProcessor = new TransactionProcessor(
    participantId,
    requestGenerator,
    domainId,
    damle,
    staticDomainParameters,
    domainCrypto,
    sequencerClient,
    inFlightSubmissionTracker,
    ephemeral,
    metrics.transactionProcessing,
    timeouts,
    loggerFactory,
    futureSupervisor,
    skipRecipientsCheck = skipRecipientsCheck,
    enableContractUpgrading = parameters.enableContractUpgrading,
  )

  private val transferOutProcessor: TransferOutProcessor = new TransferOutProcessor(
    SourceDomainId(domainId),
    participantId,
    damle,
    transferCoordination,
    inFlightSubmissionTracker,
    ephemeral,
    domainCrypto,
    seedGenerator,
    sequencerClient,
    timeouts,
    SourceProtocolVersion(staticDomainParameters.protocolVersion),
    loggerFactory,
    futureSupervisor,
    skipRecipientsCheck = skipRecipientsCheck,
  )

  private val transferInProcessor: TransferInProcessor = new TransferInProcessor(
    TargetDomainId(domainId),
    participantId,
    damle,
    transferCoordination,
    inFlightSubmissionTracker,
    ephemeral,
    domainCrypto,
    seedGenerator,
    sequencerClient,
    timeouts,
    TargetProtocolVersion(staticDomainParameters.protocolVersion),
    loggerFactory,
    futureSupervisor,
    skipRecipientsCheck = skipRecipientsCheck,
  )

  private val sortedReconciliationIntervalsProvider = new SortedReconciliationIntervalsProvider(
    topologyClient,
    futureSupervisor,
    loggerFactory,
  )
  private val pruneObserver = new PruneObserver(
    persistent.requestJournalStore,
    persistent.sequencerCounterTrackerStore,
    sortedReconciliationIntervalsProvider,
    persistent.acsCommitmentStore,
    persistent.activeContractStore,
    persistent.contractKeyJournal,
    persistent.submissionTrackerStore,
    participantNodePersistentState.map(_.inFlightSubmissionStore),
    domainId,
    clock,
    timeouts,
    loggerFactory,
  )

  private val acsCommitmentProcessor = {
    val listener = new AcsCommitmentProcessor(
      domainId,
      participantId,
      sequencerClient,
      domainCrypto,
      sortedReconciliationIntervalsProvider,
      persistent.acsCommitmentStore,
      pruneObserver.observer(_),
      pruningMetrics,
      staticDomainParameters.protocolVersion,
      timeouts,
      futureSupervisor,
      persistent.activeContractStore,
      persistent.contractStore,
      persistent.enableAdditionalConsistencyChecks,
      loggerFactory,
    )
    ephemeral.recordOrderPublisher.setAcsChangeListener(listener)
    listener
  }
  private val topologyProcessor = topologyProcessorFactory.create(
    acsCommitmentProcessor.scheduleTopologyTick
  )

  private val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor =
    new BadRootHashMessagesRequestProcessor(
      ephemeral,
      domainCrypto,
      sequencerClient,
      domainId,
      participantId,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  private val repairProcessor: RepairProcessor =
    new RepairProcessor(
      ephemeral.requestCounterAllocator,
      loggerFactory,
    )

  private val registerIdentityTransactionHandle = identityPusher.createHandler(
    domainHandle.domainAlias,
    domainId,
    staticDomainParameters.protocolVersion,
    domainHandle.topologyClient,
    domainHandle.sequencerClient,
  )

  private val messageDispatcher: MessageDispatcher =
    messageDispatcherFactory.create(
      staticDomainParameters.protocolVersion,
      staticDomainParameters.uniqueContractKeys,
      domainId,
      participantId,
      ephemeral.requestTracker,
      transactionProcessor,
      transferOutProcessor,
      transferInProcessor,
      registerIdentityTransactionHandle.processor,
      topologyProcessor,
      acsCommitmentProcessor.processBatch,
      ephemeral.requestCounterAllocator,
      ephemeral.recordOrderPublisher,
      badRootHashMessagesRequestProcessor,
      repairProcessor,
      inFlightSubmissionTracker,
      loggerFactory,
      metrics,
    )

  def getTrafficControlState(implicit tc: TraceContext): Future[Option[MemberTrafficStatus]] =
    trafficStateController.getState

  def authorityOfInSnapshotApproximation(requestingAuthority: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): Future[AuthorityOfResponse] =
    topologyClient.currentSnapshotApproximation.authorityOf(requestingAuthority)

  private def initialize(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainInitializationError, Unit] = {
    def liftF[A](f: Future[A]): EitherT[Future, SyncDomainInitializationError, A] = EitherT.liftF(f)

    def withMetadataSeq(cids: Seq[LfContractId]): Future[Seq[StoredContract]] =
      persistent.contractStore
        .lookupManyUncached(cids)
        .valueOr { missingContractId =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Contract $missingContractId is in the active contract store but not in the contract store"
            )
          )
        }

    def lookupChangeMetadata(change: ActiveContractIdsChange): Future[AcsChange] = {
      for {
        // TODO(i9270) extract magic numbers
        storedActivatedContracts <- MonadUtil.batchedSequentialTraverse(
          parallelism = PositiveInt.tryCreate(20),
          chunkSize = PositiveInt.tryCreate(500),
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
              c.contractId -> WithContractHash.fromContract(
                c.contract,
                ContractMetadataAndTransferCounter(
                  c.contract.metadata,
                  change.activations(c.contractId).transferCounter,
                ),
              )
            )
            .toMap,
          deactivations = storedDeactivatedContracts
            .map(c =>
              c.contractId -> WithContractHash
                .fromContract(
                  c.contract,
                  ContractStakeholdersAndTransferCounter(
                    c.contract.metadata.stakeholders,
                    change.deactivations(c.contractId).transferCounter,
                  ),
                )
            )
            .toMap,
        )
      }
    }

    // pre-inform the ACS commitment processor about upcoming topology changes.
    // as we future date the topology changes, they happen at an effective time
    // and not necessarily at a timestamp triggered by a sequencer message
    // therefore, we need to pre-register them with the acs commitment processor
    // who will then consume them once a tick with a higher timestamp is observed.
    // on a restart, we can just load the relevant effective times from the database
    def loadPendingEffectiveTimesFromTopologyStore(
        timestamp: CantonTimestamp
    ): EitherT[Future, SyncDomainInitializationError, Unit] = {
      val store = domainHandle.domainPersistentState.topologyStore
      EitherT.right(store.findUpcomingEffectiveChanges(timestamp).map { changes =>
        changes.headOption.foreach { head =>
          logger.debug(
            s"Initialising the acs commitment processor with ${changes.length} effective times starting from: ${head.effective}"
          )
          acsCommitmentProcessor.initializeTicksOnStartup(changes.map(_.effective).toList)
        }
      })
    }

    def replayAcsChanges(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SyncDomainInitializationError, LazyList[(RecordTime, AcsChange)]] = {
      liftF(for {
        contractIdChanges <- persistent.activeContractStore
          .changesBetween(fromExclusive, toInclusive)
        changes <- contractIdChanges.parTraverse { case (toc, change) =>
          val changeWithAdjustedTransferCountersForUnassignments = ActiveContractIdsChange(
            change.activations,
            change.deactivations.fmap {
              case StateChangeType(ContractChange.Unassigned, transferCounter) =>
                StateChangeType(ContractChange.Unassigned, transferCounter.map(_ - 1))
              case change => change
            },
          )
          lookupChangeMetadata(changeWithAdjustedTransferCountersForUnassignments).map(ch =>
            (RecordTime.fromTimeOfChange(toc), ch)
          )
        }
      } yield {
        logger.info(
          s"Replaying ${changes.size} ACS changes between $fromExclusive (exclusive) and $toInclusive to the commitment processor"
        )
        logger.debug(
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
    }

    def initializeClientAtCleanHead(): Future[Unit] = {
      // generally, the topology client will be initialised by the topology processor. however,
      // if there is nothing to be replayed, then the topology processor will only be initialised
      // once the first event is dispatched.
      // however, this is bad for transfer processing as we need to be able to access the topology state
      // across domains and this requires that the clients are separately initialised on the participants
      val resubscriptionTs =
        ephemeral.startingPoints.rewoundSequencerCounterPrehead.fold(CantonTimestamp.MinValue)(
          _.timestamp
        )
      logger.debug(s"Initialising topology client at clean head=$resubscriptionTs")
      // startup with the resubscription-ts
      topologyClient.updateHead(
        EffectiveTime(resubscriptionTs),
        ApproximateTime(resubscriptionTs),
        potentialTopologyChange = true,
      )
      // now, compute epsilon at resubscriptionTs
      topologyClient
        .awaitSnapshot(resubscriptionTs)
        .flatMap(
          _.findDynamicDomainParametersOrDefault(
            staticDomainParameters.protocolVersion,
            warnOnUsingDefault = false,
          )
        )
        .map(_.topologyChangeDelay)
        .map { topologyChangeDelay =>
          // update client
          topologyClient.updateHead(
            EffectiveTime(resubscriptionTs.plus(topologyChangeDelay.duration)),
            ApproximateTime(resubscriptionTs),
            potentialTopologyChange = true,
          )
        }
    }

    val startingPoints = ephemeral.startingPoints
    val cleanHeadRc = startingPoints.processing.nextRequestCounter
    val cleanPreHeadO = startingPoints.processing.cleanRequestPrehead
    val cleanHeadPrets = startingPoints.processing.prenextTimestamp

    for {
      // Prepare missing key alerter
      _ <- EitherT.right(missingKeysAlerter.init())

      // Phase 0: Initialise topology client at current clean head
      _ <- EitherT.right(initializeClientAtCleanHead())

      // Phase 1: remove in-flight submissions that have been sequenced and published,
      // but not yet removed from the in-flight submission store
      //
      // Remove and complete all in-flight submissions that have been published at the multi-domain event log.
      _ <- EitherT.right(
        inFlightSubmissionTracker.recoverDomain(
          domainId,
          startingPoints.processing.prenextTimestamp,
        )
      )

      // Phase 2: recover events that have been published to the single-dimension event log, but were not published at
      // the multi-domain event log before the crash
      pending <- cleanPreHeadO.fold(
        EitherT.pure[Future, SyncDomainInitializationError](Seq[PendingPublish]())
      ) { lastProcessedOffset =>
        EitherT.right(
          participantNodePersistentState.value.multiDomainEventLog
            .fetchUnpublished(
              id = persistent.eventLog.id,
              upToInclusiveO = Some(lastProcessedOffset),
            )
        )
      }

      _unit = ephemeral.recordOrderPublisher.scheduleRecoveries(pending)

      // Phase 3: Initialize the repair processor
      repairs <- EitherT.right[SyncDomainInitializationError](
        persistent.requestJournalStore.repairRequests(
          ephemeral.startingPoints.cleanReplay.nextRequestCounter
        )
      )
      _ = logger.info(
        show"Found ${repairs.size} repair requests at request counters ${repairs.map(_.rc)}"
      )
      _ = repairProcessor.setRemainingRepairRequests(repairs)

      // Phase 4: publish ACS changes from some suitable point up to clean head timestamp to the commitment processor.
      // The "suitable point" must ensure that the [[com.digitalasset.canton.participant.store.AcsSnapshotStore]]
      // receives any partially-applied changes; choosing the timestamp returned by the store is sufficient and optimal
      // in terms of performance, but any earlier timestamp is also correct
      acsChangesReplayStartRt <- liftF(persistent.acsCommitmentStore.runningCommitments.watermark)
      _ <- loadPendingEffectiveTimesFromTopologyStore(acsChangesReplayStartRt.timestamp)
      acsChangesToReplay <-
        if (
          cleanHeadPrets >= acsChangesReplayStartRt.timestamp && cleanHeadRc > RequestCounter.Genesis
        ) {
          logger.info(
            s"Looking for ACS changes to replay between ${acsChangesReplayStartRt.timestamp} and $cleanHeadPrets"
          )
          replayAcsChanges(
            acsChangesReplayStartRt.toTimeOfChange,
            TimeOfChange(cleanHeadRc, cleanHeadPrets),
          )
        } else EitherT.pure[Future, SyncDomainInitializationError](Seq.empty)
      _ = acsChangesToReplay.foreach { case (toc, change) =>
        acsCommitmentProcessor.publish(toc, change)
      }
    } yield ()
  }

  protected def startAsync()(implicit
      initializationTraceContext: TraceContext
  ): Future[Either[SyncDomainInitializationError, Unit]] = {

    val delayLogger =
      new DelayLogger(
        clock,
        logger,
        parameters.delayLoggingThreshold,
        metrics.sequencerClient.delay,
      )

    def firstUnpersistedEventScF: Future[SequencerCounter] =
      persistent.sequencedEventStore
        .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))(initializationTraceContext)
        .fold(_ => SequencerCounter.Genesis, _.counter + 1)

    val sequencerCounterPreheadTsO =
      ephemeral.startingPoints.rewoundSequencerCounterPrehead.map(_.timestamp)
    val subscriptionPriorTs = {
      val cleanReplayTs = ephemeral.startingPoints.cleanReplay.prenextTimestamp
      val sequencerCounterPreheadTs = sequencerCounterPreheadTsO.getOrElse(CantonTimestamp.MinValue)
      Ordering[CantonTimestamp].min(cleanReplayTs, sequencerCounterPreheadTs)
    }

    def waitForParticipantToBeInTopology(
        initializationTraceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SyncDomainInitializationError, Unit] =
      EitherT(
        domainHandle.topologyClient
          .await(_.isParticipantActive(participantId), timeouts.verifyActive.duration)(
            initializationTraceContext
          )
          .map(isActive =>
            if (isActive) Right(())
            else
              Left(
                ParticipantDidNotBecomeActive(
                  s"Participant did not become active after ${timeouts.verifyActive.duration}"
                )
              )
          )
      )

    // Initialize, replay and process stored events, then subscribe to new events
    (for {
      _ <- initialize(initializationTraceContext)
      firstUnpersistedEventSc <- EitherT.liftF(firstUnpersistedEventScF)
      monitor = new SyncDomain.EventProcessingMonitor(
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
          override def name: String = s"sync-domain-$domainId"

          override def subscriptionStartsAt(
              start: SubscriptionStart,
              domainTimeTracker: DomainTimeTracker,
          )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
            topologyProcessor.subscriptionStartsAt(start, domainTimeTracker)(traceContext)

          override def apply(
              tracedEvents: BoxedEnvelope[Lambda[
                `+X <: Envelope[_]` => Traced[Seq[PossiblyIgnoredSequencedEvent[X]]]
              ], ClosedEnvelope]
          ): HandlerResult = {
            tracedEvents.withTraceContext { traceContext => closedEvents =>
              val openEvents = closedEvents.map { event =>
                trafficStateController.updateState(event)
                val openedEvent = PossiblyIgnoredSequencedEvent.openEnvelopes(event)(
                  staticDomainParameters.protocolVersion,
                  domainCrypto.crypto.pureCrypto,
                )

                openedEvent match {
                  case Right(_) =>
                  case Left(Traced(EventWithErrors(content, openingErrors, _isIgnored))) =>
                    // Raise alarms
                    // TODO(i11804): Send a rejection
                    openingErrors.foreach { error =>
                      val cause =
                        s"Received an envelope at ${content.timestamp} that cannot be opened. " +
                          s"Discarding envelope... Reason: $error"
                      SyncServiceAlarm.Warn(cause).report()
                    }
                }

                openedEvent
              }

              messageDispatcher.handleAll(Traced(openEvents)(traceContext))
            }
          }
        }
      eventHandler = monitor(messageHandler)

      cleanSequencerCounterTracker = new CleanSequencerCounterTracker(
        persistent.sequencerCounterTrackerStore,
        ephemeral.timelyRejectNotifier.notifyAsync,
        loggerFactory,
      )
      trackingHandler = cleanSequencerCounterTracker(eventHandler)
      _ <- EitherT.right[SyncDomainInitializationError](
        sequencerClient.subscribeAfter(
          subscriptionPriorTs,
          sequencerCounterPreheadTsO,
          trackingHandler,
          ephemeral.timeTracker,
          PeriodicAcknowledgements.fetchCleanCounterFromStore(
            persistent.sequencerCounterTrackerStore
          ),
        )(initializationTraceContext)
      )

      // wait for initial topology transactions to be sequenced and received before we start computing pending
      // topology transactions to push for IDM approval
      _ <- waitForParticipantToBeInTopology(initializationTraceContext).onShutdown(Right(()))
      _ <-
        registerIdentityTransactionHandle
          .domainConnected()(initializationTraceContext)
          .onShutdown(Right(()))
          .leftMap[SyncDomainInitializationError](ParticipantTopologyHandshakeError)
    } yield {
      logger.debug(s"Started sync domain for $domainId")(initializationTraceContext)
      ephemeral.markAsRecovered()
      logger.debug("Sync domain is ready.")(initializationTraceContext)
      FutureUtil.doNotAwait(
        completeTransferIn.unwrap,
        "Failed to complete outstanding transfer-ins on startup. " +
          "You may have to complete the transfer-ins manually.",
      )
      ()
    }).value
  }

  private def completeTransferIn(implicit tc: TraceContext): FutureUnlessShutdown[Unit] = {

    val fetchLimit = 1000

    def completeTransfers(
        previous: Option[(CantonTimestamp, SourceDomainId)]
    ): FutureUnlessShutdown[Either[Option[(CantonTimestamp, SourceDomainId)], Unit]] = {
      logger.debug(s"Fetch $fetchLimit pending transfers")
      val resF = for {
        pendingTransfers <- performUnlessClosingF(functionFullName)(
          persistent.transferStore.findAfter(
            requestAfter = previous,
            limit = fetchLimit,
          )
        )
        // TODO(i9500): Here, transfer-ins are completed sequentially. Consider running several in parallel to speed
        // this up. It may be helpful to use the `RateLimiter`
        eithers <- MonadUtil
          .sequentialTraverse(pendingTransfers) { data =>
            logger.debug(s"Complete ${data.transferId} after startup")
            val eitherF =
              performUnlessClosingEitherU[TransferProcessorError, Unit](functionFullName)(
                AutomaticTransferIn.perform(
                  data.transferId,
                  TargetDomainId(domainId),
                  transferCoordination,
                  data.contract.metadata.stakeholders,
                  data.transferOutRequest.submitterMetadata,
                  participantId,
                  data.transferOutRequest.targetTimeProof.timestamp,
                )
              )
            eitherF.value.map(_.left.map(err => data.transferId -> err))
          }

      } yield {
        // Log any errors, then discard the errors and continue to complete pending transfers
        eithers.foreach {
          case Left((transferId, error)) =>
            logger.debug(s"Failed to complete pending transfer $transferId. The error was $error.")
          case Right(()) => ()
        }

        pendingTransfers.lastOption.map(t => t.transferId.transferOutTimestamp -> t.sourceDomain)
      }

      resF.map {
        // Continue completing transfers that are after the last completed transfer
        case Some(value) => Left(Some(value))
        // We didn't find any uncompleted transfers, so stop
        case None => Right(())
      }
    }

    logger.debug(s"Wait for replay to complete")
    for {
      // Wait to see a timestamp >= now from the domain -- when we see such a timestamp, it means that the participant
      // has "caught up" on messages from the domain (and so should have seen all the transfer-ins)
      // TODO(i9009): This assumes the participant and domain clocks are synchronized, which may not be the case
      waitForReplay <- FutureUnlessShutdown.outcomeF(
        timeTracker
          .awaitTick(clock.now)
          .map(_.void)
          .getOrElse(Future.unit)
      )

      params <- performUnlessClosingF(functionFullName)(
        topologyClient.currentSnapshotApproximation.findDynamicDomainParametersOrDefault(
          staticDomainParameters.protocolVersion
        )
      )

      _bool <- Monad[FutureUnlessShutdown].tailRecM(
        None: Option[(CantonTimestamp, SourceDomainId)]
      )(ts => completeTransfers(ts))
    } yield {
      logger.debug(s"Transfer in completion has finished")
    }

  }

  /** A [[SyncDomain]] is ready when it has resubscribed to the sequencer client. */
  def ready: Boolean = !ephemeral.isFailed

  def readyForSubmission: Boolean =
    ready && !isFailed && !sequencerClient.healthComponent.isFailed

  /** @return The outer future completes after the submission has been registered as in-flight.
    *          The inner future completes after the submission has been sequenced or if it will never be sequenced.
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionSubmissionError, FutureUnlessShutdown[
    TransactionSubmitted
  ]] =
    performUnlessClosingEitherT[TransactionSubmissionError, FutureUnlessShutdown[
      TransactionSubmitted
    ]](
      functionFullName,
      SubmissionDuringShutdown.Rejection(),
    ) {
      ErrorUtil.requireState(ready, "Cannot submit transaction before recovery")
      transactionProcessor
        .submit(submitterInfo, transactionMeta, keyResolver, transaction, disclosedContracts)
        .onShutdown(Left(SubmissionDuringShutdown.Rejection()))
    }

  override def submitTransferOut(
      submitterMetadata: TransferSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, FutureUnlessShutdown[
    TransferOutProcessingSteps.SubmissionResult
  ]] =
    performUnlessClosingEitherT[
      TransferProcessorError,
      FutureUnlessShutdown[TransferOutProcessingSteps.SubmissionResult],
    ](functionFullName, DomainNotReady(domainId, "The domain is shutting down.")) {
      logger.debug(s"Submitting transfer-out of `$contractId` from `$domainId` to `$targetDomain`")

      if (!ready)
        DomainNotReady(domainId, "Cannot submit transfer-out before recovery").discard
      transferOutProcessor
        .submit(
          TransferOutProcessingSteps
            .SubmissionParam(
              submitterMetadata,
              contractId,
              targetDomain,
              targetProtocolVersion,
            )
        )
        .onShutdown(Left(DomainNotReady(domainId, "The domain is shutting down")))
    }

  override def submitTransferIn(
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      sourceProtocolVersion: SourceProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, FutureUnlessShutdown[
    TransferInProcessingSteps.SubmissionResult
  ]] =
    performUnlessClosingEitherT[TransferProcessorError, FutureUnlessShutdown[
      TransferInProcessingSteps.SubmissionResult
    ]](
      functionFullName,
      DomainNotReady(domainId, "The domain is shutting down."),
    ) {
      logger.debug(s"Submitting transfer-in of `$transferId` to `$domainId`")

      if (!ready)
        DomainNotReady(domainId, "Cannot submit transfer-out before recovery").discard

      transferInProcessor
        .submit(
          TransferInProcessingSteps
            .SubmissionParam(
              submitterMetadata,
              transferId,
              sourceProtocolVersion,
            )
        )
        .onShutdown(Left(DomainNotReady(domainId, "The domain is shutting down")))
    }

  def numberOfDirtyRequests(): Int = ephemeral.requestJournal.numberOfDirtyRequests

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    // As the commitment and protocol processors use the sequencer client to send messages, close
    // them before closing the domainHandle. Both of them will ignore the requests from the message dispatcher
    // after they get closed.
    Seq(
      SyncCloseable(
        "sync-domain",
        Lifecycle.close(
          // Close the domain crypto client first to stop waiting for snapshots that may block the sequencer subscription
          domainCrypto,
          // Close the sequencer client so that the processors won't receive or handle events when
          // their shutdown is initiated.
          domainHandle.sequencerClient,
          pruneObserver,
          acsCommitmentProcessor,
          transactionProcessor,
          transferOutProcessor,
          transferInProcessor,
          badRootHashMessagesRequestProcessor,
          topologyProcessor,
          ephemeral.timeTracker, // need to close time tracker before domain handle, as it might otherwise send messages
          domainHandle,
          ephemeral,
        )(logger),
      )
    )

  override def toString: String = s"SyncDomain(domain=$domainId, participant=$participantId)"
}

object SyncDomain {
  val healthName: String = "sync-domain"

  private class EventProcessingMonitor(
      startingPoints: ProcessingStartingPoints,
      firstUnpersistedSc: SequencerCounter,
      delayLogger: DelayLogger,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    def apply[Env <: Envelope[_]](
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
              s"Replaying requests ${startingPoints.cleanReplay.nextRequestCounter} up to clean prehead ${startingPoints.processing.nextRequestCounter - 1L}"
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

  trait Factory[+T <: SyncDomain] {

    def create(
        domainId: DomainId,
        domainHandle: DomainHandle,
        participantId: ParticipantId,
        engine: Engine,
        authorityResolver: AuthorityResolver,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        persistentState: SyncDomainPersistentState,
        ephemeralState: SyncDomainEphemeralState,
        packageService: PackageService,
        domainCrypto: DomainSyncCryptoClient,
        identityPusher: ParticipantTopologyDispatcherCommon,
        topologyProcessorFactory: TopologyTransactionProcessorCommon.Factory,
        missingKeysAlerter: MissingKeysAlerter,
        transferCoordination: TransferCoordination,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        clock: Clock,
        pruningMetrics: PruningMetrics,
        syncDomainMetrics: SyncDomainMetrics,
        trafficStateController: TrafficStateController,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        skipRecipientsCheck: Boolean,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): T
  }

  object DefaultFactory extends Factory[SyncDomain] {
    override def create(
        domainId: DomainId,
        domainHandle: DomainHandle,
        participantId: ParticipantId,
        engine: Engine,
        authorityResolver: AuthorityResolver,
        parameters: ParticipantNodeParameters,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        persistentState: SyncDomainPersistentState,
        ephemeralState: SyncDomainEphemeralState,
        packageService: PackageService,
        domainCrypto: DomainSyncCryptoClient,
        identityPusher: ParticipantTopologyDispatcherCommon,
        topologyProcessorFactory: TopologyTransactionProcessorCommon.Factory,
        missingKeysAlerter: MissingKeysAlerter,
        transferCoordination: TransferCoordination,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        clock: Clock,
        pruningMetrics: PruningMetrics,
        syncDomainMetrics: SyncDomainMetrics,
        trafficStateController: TrafficStateController,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        skipRecipientsCheck: Boolean,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): SyncDomain =
      new SyncDomain(
        domainId,
        domainHandle,
        participantId,
        engine,
        authorityResolver,
        parameters,
        participantNodePersistentState,
        persistentState,
        ephemeralState,
        packageService,
        domainCrypto,
        identityPusher,
        topologyProcessorFactory,
        missingKeysAlerter,
        transferCoordination,
        inFlightSubmissionTracker,
        MessageDispatcher.DefaultFactory,
        clock,
        pruningMetrics,
        syncDomainMetrics,
        trafficStateController,
        futureSupervisor,
        loggerFactory,
        skipRecipientsCheck = skipRecipientsCheck,
      )
  }
}

sealed trait SyncDomainInitializationError

final case class SequencedEventStoreError(err: store.SequencedEventStoreError)
    extends SyncDomainInitializationError

final case class ParticipantTopologyHandshakeError(err: DomainRegistryError)
    extends SyncDomainInitializationError

final case class ParticipantDidNotBecomeActive(msg: String) extends SyncDomainInitializationError
