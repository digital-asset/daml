// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.error.*
import com.daml.lf.data.Ref.{Party, SubmissionId}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ProcessedDisclosedContract,
  TransferSubmitterMetadata,
}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.SyncServiceErrorGroup
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.health.MutableHealthComponent
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.configuration.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.error.{CommonErrors, PackageServiceErrors}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.v2.ReadService.ConnectedDomainResponse
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError
import com.digitalasset.canton.participant.admin.inspection.{
  JournalGarbageCollectorControl,
  SyncStateInspection,
}
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.participant.protocol.transfer.{
  IncompleteTransferData,
  TransferCoordination,
}
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  NoOpPruningProcessor,
  PruningProcessor,
}
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.MissingConfigForAlias
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceDomainBecamePassive,
  SyncServiceDomainDisabledUs,
  SyncServiceDomainDisconnect,
  SyncServiceFailedDomainConnection,
}
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.Schedulers
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.chaining.scalaUtilChainingOps
import scala.util.{Failure, Right, Success}

/** The Canton-based synchronization service.
  *
  * A single Canton sync service can connect to multiple domains.
  *
  * @param participantId               The participant node id hosting this sync service.
  * @param domainRegistry              Domain registry for connecting to domains.
  * @param domainConnectionConfigStore Storage for domain connection configs
  * @param packageService              Underlying package management service.
  * @param syncCrypto                  Synchronisation crypto utility combining IPS and Crypto operations.
  * @param isActive                    Returns true of the node is the active replica
  */
class CantonSyncService(
    val participantId: ParticipantId,
    private[participant] val domainRegistry: DomainRegistry,
    private[canton] val domainConnectionConfigStore: DomainConnectionConfigStore,
    private[canton] val aliasManager: DomainAliasManager,
    private[canton] val participantNodePersistentState: Eval[ParticipantNodePersistentState],
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    private[canton] val syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    private[canton] val packageService: PackageService,
    topologyManagerOps: ParticipantTopologyManagerOps,
    identityPusher: ParticipantTopologyDispatcherCommon,
    partyNotifier: LedgerServerPartyNotifier,
    val syncCrypto: SyncCryptoApiProvider,
    val pruningProcessor: PruningProcessor,
    engine: Engine,
    syncDomainStateFactory: SyncDomainEphemeralStateFactory,
    clock: Clock,
    resourceManagementService: ResourceManagementService,
    parameters: ParticipantNodeParameters,
    syncDomainFactory: SyncDomain.Factory[SyncDomain],
    indexedStringStore: IndexedStringStore,
    storage: Storage,
    metrics: ParticipantMetrics,
    sequencerInfoLoader: SequencerInfoLoader,
    val isActive: () => Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    skipRecipientsCheck: Boolean,
    multiDomainLedgerAPIEnabled: Boolean,
    testingConfig: TestingConfigInternal,
)(implicit ec: ExecutionContext, mat: Materializer, val tracer: Tracer)
    extends state.v2.WriteService
    with WriteParticipantPruningService
    with state.v2.ReadService
    with FlagCloseable
    with Spanning
    with NamedLogging
    with HasCloseContext {

  import ShowUtil.*

  val syncDomainHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SyncDomain.healthName, timeouts)
  val ephemeralHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SyncDomainEphemeralState.healthName, timeouts)
  val sequencerClientHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)
  val acsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, AcsCommitmentProcessor.healthName, timeouts)

  val maxDeduplicationDuration: NonNegativeFiniteDuration =
    participantNodePersistentState.value.settingsStore.settings.maxDeduplicationDuration
      .getOrElse(throw new RuntimeException("Max deduplication duration is not available"))

  val eventTranslationStrategy = new EventTranslationStrategy(
    multiDomainLedgerAPIEnabled = multiDomainLedgerAPIEnabled,
    excludeInfrastructureTransactions = parameters.excludeInfrastructureTransactions,
  )

  type ConnectionListener = DomainAlias => Unit

  // Listeners to domain connections
  private val connectionListeners = new AtomicReference[List[ConnectionListener]](List.empty)

  def subscribeToConnections(subscriber: ConnectionListener): Unit =
    connectionListeners.updateAndGet(subscriber :: _).discard

  protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** The domains this sync service is connected to. Can change due to connect/disconnect operations.
    * This may contain domains for which recovery is still running.
    * Invariant: All domain IDs in this map have a corresponding domain alias in the alias manager
    * DO NOT PASS THIS MUTABLE MAP TO OTHER CLASSES THAT ONLY REQUIRE READ ACCESS. USE [[connectedDomainsLookup]] INSTEAD
    */
  private val connectedDomainsMap: TrieMap[DomainId, SyncDomain] =
    TrieMap.empty[DomainId, SyncDomain]
  private val connectedDomainsLookup: ConnectedDomainsLookup =
    ConnectedDomainsLookup.create(connectedDomainsMap)

  private val partyAllocation = new PartyAllocation(
    participantId,
    participantNodeEphemeralState,
    topologyManagerOps,
    partyNotifier,
    parameters,
    isActive,
    connectedDomainsLookup,
    loggerFactory,
  )

  private case class AttemptReconnect(
      alias: DomainAlias,
      last: CantonTimestamp,
      retryDelay: Duration,
      trace: TraceContext,
  ) {
    val earliest: CantonTimestamp = last.plusMillis(retryDelay.toMillis)
  }

  // Track domains we would like to "keep on reconnecting until available"
  private val attemptReconnect: TrieMap[DomainAlias, AttemptReconnect] = TrieMap.empty

  private def resolveReconnectAttempts(alias: DomainAlias): Unit =
    attemptReconnect.remove(alias).discard

  // A connected domain is ready if recovery has succeeded
  private[canton] def readySyncDomainById(domainId: DomainId): Option[SyncDomain] =
    connectedDomainsMap.get(domainId).filter(_.ready)

  private def existsReadyDomain: Boolean = connectedDomainsMap.exists { case (_, sync) =>
    sync.ready
  }

  private def syncDomainForAlias(alias: DomainAlias): Option[SyncDomain] =
    aliasManager.domainIdForAlias(alias).flatMap(connectedDomainsMap.get)

  private val domainRouter =
    DomainRouter(
      connectedDomainsLookup,
      domainConnectionConfigStore,
      aliasManager,
      syncCrypto.pureCrypto,
      participantId,
      parameters,
      loggerFactory,
    )(ec)

  private val transferCoordination: TransferCoordination =
    TransferCoordination(
      parameters.transferTimeProofFreshnessProportion,
      syncDomainPersistentStateManager,
      connectedDomainsLookup.get,
      syncCrypto,
      loggerFactory,
    )(ec)

  val protocolVersionGetter: Traced[DomainId] => Option[ProtocolVersion] =
    (tracedDomainId: Traced[DomainId]) =>
      syncDomainPersistentStateManager.protocolVersionFor(tracedDomainId.value)

  val transferService: TransferService = new TransferService(
    domainIdOfAlias = aliasManager.domainIdForAlias,
    submissionHandles = readySyncDomainById,
    transferLookups = domainId =>
      syncDomainPersistentStateManager.get(domainId.unwrap).map(_.transferStore),
    protocolVersionFor = protocolVersionGetter,
  )

  private val commandDeduplicator = new CommandDeduplicatorImpl(
    participantNodePersistentState.map(_.commandDeduplicationStore),
    clock,
    participantNodePersistentState.flatMap(mdel =>
      Eval.always(mdel.multiDomainEventLog.publicationTimeLowerBound)
    ),
    loggerFactory,
  )

  private val inFlightSubmissionTracker = {
    def domainStateFor(domainId: DomainId): Option[InFlightSubmissionTrackerDomainState] =
      connectedDomainsMap.get(domainId).map(_.ephemeral.inFlightSubmissionTrackerDomainState)

    new InFlightSubmissionTracker(
      participantNodePersistentState.map(_.inFlightSubmissionStore),
      participantNodeEphemeralState.participantEventPublisher,
      commandDeduplicator,
      participantNodePersistentState.map(_.multiDomainEventLog),
      domainStateFor,
      timeouts,
      loggerFactory,
    )
  }

  // Setup the propagation from the MultiDomainEventLog to the InFlightSubmissionTracker
  // before we run crash recovery
  participantNodePersistentState.value.multiDomainEventLog.setOnPublish(
    inFlightSubmissionTracker.onPublishListener
  )

  if (isActive()) {
    TraceContext.withNewTraceContext { implicit traceContext =>
      initializeState()
    }
  }

  private val repairServiceDAMLe =
    new DAMLe(
      pkgId => traceContext => packageService.getPackage(pkgId)(traceContext),
      // The repair service should not need any topology-aware authorisation because it only needs DAMLe
      // to check contract instance arguments, which cannot trigger a ResultNeedAuthority.
      CantonAuthorityResolver.topologyUnawareAuthorityResolver,
      None,
      engine,
      parameters.engine.validationPhaseLogging,
      loggerFactory,
    )

  val cantonAuthorityResolver: AuthorityResolver =
    new CantonAuthorityResolver(connectedDomainsLookup, loggerFactory)

  private val connectQueue = new SimpleExecutionQueue(
    "sync-service-connect-and-repair-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  val repairService: RepairService = new RepairService(
    participantId,
    syncCrypto,
    packageService.dependencyResolver,
    repairServiceDAMLe,
    participantNodePersistentState.map(_.multiDomainEventLog),
    syncDomainPersistentStateManager,
    aliasManager,
    parameters,
    Storage.threadsAvailableForWriting(storage),
    indexedStringStore,
    connectedDomainsLookup.isConnected,
    // Share the sync service queue with the repair service, so that repair operations cannot run concurrently with
    // domain connections.
    connectQueue,
    futureSupervisor,
    loggerFactory,
  )

  private val migrationService =
    new SyncDomainMigration(
      aliasManager,
      domainConnectionConfigStore,
      stateInspection,
      repairService,
      prepareDomainConnectionForMigration,
      parameters.processingTimeouts,
      loggerFactory,
    )

  val dynamicDomainParameterGetter =
    new CantonDynamicDomainParameterGetter(
      syncCrypto,
      syncDomainPersistentStateManager.protocolVersionFor,
      aliasManager,
      loggerFactory,
    )

  // Submit a transaction (write service implementation)
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: LfSubmittedTransaction,
      _estimatedInterpretationCost: Long,
      keyResolver: LfKeyResolver,
      disclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] = {
    import scala.jdk.FutureConverters.*
    withSpan("CantonSyncService.submitTransaction") { implicit traceContext => span =>
      span.setAttribute("command_id", submitterInfo.commandId)
      logger.debug(s"Received submit-transaction ${submitterInfo.commandId} from ledger-api server")
      submitTransactionF(
        submitterInfo,
        transactionMeta,
        transaction,
        keyResolver,
        disclosedContracts,
      )
    }.map(result =>
      result.map { _asyncResult =>
        // It's OK to throw away the asynchronous result because its errors were already logged in `submitTransactionF`.
        // We merely retain it until here so that the span ends only after the asynchronous computation
        SubmissionResult.Acknowledged
      }.merge
    ).asJava
  }

  lazy val stateInspection = new SyncStateInspection(
    syncDomainPersistentStateManager,
    participantNodePersistentState,
    parameters.processingTimeouts,
    new JournalGarbageCollectorControl {
      override def disable(domainId: DomainId)(implicit traceContext: TraceContext): Future[Unit] =
        connectedDomainsMap
          .get(domainId)
          .map(_.addJournalGarageCollectionLock())
          .getOrElse(Future.unit)
      override def enable(domainId: DomainId)(implicit traceContext: TraceContext): Unit = {
        connectedDomainsMap
          .get(domainId)
          .foreach(_.removeJournalGarageCollectionLock())
      }
    },
    connectedDomainsLookup,
    participantId,
    loggerFactory,
  )

  override def prune(
      pruneUpToInclusive: LedgerSyncOffset,
      submissionId: LedgerSubmissionId,
      _pruneAllDivulgedContracts: Boolean, // Canton always prunes divulged contracts ignoring this flag
  ): CompletionStage[PruningResult] =
    (withNewTrace("CantonSyncService.prune") { implicit traceContext => span =>
      span.setAttribute("submission_id", submissionId)
      pruneInternally(pruneUpToInclusive)
        .fold(
          err => PruningResult.NotPruned(ErrorCode.asGrpcStatus(err)),
          _ => PruningResult.ParticipantPruned,
        )
        .onShutdown(
          PruningResult.NotPruned(GrpcErrors.AbortedDueToShutdown.Error().asGoogleGrpcStatus)
        )
    }).asJava

  def pruneInternally(
      pruneUpToInclusive: LedgerSyncOffset
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    (for {
      pruneUpToMultiDomainGlobalOffset <- EitherT
        .fromEither[FutureUnlessShutdown](UpstreamOffsetConvert.toGlobalOffset(pruneUpToInclusive))
        .leftMap { message =>
          LedgerPruningOffsetNonCantonFormat(
            s"Specified offset does not convert to a canton multi domain event log global offset: ${message}"
          )
        }
      _pruned <- pruningProcessor.pruneLedgerEvents(pruneUpToMultiDomainGlobalOffset)
    } yield ()).transform {
      case Left(err @ LedgerPruningNothingToPrune(_, _)) =>
        logger.info(
          s"Could not locate pruning point: ${err.message}. Considering success for idempotency"
        )
        Right(())
      case Left(err @ LedgerPruningOnlySupportedInEnterpriseEdition) =>
        logger.warn(
          s"Canton participant pruning not supported in canton-open-source edition: ${err.message}"
        )
        Left(PruningServiceError.PruningNotSupportedInCommunityEdition.Error())
      case Left(err: LedgerPruningOffsetNonCantonFormat) =>
        logger.info(err.message)
        Left(PruningServiceError.NonCantonOffset.Error(err.message))
      case Left(err: LedgerPruningOffsetUnsafeToPrune) =>
        logger.info(s"Unsafe to prune: ${err.message}")
        Left(
          PruningServiceError.UnsafeToPrune.Error(
            err.cause,
            err.message,
            err.lastSafeOffset.fold("")(UpstreamOffsetConvert.fromGlobalOffset(_).toHexString),
          )
        )
      case Left(err: LedgerPruningOffsetUnsafeDomain) =>
        logger.info(s"Unsafe to prune ${err.domain}: ${err.message}")
        Left(
          PruningServiceError.UnsafeToPrune.Error(
            s"no suitable offset for domain ${err.domain}",
            err.message,
            "none",
          )
        )
      case Left(LedgerPruningCancelledDueToShutdown) =>
        logger.info(s"Pruning interrupted due to shutdown")
        Left(PruningServiceError.ParticipantShuttingDown.Error())
      case Left(err) =>
        logger.warn(s"Internal error while pruning: $err")
        Left(PruningServiceError.InternalServerError.Error(err.message))
      case Right(()) => Right(())
    }

  private def submitTransactionF(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: LfSubmittedTransaction,
      keyResolver: LfKeyResolver,
      explicitlyDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): Future[Either[SubmissionResult, FutureUnlessShutdown[_]]] = {

    def processSubmissionError(
        error: TransactionError
    ): Either[SubmissionResult, FutureUnlessShutdown[_]] = {
      error.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "applicationId" -> submitterInfo.applicationId)
      )
      Left(SubmissionResult.SynchronousError(error.rpcStatus()))
    }

    if (isClosing) {
      Future.successful(processSubmissionError(SubmissionDuringShutdown.Rejection()))
    } else if (!isActive()) {
      // this is the only error we can not really return with a rejection, as this is the passive replica ...
      val err = SyncServiceInjectionError.PassiveReplica.Error(
        submitterInfo.applicationId,
        submitterInfo.commandId,
      )
      err.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "applicationId" -> submitterInfo.applicationId)
      )
      Future.successful(Left(SubmissionResult.SynchronousError(err.rpcStatus())))
    } else if (!existsReadyDomain) {
      Future.successful(
        processSubmissionError(SyncServiceInjectionError.NotConnectedToAnyDomain.Error())
      )
    } else {
      val submittedFF = domainRouter.submitTransaction(
        submitterInfo,
        transactionMeta,
        keyResolver,
        transaction,
        explicitlyDisclosedContracts,
      )
      // TODO(i2794) retry command if token expired
      submittedFF.value.transform { result =>
        val loggedResult = result match {
          case Success(Right(sequencedF)) =>
            // Reply with ACK as soon as the submission has been registered as in-flight,
            // and asynchronously send it to the sequencer.
            logger.debug(s"Command ${submitterInfo.commandId} is now in flight.")
            val loggedF = sequencedF.transform {
              case Success(UnlessShutdown.Outcome(_)) =>
                logger.debug(s"Successfully submitted transaction ${submitterInfo.commandId}.")
                Success(UnlessShutdown.Outcome(()))
              case Success(UnlessShutdown.AbortedDueToShutdown) =>
                logger.debug(
                  s"Transaction submission aborted due to shutdown ${submitterInfo.commandId}."
                )
                Success(UnlessShutdown.Outcome(()))
              case Failure(ex) =>
                logger.error(s"Command submission for ${submitterInfo.commandId} failed", ex)
                Success(UnlessShutdown.Outcome(()))
            }
            Right(loggedF)
          case Success(Left(submissionError)) =>
            processSubmissionError(submissionError)
          case Failure(PassiveInstanceException(_reason)) =>
            val err = SyncServiceInjectionError.PassiveReplica.Error(
              submitterInfo.applicationId,
              submitterInfo.commandId,
            )
            Left(SubmissionResult.SynchronousError(err.rpcStatus()))
          case Failure(exception) =>
            val err = SyncServiceInjectionError.InjectionFailure.Failure(exception)
            err.logWithContext()
            Left(SubmissionResult.SynchronousError(err.rpcStatus()))
        }
        Success(loggedResult)
      }
    }
  }

  override def submitConfiguration(
      _maxRecordTimeToBeRemovedUpstream: participant.LedgerSyncRecordTime,
      submissionId: LedgerSubmissionId,
      config: Configuration,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] = {
    logger.info("Canton does not support dynamic reconfiguration of time model")
    CompletableFuture.completedFuture(TransactionError.NotSupported)
  }

  /** Build source for subscription (for ledger api server indexer).
    *
    * @param beginAfterOffset offset after which to emit events
    */
  override def stateUpdates(
      beginAfterOffset: Option[LedgerSyncOffset]
  )(implicit traceContext: TraceContext): Source[(LedgerSyncOffset, Traced[Update]), NotUsed] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.debug(s"Subscribing to stateUpdates from $beginAfterOffset")
      // Plus one since dispatchers use inclusive offsets.
      beginAfterOffset
        .traverse(after => UpstreamOffsetConvert.toGlobalOffset(after).map(_.increment))
        .fold(
          e => Source.failed(new IllegalArgumentException(e)),
          beginStartingAt =>
            participantNodePersistentState.value.multiDomainEventLog
              .subscribe(beginStartingAt)
              .mapConcat { case (offset, event) =>
                event
                  .traverse(eventTranslationStrategy.translate)
                  .map { e =>
                    logger.debug(show"Emitting event at offset $offset. Event: ${event.value}")
                    (UpstreamOffsetConvert.fromGlobalOffset(offset), e)
                  }
              },
        )
    }

  override def allocateParty(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    partyAllocation.allocate(hint, displayName, rawSubmissionId)

  override def uploadPackages(
      submissionId: LedgerSubmissionId,
      dar: ByteString,
      sourceDescription: Option[String],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] = {
    withSpan("CantonSyncService.uploadPackages") { implicit traceContext => span =>
      if (!isActive()) {
        logger.debug(s"Rejecting package upload on passive replica.")
        Future.successful(TransactionError.PassiveNode)
      } else {
        span.setAttribute("submission_id", submissionId)
        packageService
          .upload(
            darBytes = dar,
            fileNameO = None,
            sourceDescriptionO = sourceDescription,
            submissionId = submissionId,
            vetAllPackages = true,
            synchronizeVetting = false,
          )
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(PackageServiceErrors.ParticipantShuttingDown.Error()))
          .valueOr(err => SubmissionResult.SynchronousError(err.rpcStatus()))
      }
    }
  }.asJava

  /** Executes ordered sequence of steps to recover any state that might have been lost if the participant previously
    * crashed. Needs to be invoked after the input stores have been created, but before they are made available to
    * dependent components.
    */
  private def recoverParticipantNodeState()(implicit traceContext: TraceContext): Unit = {

    val participantEventLogId = participantNodePersistentState.value.participantEventLog.id

    // Note that state from domain event logs is recovered when the participant reconnects to domains.

    val recoveryF = {
      logger.info("Recovering published timely rejections")
      // Recover the published in-flight submissions for all domain ids we know
      val domains =
        configuredDomains
          .collect { case domain if domain.status.isActive => domain.config.domain }
          .mapFilter(aliasManager.domainIdForAlias)
      for {
        _ <- inFlightSubmissionTracker.recoverPublishedTimelyRejections(domains)
        _ = logger.info("Publishing the unpublished events from the ParticipantEventLog")
        // These publications will propagate to the CommandDeduplicator and InFlightSubmissionTracker like during normal processing
        unpublished <- participantNodePersistentState.value.multiDomainEventLog.fetchUnpublished(
          participantEventLogId,
          None,
        )
        unpublishedEvents = unpublished.mapFilter {
          case RecordOrderPublisher.PendingTransferPublish(ts, _eventLogId) =>
            logger.error(
              s"Pending transfer event with timestamp $ts found in participant event log " +
                s"$participantEventLogId. Participant event log should not contain transfers."
            )
            None
          case RecordOrderPublisher.PendingEventPublish(tse, _ts, _eventLogId) => Some(tse)
        }

        _units <- MonadUtil.sequentialTraverse(unpublishedEvents) { tse =>
          participantNodePersistentState.value.multiDomainEventLog.publish(
            PublicationData(participantEventLogId, tse, None)
          )
        }
      } yield {
        logger.debug(s"Participant event log recovery completed")
      }
    }

    // also resume pending party notifications
    val resumePendingF = recoveryF.flatMap { _ =>
      partyNotifier.resumePending()
    }

    parameters.processingTimeouts.unbounded.await(
      "Wait for participant event log recovery to finish"
    )(resumePendingF)
  }

  def initializeState()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Invoke crash recovery or initialize active participant")

    // Important to invoke recovery before we do anything else with persisted stores.
    recoverParticipantNodeState()

    // Starting with Daml 1.1.0, the ledger api server requires the ledgers to publish their time model as the
    // first event. Only do so on brand new ledgers without preexisting state.
    logger.debug("Publishing time model configuration event if ledger is brand new")
    parameters.processingTimeouts.default
      .await("Publish time model configuration event if ledger is brand new")(
        participantNodeEphemeralState.participantEventPublisher.publishTimeModelConfigNeededUpstreamOnlyIfFirst
          .onShutdown(logger.debug("Aborted publishing of time model due to shutdown"))
      )
  }

  /** Returns the ready domains this sync service is connected to. */
  def readyDomains: Map[DomainAlias, (DomainId, Boolean)] =
    connectedDomainsMap
      .to(LazyList)
      .mapFilter {
        case (id, sync) if sync.ready =>
          aliasManager.aliasForDomainId(id).map(_ -> ((id, sync.readyForSubmission)))
        case _ => None
      }
      .toMap

  /** Returns the domains this sync service is configured with. */
  def configuredDomains: Seq[StoredDomainConnectionConfig] = domainConnectionConfigStore.getAll()

  /** Returns the pure crypto operations used for the sync protocol */
  def pureCryptoApi: CryptoPureApi = syncCrypto.pureCrypto

  /** Lookup a time tracker for the given `domainId`.
    * A time tracker will only be returned if the domain is registered and connected.
    */
  def lookupDomainTimeTracker(domainId: DomainId): Option[DomainTimeTracker] =
    connectedDomainsMap.get(domainId).map(_.timeTracker)

  /** Adds a new domain to the sync service's configuration.
    *
    * NOTE: Does not automatically connect the sync service to the new domain.
    *
    * @param config The domain configuration.
    * @return Error or unit.
    */
  def addDomain(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Unit] = {
    domainConnectionConfigStore
      .put(config, DomainConnectionConfigStore.Active)
      .leftMap(e => SyncServiceError.SyncServiceAlreadyAdded.Error(e.alias))
  }

  /** Modifies the settings of the sync-service's configuration
    *
    * NOTE: This does not automatically reconnect the sync service.
    */
  def modifyDomain(
      config: DomainConnectionConfig
  )(implicit traceContext: TraceContext): EitherT[Future, SyncServiceError, Unit] =
    domainConnectionConfigStore
      .replace(config)
      .leftMap(e => SyncServiceError.SyncServiceUnknownDomain.Error(e.alias))

  def migrateDomain(
      source: DomainAlias,
      target: DomainConnectionConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def mustBeOffline(alias: DomainAlias, domainId: DomainId) = EitherT.cond[FutureUnlessShutdown](
      !connectedDomainsMap.contains(domainId),
      (),
      SyncServiceError.SyncServiceDomainMustBeOffline.Error(alias): SyncServiceError,
    )

    for {
      targetDomainInfo <- performUnlessClosingEitherU(functionFullName)(
        sequencerInfoLoader
          .loadSequencerEndpoints(target.domain, target.sequencerConnections)(
            traceContext,
            CloseContext(this),
          )
          .leftMap(DomainRegistryError.fromSequencerInfoLoaderError)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.domain,
              DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(err.cause),
            )
          )
      )
      _ <- performUnlessClosingEitherU(functionFullName)(
        aliasManager
          .processHandshake(target.domain, targetDomainInfo.domainId)
          .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.domain,
              err,
            )
          )
      )

      sourceDomainId <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .domainIdForAlias(source)
          .toRight(
            SyncServiceError.SyncServiceUnknownDomain.Error(source): SyncServiceError
          )
      )
      _ <- mustBeOffline(source, sourceDomainId)
      _ <- mustBeOffline(target.domain, targetDomainInfo.domainId)
      _ <-
        connectQueue.executeEUS(
          migrationService
            .migrateDomain(source, target, targetDomainInfo.domainId)
            .leftMap[SyncServiceError](
              SyncServiceError.SyncServiceMigrationError(source, target.domain, _)
            ),
          "migrate domain",
        )
    } yield ()
  }

  /** Removes a configured and disconnected domain.
    *
    * This is an unsafe operation as it changes the ledger offsets.
    */
  def purgeDomain(domain: DomainAlias): Either[SyncServiceError, Unit] =
    throw new UnsupportedOperationException("This unsafe operation has not been implemented yet")

  /** Reconnect to all configured domains that have autoStart = true */
  def reconnectDomains(
      ignoreFailures: Boolean
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[DomainAlias]] =
    connectQueue.executeEUS(
      performReconnectDomains(ignoreFailures),
      "reconnect domains",
    )

  private def performReconnectDomains(ignoreFailures: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[DomainAlias]] = {

    // TODO(i2833): do this in parallel to speed up start-up once this is stable enough
    //  This will need additional synchronization in performDomainConnection
    def go(
        connected: List[DomainAlias],
        open: List[DomainAlias],
    ): EitherT[FutureUnlessShutdown, SyncServiceError, List[DomainAlias]] =
      open match {
        case Nil => EitherT.rightT(connected)
        case con :: rest =>
          for {
            succeeded <- performDomainConnection(con, startSyncDomain = false).transform {
              case Left(SyncServiceFailedDomainConnection(_, parent)) if ignoreFailures =>
                // if the error is retryable, we'll reschedule an automatic retry so this domain gets connected eventually
                if (parent.retryable.nonEmpty) {
                  logger.warn(
                    s"Skipping failing domain $con after ${parent.code
                        .toMsg(parent.cause, traceContext.traceId, limit = None)}. Will schedule subsequent retry."
                  )
                  attemptReconnect
                    .put(
                      con,
                      AttemptReconnect(
                        con,
                        clock.now,
                        parameters.sequencerClient.startupConnectionRetryDelay.unwrap,
                        traceContext,
                      ),
                    )
                    .discard
                  scheduleReconnectAttempt(
                    clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava)
                  )
                } else {
                  logger.warn(
                    s"Skipping failing domain $con after ${parent.code
                        .toMsg(parent.cause, traceContext.traceId, limit = None)}. Will not schedule retry. Please connect it manually."
                  )
                }
                Right(false)
              case Left(err) =>
                // disconnect from pending connections on failure
                val failures = connected.mapFilter(performDomainDisconnect(_).left.toOption)
                if (failures.nonEmpty) {
                  logger.error(s"Failed to disconnect from domains: ${failures}")
                }
                Left(err)
              case Right(_) => Right(true)
            }
            res <- go(if (succeeded) connected :+ con else connected, rest)
          } yield res
      }

    def startDomains(domains: Seq[DomainAlias]): EitherT[Future, SyncServiceError, Unit] = {
      // we need to start all domains concurrently in order to avoid the transfer processing
      // to hang
      val futE = Future.traverse(domains)(domain =>
        (for {
          syncDomain <- EitherT.fromOption[Future](
            syncDomainForAlias(domain),
            SyncServiceError.SyncServiceUnknownDomain.Error(domain),
          )
          _ <- startDomain(domain, syncDomain)
        } yield ()).value.map(v => (domain, v))
      )
      EitherT(futE.map { res =>
        val failed = res.collect { case (_, Left(err)) => err }
        NonEmpty.from(failed) match {
          case None => Right(())
          case Some(lst) =>
            domains.foreach(performDomainDisconnect(_).discard[Either[SyncServiceError, Unit]])
            Left(SyncServiceError.SyncServiceStartupError(lst))
        }
      })
    }

    val connectedDomains =
      connectedDomainsMap.keys.to(LazyList).mapFilter(aliasManager.aliasForDomainId).toSet

    def shouldConnectTo(config: StoredDomainConnectionConfig): Boolean = {
      config.status.isActive && !config.config.manualConnect && !connectedDomains.contains(
        config.config.domain
      )
    }

    for {
      configs <- EitherT.pure[FutureUnlessShutdown, SyncServiceError](
        domainConnectionConfigStore
          .getAll()
          .collect {
            case storedConfig if shouldConnectTo(storedConfig) => storedConfig.config.domain
          }
      )

      _ = logger.info(
        s"Reconnecting to domains ${configs.map(_.unwrap)}. Already connected: $connectedDomains"
      )
      // step connect
      connected <- go(List(), configs.toList)
      _ = if (configs.nonEmpty) {
        if (connected.nonEmpty)
          logger.info("Starting sync-domains for global reconnect of domains")
        else
          logger.info("Not starting any sync-domain as none can be contacted")
      }
      // step subscribe
      _ <- startDomains(connected).mapK(FutureUnlessShutdown.outcomeK)
    } yield {
      if (connected != configs)
        logger.info(
          s"Successfully re-connected to a subset of domains ${connected}, failed to connect to ${configs.toSet -- connected.toSet}"
        )
      else
        logger.info(s"Successfully re-connected to domains ${connected}")
      connected
    }
  }

  private def startDomain(alias: DomainAlias, syncDomain: SyncDomain)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Unit] =
    EitherTUtil
      .fromFuture(
        syncDomain.start(),
        t => SyncServiceError.SyncServiceInternalError.Failure(alias, t),
      )
      .subflatMap[SyncServiceError, Unit](
        _.leftMap(error => SyncServiceError.SyncServiceInternalError.InitError(alias, error))
      )

  /** Connect the sync service to the given domain.
    * This method makes sure there can only be one connection in progress at a time.
    */
  def connectDomain(domainAlias: DomainAlias, keepRetrying: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Boolean] =
    domainConnectionConfigByAlias(domainAlias)
      .mapK(FutureUnlessShutdown.outcomeK)
      .leftMap(_ => SyncServiceError.SyncServiceUnknownDomain.Error(domainAlias))
      .flatMap { _ =>
        val initial = if (keepRetrying) {
          // we're remembering that we have been trying to reconnect here
          attemptReconnect
            .put(
              domainAlias,
              AttemptReconnect(
                domainAlias,
                clock.now,
                parameters.sequencerClient.startupConnectionRetryDelay.unwrap,
                traceContext,
              ),
            )
            .isEmpty
        } else true
        attemptDomainConnection(domainAlias, keepRetrying = keepRetrying, initial = initial)
      }

  private def attemptDomainConnection(
      domainAlias: DomainAlias,
      keepRetrying: Boolean,
      initial: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Boolean] = {
    connectQueue.executeEUS(
      if (keepRetrying && !attemptReconnect.isDefinedAt(domainAlias)) {
        EitherT.rightT[FutureUnlessShutdown, SyncServiceError](false)
      } else {
        performDomainConnection(domainAlias, startSyncDomain = true).transform {
          case Left(SyncServiceError.SyncServiceFailedDomainConnection(_, err))
              if keepRetrying && err.retryable.nonEmpty =>
            if (initial)
              logger.warn(s"Initial connection attempt to ${domainAlias} failed with ${err.code
                  .toMsg(err.cause, traceContext.traceId, limit = None)}. Will keep on trying.")
            else
              logger.info(
                s"Initial connection attempt to ${domainAlias} failed. Will keep on trying."
              )
            scheduleReconnectAttempt(
              clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava)
            )
            Right(false)
          case Right(()) =>
            resolveReconnectAttempts(domainAlias)
            Right(true)
          case Left(x) =>
            resolveReconnectAttempts(domainAlias)
            Left(x)
        }
      },
      s"connect to $domainAlias",
    )
  }

  private def scheduleReconnectAttempt(timestamp: CantonTimestamp): Unit = {
    def mergeLarger(cur: Option[CantonTimestamp], ts: CantonTimestamp): Option[CantonTimestamp] =
      cur match {
        case None => Some(ts)
        case Some(old) => Some(ts.max(old))
      }

    val _ = clock.scheduleAt(
      ts => {
        val (reconnect, nextO) = {
          attemptReconnect.toList.foldLeft(
            (Seq.empty[AttemptReconnect], None: Option[CantonTimestamp])
          ) { case ((reconnect, next), (alias, item)) =>
            // if we can't retry now, remember to retry again
            if (item.earliest > ts)
              (reconnect, mergeLarger(next, item.earliest))
            else {
              // update when we retried
              val nextRetry = item.retryDelay.*(2.0)
              val maxRetry = parameters.sequencerClient.maxConnectionRetryDelay.unwrap
              val nextRetryCapped = if (nextRetry > maxRetry) maxRetry else nextRetry
              attemptReconnect
                .put(alias, item.copy(last = ts, retryDelay = nextRetryCapped))
                .discard
              (reconnect :+ item, mergeLarger(next, ts.plusMillis(nextRetryCapped.toMillis)))
            }
          }
        }
        reconnect.foreach { item =>
          implicit val traceContext: TraceContext = item.trace
          val domainAlias = item.alias
          logger.debug(s"Starting background reconnect attempt for $domainAlias")
          EitherTUtil.doNotAwaitUS(
            attemptDomainConnection(item.alias, keepRetrying = true, initial = false),
            s"Background reconnect to $domainAlias",
          )
        }
        nextO.foreach(scheduleReconnectAttempt)
      },
      timestamp,
    )
  }

  def domainConnectionConfigByAlias(
      domainAlias: DomainAlias
  ): EitherT[Future, MissingConfigForAlias, StoredDomainConnectionConfig] =
    EitherT.fromEither[Future](domainConnectionConfigStore.get(domainAlias))

  /** Connect the sync service to the given domain. */
  private def performDomainConnection(
      domainAlias: DomainAlias,
      startSyncDomain: Boolean,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def createDomainHandle(
        config: DomainConnectionConfig
    ): EitherT[FutureUnlessShutdown, SyncServiceError, DomainHandle] =
      EitherT(domainRegistry.connect(config)).leftMap(err =>
        SyncServiceError.SyncServiceFailedDomainConnection(domainAlias, err)
      )

    def handleCloseDegradation(syncDomain: SyncDomain, fatal: Boolean)(err: CantonError) = {
      if (fatal && parameters.exitOnFatalFailures) {
        FatalError.exitOnFatalError(err, logger)
      } else {
        // If the error is not fatal or the crash on fatal failures flag is off, then we report the unhealthy state and disconnect from the domain
        syncDomain.failureOccurred(err)
        disconnectDomain(domainAlias)
      }
    }

    if (aliasManager.domainIdForAlias(domainAlias).exists(connectedDomainsMap.contains)) {
      logger.debug(s"Already connected to domain: ${domainAlias.unwrap}")
      resolveReconnectAttempts(domainAlias)
      EitherT.rightT(())
    } else {

      logger.debug(s"Connecting to domain: ${domainAlias.unwrap}")
      val domainMetrics = metrics.domainMetrics(domainAlias)

      val ret: EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = for {

        domainConnectionConfig <- domainConnectionConfigByAlias(domainAlias)
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[SyncServiceError] { case MissingConfigForAlias(alias) =>
            SyncServiceError.SyncServiceUnknownDomain.Error(alias)
          }
        // do not connect to a domain that is not active
        _ <- EitherT.cond[FutureUnlessShutdown](
          domainConnectionConfig.status.isActive || skipStatusCheck,
          (),
          SyncServiceError.SyncServiceDomainIsNotActive
            .Error(domainAlias, domainConnectionConfig.status): SyncServiceError,
        )
        domainHandle <- createDomainHandle(domainConnectionConfig.config)

        persistent = domainHandle.domainPersistentState
        domainId = domainHandle.domainId
        domainCrypto = syncCrypto.tryForDomain(domainId, domainHandle.staticParameters)

        ephemeral <- EitherT.right[SyncServiceError](
          FutureUnlessShutdown.outcomeF(
            syncDomainStateFactory
              .createFromPersistent(
                persistent,
                participantNodePersistentState.map(_.multiDomainEventLog),
                inFlightSubmissionTracker,
                (loggerFactory: NamedLoggerFactory) =>
                  DomainTimeTracker(
                    domainConnectionConfig.config.timeTracker,
                    clock,
                    domainHandle.sequencerClient,
                    domainHandle.staticParameters.protocolVersion,
                    timeouts,
                    loggerFactory,
                  ),
                domainMetrics,
                parameters.cachingConfigs.sessionKeyCacheConfig,
                participantId,
              )
          )
        )
        domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

        missingKeysAlerter = new MissingKeysAlerter(
          participantId,
          domainId,
          domainHandle.topologyClient,
          domainCrypto.crypto.cryptoPrivateStore,
          loggerFactory,
        )

        syncDomain = syncDomainFactory.create(
          domainId,
          domainHandle,
          participantId,
          engine,
          cantonAuthorityResolver,
          parameters,
          participantNodePersistentState,
          persistent,
          ephemeral,
          packageService,
          domainCrypto,
          identityPusher,
          domainHandle.topologyFactory
            .createTopologyProcessorFactory(
              partyNotifier,
              missingKeysAlerter,
              domainHandle.topologyClient,
              domainCrypto,
              ephemeral.recordOrderPublisher,
              domainHandle.staticParameters.protocolVersion,
            ),
          missingKeysAlerter,
          transferCoordination,
          inFlightSubmissionTracker,
          clock,
          metrics.pruning,
          domainMetrics,
          futureSupervisor,
          domainLoggerFactory,
          skipRecipientsCheck = skipRecipientsCheck,
          testingConfig,
        )

        // update list of connected domains
        _ = connectedDomainsMap += (domainId -> syncDomain)

        _ = syncDomainHealth.set(syncDomain)
        _ = ephemeralHealth.set(syncDomain.ephemeral)
        _ = sequencerClientHealth.set(syncDomain.sequencerClient.healthComponent)
        _ = acsCommitmentProcessorHealth.set(syncDomain.acsCommitmentProcessor.healthComponent)
        _ = syncDomain.resolveUnhealthy()

        // Start sequencer client subscription only after sync domain has been added to connectedDomainsMap, e.g. to
        // prevent sending PartyAddedToParticipantEvents before the domain is available for command submission. (#2279)
        _ <-
          if (startSyncDomain) {
            logger.info(s"Connected to domain and starting synchronisation: $domainAlias")
            startDomain(domainAlias, syncDomain).mapK(FutureUnlessShutdown.outcomeK)
          } else {
            logger.info(s"Connected to domain: $domainAlias, without starting synchronisation")
            EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
          }
        _ = domainHandle.sequencerClient.completion.onComplete {
          case Success(denied: CloseReason.PermissionDenied) =>
            handleCloseDegradation(syncDomain, fatal = false)(
              SyncServiceDomainDisabledUs.Error(domainAlias, denied.cause)
            )
          case Success(CloseReason.BecamePassive) =>
            handleCloseDegradation(syncDomain, fatal = false)(
              SyncServiceDomainBecamePassive.Error(domainAlias)
            )
          case Success(error: CloseReason.UnrecoverableError) =>
            if (isClosing)
              disconnectDomain(domainAlias)
            else
              handleCloseDegradation(syncDomain, fatal = true)(
                SyncServiceDomainDisconnect.UnrecoverableError(domainAlias, error.cause)
              )
          case Success(error: CloseReason.UnrecoverableException) =>
            handleCloseDegradation(syncDomain, fatal = true)(
              SyncServiceDomainDisconnect.UnrecoverableException(domainAlias, error.throwable)
            )
          case Success(CloseReason.ClientShutdown) =>
            logger.info(s"$domainAlias disconnected because sequencer client was closed")
            disconnectDomain(domainAlias)
          case Failure(exception) =>
            handleCloseDegradation(syncDomain, fatal = true)(
              SyncServiceDomainDisconnect.UnrecoverableException(domainAlias, exception)
            )
        }
      } yield {
        // remove this one from the reconnect attempt list, as we are successfully connected now
        this.resolveReconnectAttempts(domainAlias)
      }

      def disconnectOn(): Unit = {
        // only invoke domain disconnect if we actually got so far that the domain-id has been read from the remote node
        if (aliasManager.domainIdForAlias(domainAlias).nonEmpty)
          performDomainDisconnect(
            domainAlias
          ).discard // Ignore Lefts because we don't know to what extent the connection succeeded.
      }

      def handleOutcome(
          outcome: UnlessShutdown[Either[SyncServiceError, Unit]]
      ): UnlessShutdown[Either[SyncServiceError, Unit]] =
        outcome match {
          case x @ UnlessShutdown.Outcome(Right(())) =>
            connectionListeners.get().foreach(_(domainAlias))
            x
          case UnlessShutdown.AbortedDueToShutdown =>
            disconnectOn()
            UnlessShutdown.AbortedDueToShutdown
          case x @ UnlessShutdown.Outcome(
                Left(SyncServiceError.SyncServiceAlreadyAdded.Error(_))
              ) =>
            x
          case x @ UnlessShutdown.Outcome(Left(_)) =>
            disconnectOn()
            x
        }

      EitherT(
        ret.value.transform(
          handleOutcome,
          err => {
            logger.error(
              s"performing domain connection for ${domainAlias.unwrap} failed with an unhandled error",
              err,
            )
            err
          },
        )
      )
    }
  }

  /** Disconnect the given domain from the sync service. */
  def disconnectDomain(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    resolveReconnectAttempts(domain)
    connectQueue.executeE(
      EitherT.fromEither(performDomainDisconnect(domain)),
      s"disconnect from $domain",
    )
  }

  private def performDomainDisconnect(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Either[SyncServiceError, Unit] = {
    logger.info(show"Disconnecting from $domain")
    (for {
      domainId <- aliasManager.domainIdForAlias(domain)
    } yield {
      connectedDomainsMap.remove(domainId) match {
        case Some(syncDomain) =>
          syncDomain.close()
          logger.info(show"Disconnected from $domain")
        case None =>
          logger.info(show"Nothing to do, as we are not connected to $domain")
      }
    }).toRight(SyncServiceError.SyncServiceUnknownDomain.Error(domain))
  }

  /** Disconnect from all connected domains. */
  def disconnectDomains()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    val connectedDomains =
      connectedDomainsMap.keys.toList.mapFilter(aliasManager.aliasForDomainId).distinct
    connectedDomains.parTraverse_(disconnectDomain)
  }

  /** Checks if a given party has any active contracts. */
  def partyHasActiveContracts(
      partyId: PartyId
  )(implicit traceContext: TraceContext): Future[Boolean] =
    stateInspection.partyHasActiveContracts(partyId)

  /** prepares a domain connection for migration: connect and wait until the topology state has been pushed
    * so we don't deploy against an empty domain
    */
  private def prepareDomainConnectionForMigration(
      aliasT: Traced[DomainAlias]
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = aliasT.withTraceContext {
    implicit tx => alias =>
      logger.debug(s"Preparing connection to $alias for migration")
      (for {
        _ <- performDomainConnection(alias, startSyncDomain = true, skipStatusCheck = true)
        success <- identityPusher
          .awaitIdle(alias, timeouts.unbounded.unwrap)
          .leftMap(reg => SyncServiceError.SyncServiceFailedDomainConnection(alias, reg))
        // now, tick the domain so we can be sure to have a tick that includes the topology changes
        syncService <- EitherT.fromEither[FutureUnlessShutdown](
          syncDomainForAlias(alias).toRight(SyncServiceError.SyncServiceUnknownDomain.Error(alias))
        )
        tick = syncService.topologyClient.approximateTimestamp
        _ = logger.debug(s"Awaiting tick at $tick from $alias for migration")
        _ <- EitherT.right(
          FutureUnlessShutdown.outcomeF(
            syncService.timeTracker.awaitTick(tick).fold(Future.unit)(_.void)
          )
        )
        _ <- repairService
          .awaitCleanHeadForTimestamp(syncService.domainId, tick)
          .leftMap(err =>
            SyncServiceError.SyncServiceInternalError.CleanHeadAwaitFailed(alias, tick, err)
          )
        _ = logger.debug(
          s"Received timestamp from $alias for migration and advanced clean-head to it"
        )
        _ <- EitherT.fromEither[FutureUnlessShutdown](performDomainDisconnect(alias))
      } yield success)
        .leftMap[SyncDomainMigrationError](err =>
          SyncDomainMigrationError.MigrationParentError(alias, err)
        )
        .flatMap { success =>
          EitherT.cond[FutureUnlessShutdown](
            success,
            (),
            SyncDomainMigrationError.InternalError.Generic(
              "Failed to successfully dispatch topology state to target domain"
            ): SyncDomainMigrationError,
          )
        }
  }

  // Canton assumes that as long as the CantonSyncService is up we are "read"-healthy. We could consider lack
  // of storage readability as a way to be read-unhealthy, but as participants share the database backend with
  // the ledger-api-server and indexer, database-non-availability is already flagged upstream.
  override def currentHealth(): HealthStatus = HealthStatus.healthy

  // Write health requires the ability to transact, i.e. connectivity to at least one domain and HA-activeness.
  def currentWriteHealth(): HealthStatus =
    if (existsReadyDomain && isActive()) HealthStatus.healthy else HealthStatus.unhealthy

  def computeTotalLoad: Int = connectedDomainsMap.foldLeft(0) { case (acc, (_, syncDomain)) =>
    acc + syncDomain.numberOfDirtyRequests()
  }

  private val emitWarningOnDetailLoggingAndHighLoad =
    (parameters.general.loggingConfig.eventDetails || parameters.general.loggingConfig.api.logMessagePayloads) && parameters.general.loggingConfig.api.warnBeyondLoad.nonEmpty

  def checkOverloaded(traceContext: TraceContext): Option[state.v2.SubmissionResult] = {
    implicit val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)(traceContext)
    val load = computeTotalLoad
    if (emitWarningOnDetailLoggingAndHighLoad) {
      parameters.general.loggingConfig.api.warnBeyondLoad match {
        case Some(warnBeyondLoad) if load > warnBeyondLoad =>
          logger.warn(
            "Your detailed API event logging is turned on but you are doing quite a few concurrent requests. Please note that detailed event logging comes with a performance penalty."
          )(traceContext)
        case _ =>
      }
    }
    resourceManagementService.checkOverloaded(load)
  }

  def refreshCaches()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- FutureUnlessShutdown.outcomeF(domainConnectionConfigStore.refreshCache())
      _ <- resourceManagementService.refreshCache()
      _ = participantNodePersistentState.value.multiDomainEventLog.setOnPublish(
        inFlightSubmissionTracker.onPublishListener
      )
    } yield ()

  override def onClosed(): Unit = {
    val instances = Seq(
      connectQueue,
      migrationService,
      repairService,
      pruningProcessor,
    ) ++ syncCrypto.ips.allDomains.toSeq ++ connectedDomainsMap.values.toSeq ++ Seq(
      packageService,
      domainRouter,
      domainRegistry,
      inFlightSubmissionTracker,
      domainConnectionConfigStore,
      syncDomainPersistentStateManager,
      participantNodePersistentState.value,
      syncDomainHealth,
      ephemeralHealth,
      sequencerClientHealth,
      acsCommitmentProcessorHealth,
    )

    Lifecycle.close(instances: _*)(logger)
  }

  override def toString: String = s"CantonSyncService($participantId)"

  override def submitReassignment(
      submitter: Party,
      applicationId: Ref.ApplicationId,
      commandId: Ref.CommandId,
      submissionId: Option[SubmissionId],
      workflowId: Option[Ref.WorkflowId],
      reassignmentCommand: ReassignmentCommand,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] = {
    import scala.jdk.FutureConverters.*
    withSpan("CantonSyncService.submitReassignment") { implicit traceContext => span =>
      span.setAttribute("command_id", commandId)
      logger.debug(s"Received submit-reassignment ${commandId} from ledger-api server")

      /* @param domain For transfer-out this should be the source domain, for transfer-in this is the target domain
       * @param remoteDomain For transfer-out this should be the target domain, for transfer-in this is the source domain
       */
      def doTransfer[E <: TransferProcessorError, T](
          domain: DomainId,
          remoteDomain: DomainId,
      )(
          transfer: ProtocolVersion => SyncDomain => EitherT[Future, E, FutureUnlessShutdown[T]]
      )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
        for {
          syncDomain <- EitherT.fromOption[Future](
            readySyncDomainById(domain),
            ifNone = RequestValidationErrors.InvalidArgument
              .Reject(s"Domain ID not found: $domain"): DamlError,
          )
          remoteProtocolVersion <- EitherT.fromOption[Future](
            protocolVersionGetter(Traced(remoteDomain)),
            ifNone = RequestValidationErrors.InvalidArgument
              .Reject(s"Domain ID's protocol version not found: $remoteDomain"): DamlError,
          )
          _ <- transfer(remoteProtocolVersion)(syncDomain)
            .leftMap(error =>
              RequestValidationErrors.InvalidArgument
                .Reject(
                  error.message
                ): DamlError // TODO(i13240): Improve reassignment-submission Ledger API errors
            )
            .mapK(FutureUnlessShutdown.outcomeK)
            .semiflatMap(Predef.identity)
            .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
        } yield SubmissionResult.Acknowledged
      }
        .leftMap(error => SubmissionResult.SynchronousError(error.rpcStatus()))
        .merge

      reassignmentCommand match {
        case unassign: ReassignmentCommand.Unassign =>
          doTransfer(
            domain = unassign.sourceDomain.unwrap,
            remoteDomain = unassign.targetDomain.unwrap,
          )(protocolVersion =>
            _.submitTransferOut(
              submitterMetadata = TransferSubmitterMetadata(
                submitter = submitter,
                applicationId = applicationId,
                submittingParticipant = participantId.toLf,
                commandId = commandId,
                submissionId = submissionId,
                workflowId = workflowId,
              ),
              contractId = unassign.contractId,
              targetDomain = unassign.targetDomain,
              targetProtocolVersion = TargetProtocolVersion(protocolVersion),
            )
          )

        case assign: ReassignmentCommand.Assign =>
          doTransfer(
            domain = assign.targetDomain.unwrap,
            remoteDomain = assign.sourceDomain.unwrap,
          )(protocolVersion =>
            _.submitTransferIn(
              submitterMetadata = TransferSubmitterMetadata(
                submitter = submitter,
                applicationId = applicationId,
                submittingParticipant = participantId.toLf,
                commandId = commandId,
                submissionId = submissionId,
                workflowId = workflowId,
              ),
              transferId = TransferId(assign.sourceDomain, assign.unassignId),
              sourceProtocolVersion = SourceProtocolVersion(protocolVersion),
            )
          )
      }
    }.asJava
  }

  override def getConnectedDomains(
      request: ReadService.ConnectedDomainRequest
  )(implicit traceContext: TraceContext): Future[ReadService.ConnectedDomainResponse] = {
    def getSnapshot(domainAlias: DomainAlias, domainId: DomainId): Future[TopologySnapshot] =
      syncCrypto.ips
        .forDomain(domainId)
        .toFuture(
          new Exception(
            s"Failed retrieving DomainTopologyClient for domain `$domainId` with alias $domainAlias"
          )
        )
        .map(_.currentSnapshotApproximation)

    val result = readyDomains
      // keep only healthy domains
      .collect { case (domainAlias, (domainId, true)) =>
        for {
          topology <- getSnapshot(domainAlias, domainId)
          attributesO <- topology.hostedOn(request.party, participantId = participantId)
        } yield attributesO.map(attributes =>
          ConnectedDomainResponse.ConnectedDomain(
            domainAlias,
            domainId,
            attributes.permission,
          )
        )
      }.toSeq

    Future.sequence(result).map(_.flatten).map(ConnectedDomainResponse.apply)
  }

  def incompleteTransferData(
      validAt: GlobalOffset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[List[IncompleteTransferData]] =
    syncDomainPersistentStateManager.getAll.values.toList
      .parTraverse {
        _.transferStore.findIncomplete(
          sourceDomain = None,
          validAt = validAt,
          stakeholders = NonEmpty.from(stakeholders),
          limit = NonNegativeInt.maxValue,
        )
      }
      .map(_.flatten)

  override def incompleteReassignmentOffsets(
      validAt: LedgerSyncOffset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Vector[LedgerSyncOffset]] =
    UpstreamOffsetConvert
      .toGlobalOffset(validAt)
      .fold(
        error => Future.failed(new IllegalArgumentException(error)),
        incompleteTransferData(_, stakeholders).map(
          _.map(
            _.transferEventGlobalOffset.globalOffset
              .pipe(UpstreamOffsetConvert.fromGlobalOffset)
          ).toVector
        ),
      )
}

object CantonSyncService {
  trait Factory[+T <: CantonSyncService] {
    def create(
        participantId: ParticipantId,
        domainRegistry: DomainRegistry,
        domainConnectionConfigStore: DomainConnectionConfigStore,
        domainAliasManager: DomainAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
        packageService: PackageService,
        topologyManagerOps: ParticipantTopologyManagerOps,
        identityPusher: ParticipantTopologyDispatcherCommon,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        engine: Engine,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        skipRecipientsCheck: Boolean,
        multiDomainLedgerAPIEnabled: Boolean,
        testingConfig: TestingConfigInternal,
    )(implicit ec: ExecutionContext, mat: Materializer, tracer: Tracer): T
  }

  object DefaultFactory extends Factory[CantonSyncService] {
    override def create(
        participantId: ParticipantId,
        domainRegistry: DomainRegistry,
        domainConnectionConfigStore: DomainConnectionConfigStore,
        domainAliasManager: DomainAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
        packageService: PackageService,
        topologyManagerOps: ParticipantTopologyManagerOps,
        identityPusher: ParticipantTopologyDispatcherCommon,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        engine: Engine,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        skipRecipientsCheck: Boolean,
        multiDomainLedgerAPIEnabled: Boolean,
        testingConfig: TestingConfigInternal,
    )(implicit
        ec: ExecutionContext,
        mat: Materializer,
        tracer: Tracer,
    ): CantonSyncService =
      new CantonSyncService(
        participantId,
        domainRegistry,
        domainConnectionConfigStore,
        domainAliasManager,
        participantNodePersistentState,
        participantNodeEphemeralState,
        syncDomainPersistentStateManager,
        packageService,
        topologyManagerOps,
        identityPusher,
        partyNotifier,
        syncCrypto,
        NoOpPruningProcessor,
        engine,
        syncDomainStateFactory,
        clock,
        resourceManagementService,
        cantonParameterConfig,
        SyncDomain.DefaultFactory,
        indexedStringStore,
        storage,
        metrics,
        sequencerInfoLoader,
        () => storage.isActive,
        futureSupervisor,
        loggerFactory,
        skipRecipientsCheck = skipRecipientsCheck,
        multiDomainLedgerAPIEnabled: Boolean,
        testingConfig,
      )
  }
}

trait SyncServiceError extends Serializable with Product with CantonError

object SyncServiceInjectionError extends InjectionErrorGroup {

  import com.daml.lf.data.Ref.{ApplicationId, CommandId}

  @Explanation("This error results if a command is submitted to the passive replica.")
  @Resolution("Send the command to the active replica.")
  object PassiveReplica
      extends ErrorCode(
        id = "NODE_IS_PASSIVE_REPLICA",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error(applicationId: ApplicationId, commandId: CommandId)
        extends TransactionErrorImpl(
          cause = "Cannot process submitted command. This participant is the passive replica."
        )
  }

  @Explanation(
    "This errors results if a command is submitted to a participant that is not connected to any domain."
  )
  @Resolution(
    "Connect your participant to the domain where the given parties are hosted."
  )
  object NotConnectedToAnyDomain
      extends ErrorCode(
        id = "NOT_CONNECTED_TO_ANY_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error()
        extends TransactionErrorImpl(cause = "This participant is not connected to any domain.")
  }

  @Explanation("This errors occurs if an internal error results in an exception.")
  @Resolution("Contact support.")
  object InjectionFailure
      extends ErrorCode(
        id = "COMMAND_INJECTION_FAILURE",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(throwable: Throwable)
        extends TransactionErrorImpl(
          cause = "Command failed with an exception",
          throwableO = Some(throwable),
        )
  }

}

object SyncServiceError extends SyncServiceErrorGroup {

  @Explanation(
    "This error results if a domain connectivity command is referring to a domain alias that has not been registered."
  )
  @Resolution(
    "Please confirm the domain alias is correct, or configure the domain before (re)connecting."
  )
  object SyncServiceUnknownDomain
      extends ErrorCode(
        "SYNC_SERVICE_UNKNOWN_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = s"The domain with alias ${domain.unwrap} is unknown.")
        with SyncServiceError
  }

  @Explanation(
    "This error results on an attempt to register a new domain under an alias already in use."
  )
  object SyncServiceAlreadyAdded
      extends ErrorCode(
        "SYNC_SERVICE_ALREADY_ADDED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = "The domain with the given alias has already been added.")
        with SyncServiceError
  }

  abstract class MigrationErrors extends ErrorGroup()

  abstract class DomainRegistryErrorGroup extends ErrorGroup()

  abstract class TrafficControlErrorGroup extends ErrorGroup()

  final case class SyncServiceFailedDomainConnection(
      domain: DomainAlias,
      parent: DomainRegistryError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[DomainRegistryError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] = Map("domain" -> domain.unwrap)

  }

  final case class SyncServiceMigrationError(
      from: DomainAlias,
      to: DomainAlias,
      parent: SyncDomainMigrationError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[SyncDomainMigrationError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] = Map("from" -> from.unwrap, "to" -> to.unwrap)

  }

  @Explanation(
    "This error is logged when the synchronization service shuts down because the remote domain has disabled this participant."
  )
  @Resolution("Contact the domain operator and inquire why you have been booted out.")
  object SyncServiceDomainDisabledUs
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISABLED_US",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(domain: DomainAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain rejected our subscription attempt with permission denied."
        )
  }

  @Explanation(
    "This error is logged when a sync domain has a non-active status."
  )
  @Resolution(
    """If you attempt to connect to a domain that has either been migrated off or has a pending migration,
      |this error will be emitted. Please complete the migration before attempting to connect to it."""
  )
  object SyncServiceDomainIsNotActive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_STATUS_NOT_ACTIVE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(domain: DomainAlias, status: DomainConnectionConfigStore.Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"$domain has status $status and can therefore not be connected to."
        )
        with SyncServiceError
  }

  @Explanation(
    "This error is logged when a sync domain is disconnected because the participant became passive."
  )
  @Resolution("Fail over to the active participant replica.")
  object SyncServiceDomainBecamePassive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_BECAME_PASSIVE",
        ErrorCategory.TransientServerFailure,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain disconnected because participant became passive."
        )
  }

  @Explanation(
    "This error is emitted when an operation is attempted such as repair that requires the domain connection to be disconnected and clean."
  )
  @Resolution("Disconnect the domain before attempting the command.")
  object SyncServiceDomainMustBeOffline
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_MUST_BE_OFFLINE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = show"$domain must be disconnected for the given operation")
        with SyncServiceError

  }

  @Explanation(
    "This error is logged when a sync domain is unexpectedly disconnected from the Canton " +
      "sync service (after having previously been connected)"
  )
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceDomainDisconnect
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISCONNECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class UnrecoverableError(domain: DomainAlias, _reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = show"$domain fatally disconnected because of ${_reason}")

    final case class UnrecoverableException(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"Domain $domain fatally disconnected because of an exception ${throwable.getMessage}",
          throwableO = Some(throwable),
        )

  }

  @Explanation("This error indicates an internal issue.")
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceInternalError
      extends ErrorCode(
        "SYNC_SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class UnknownDomainParameters(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The domain parameters for the given domain are missing in the store"
        )
        with SyncServiceError

    final case class Failure(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The domain failed to startup due to an internal error",
          throwableO = Some(throwable),
        )
        with SyncServiceError

    final case class InitError(domain: DomainAlias, error: SyncDomainInitializationError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "The domain failed to initialize due to an internal error")
        with SyncServiceError

    final case class DomainIsMissingInternally(domain: DomainAlias, where: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to await for participant becoming active due to missing domain objects"
        )
        with SyncServiceError
    final case class CleanHeadAwaitFailed(domain: DomainAlias, ts: CantonTimestamp, err: String)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Failed to await for clean-head at ${ts}: $err"
        )
        with SyncServiceError
  }

  @Explanation("The participant has detected that another node is behaving maliciously.")
  @Resolution("Contact support.")
  object SyncServiceAlarm extends AlarmErrorCode("SYNC_SERVICE_ALARM") {
    final case class Warn(override val cause: String) extends Alarm(cause)
  }

  final case class SyncServiceStartupError(override val errors: NonEmpty[Seq[SyncServiceError]])(
      implicit val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with CombinedError[SyncServiceError]

  @Explanation(
    """The participant is not connected to a domain and can therefore not allocate a party
    because the party notification is configured as ``party-notification.type = via-domain``."""
  )
  @Resolution(
    "Connect the participant to a domain first or change the participant's party notification config to ``eager``."
  )
  object PartyAllocationNoDomainError
      extends ErrorCode(
        "PARTY_ALLOCATION_WITHOUT_CONNECTED_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(submission_id: LedgerSubmissionId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Cannot allocate a party without being connected to a domain"
        )
  }
}
