// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApiParticipantProvider}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Offset,
  ProcessedDisclosedContract,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.*
import com.digitalasset.canton.health.MutableHealthComponent
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.ledger.participant.state.SyncService.ConnectedSynchronizerResponse
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.data.UploadDarData
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError
import com.digitalasset.canton.participant.admin.inspection.{
  JournalGarbageCollectorControl,
  SyncStateInspection,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.admin.repair.RepairService.SynchronizerLookup
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionFailure,
  TransactionSubmissionUnknown,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentCoordination
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.routing.{
  RoutingSynchronizerState,
  TransactionRoutingProcessor,
}
import com.digitalasset.canton.participant.pruning.{AcsCommitmentProcessor, PruningProcessor}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.MissingConfigForAlias
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectSynchronizer
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceBecamePassive,
  SyncServiceFailedSynchronizerConnection,
  SyncServicePurgeSynchronizerError,
  SyncServiceSynchronizerDisabledUs,
  SyncServiceSynchronizerDisconnect,
  SyncServiceUnknownSynchronizer,
}
import com.digitalasset.canton.participant.synchronizer.*
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.Schedulers
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, SubmissionId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.google.protobuf.ByteString
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Right, Success, Try}

/** The Canton-based synchronization service.
  *
  * A single Canton sync service can connect to multiple synchronizers.
  *
  * @param participantId
  *   The participant node id hosting this sync service.
  * @param synchronizerRegistry
  *   registry for connecting to synchronizers.
  * @param synchronizerConnectionConfigStore
  *   Storage for synchronizer connection configs
  * @param packageService
  *   Underlying package management service.
  * @param syncCrypto
  *   Synchronisation crypto utility combining IPS and Crypto operations.
  * @param isActive
  *   Returns true of the node is the active replica
  */
class CantonSyncService(
    val participantId: ParticipantId,
    private[participant] val synchronizerRegistry: SynchronizerRegistry,
    private[canton] val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    private[canton] val aliasManager: SynchronizerAliasManager,
    private[canton] val participantNodePersistentState: Eval[ParticipantNodePersistentState],
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    private[canton] val syncPersistentStateManager: SyncPersistentStateManager,
    private[canton] val packageService: Eval[PackageService],
    partyOps: PartyOps,
    identityPusher: ParticipantTopologyDispatcher,
    partyNotifier: LedgerServerPartyNotifier,
    val syncCrypto: SyncCryptoApiParticipantProvider,
    val pruningProcessor: PruningProcessor,
    engine: Engine,
    private[canton] val commandProgressTracker: CommandProgressTracker,
    syncEphemeralStateFactory: SyncEphemeralStateFactory,
    clock: Clock,
    resourceManagementService: ResourceManagementService,
    parameters: ParticipantNodeParameters,
    connectedSynchronizerFactory: ConnectedSynchronizer.Factory[ConnectedSynchronizer],
    storage: Storage,
    metrics: ParticipantMetrics,
    sequencerInfoLoader: SequencerInfoLoader,
    val isActive: () => Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    testingConfig: TestingConfigInternal,
    val ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
)(implicit ec: ExecutionContextExecutor, mat: Materializer, val tracer: Tracer)
    extends state.SyncService
    with ParticipantPruningSyncService
    with FlagCloseable
    with Spanning
    with NamedLogging
    with HasCloseContext
    with InternalStateServiceProviderImpl {

  import ShowUtil.*

  val connectedSynchronizerHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, ConnectedSynchronizer.healthName, timeouts)
  val ephemeralHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SyncEphemeralState.healthName, timeouts)
  val sequencerClientHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)
  val acsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, AcsCommitmentProcessor.healthName, timeouts)

  val maxDeduplicationDuration: NonNegativeFiniteDuration =
    participantNodePersistentState.value.settingsStore.settings.maxDeduplicationDuration
      .getOrElse(throw new RuntimeException("Max deduplication duration is not available"))

  private type ConnectionListener = Traced[SynchronizerId] => Unit

  // Listeners to synchronizer connections
  private val connectionListeners = new AtomicReference[List[ConnectionListener]](List.empty)

  def subscribeToConnections(subscriber: ConnectionListener): Unit =
    connectionListeners.updateAndGet(subscriber :: _).discard

  protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** The synchronizers this sync service is connected to. Can change due to connect/disconnect
    * operations. This may contain synchronizers for which recovery is still running. Invariant: All
    * synchronizer ids in this map have a corresponding synchronizer alias in the alias manager DO
    * NOT PASS THIS MUTABLE MAP TO OTHER CLASSES THAT ONLY REQUIRE READ ACCESS. USE
    * [[connectedSynchronizersLookup]] INSTEAD
    */
  private val connectedSynchronizersMap: TrieMap[SynchronizerId, ConnectedSynchronizer] =
    TrieMap.empty[SynchronizerId, ConnectedSynchronizer]
  private val connectedSynchronizersLookup: ConnectedSynchronizersLookup =
    ConnectedSynchronizersLookup.create(connectedSynchronizersMap)
  connectedSynchronizersLookupContainer.registerDelegate(connectedSynchronizersLookup)

  private val partyAllocation = new PartyAllocation(
    participantId,
    partyOps,
    partyNotifier,
    isActive,
    connectedSynchronizersLookup,
    timeouts,
    loggerFactory,
  )

  /** Validates that the provided packages are vetted on the currently connected synchronizers. */
  // TODO(i21341) remove this waiting logic once topology events are published on the ledger api
  val synchronizeVettingOnConnectedSynchronizers: PackageVettingSynchronization =
    new PackageVettingSynchronization {
      override def sync(packages: Set[PackageId])(implicit
          traceContext: TraceContext
      ): EitherT[Future, ParticipantTopologyManagerError, Unit] =
        // wait for packages to be vetted on the currently connected synchronizers
        EitherT
          .right[ParticipantTopologyManagerError](
            connectedSynchronizersLookup.snapshot.toSeq.parTraverse {
              case (synchronizerId, connectedSynchronizer) =>
                connectedSynchronizer.topologyClient
                  .await(
                    _.determinePackagesWithNoVettingEntry(participantId, packages)
                      .map(_.isEmpty)
                      .onShutdown(false),
                    timeouts.network.duration,
                  )
                  // turn AbortedDuToShutdown into a verdict, as we don't want to turn
                  // the overall result into AbortedDueToShutdown, just because one of
                  // the synchronizers disconnected in the meantime.
                  .onShutdown(false)
                  .map(synchronizerId -> _)
            }
          )
          .map { result =>
            result.foreach { case (synchronizerId, successful) =>
              if (!successful)
                logger.info(
                  s"Waiting for vetting of packages $packages on synchronizer $synchronizerId either timed out or the synchronizer got disconnected."
                )
            }
            result
          }
          .void
    }

  private case class AttemptReconnect(
      alias: SynchronizerAlias,
      last: CantonTimestamp,
      retryDelay: Duration,
      trace: TraceContext,
  ) {
    val earliest: CantonTimestamp = last.plusMillis(retryDelay.toMillis)
  }

  // Track synchronizers we would like to "keep on reconnecting until available"
  private val attemptReconnect: TrieMap[SynchronizerAlias, AttemptReconnect] = TrieMap.empty

  private def resolveReconnectAttempts(alias: SynchronizerAlias): Unit =
    attemptReconnect.remove(alias).discard

  // A connected synchronizer is ready if recovery has succeeded
  private[canton] def readyConnectedSynchronizerById(
      synchronizerId: SynchronizerId
  ): Option[ConnectedSynchronizer] =
    connectedSynchronizersMap.get(synchronizerId).filter(_.ready)

  private def existsReadySynchronizer: Boolean = connectedSynchronizersMap.exists {
    case (_, sync) =>
      sync.ready
  }

  private[canton] def connectedSynchronizerForAlias(
      alias: SynchronizerAlias
  ): Option[ConnectedSynchronizer] =
    aliasManager.synchronizerIdForAlias(alias).flatMap(connectedSynchronizersMap.get)

  private val transactionRoutingProcessor = TransactionRoutingProcessor(
    connectedSynchronizersLookup = connectedSynchronizersLookup,
    cryptoPureApi = syncCrypto.pureCrypto,
    synchronizerConnectionConfigStore = synchronizerConnectionConfigStore,
    synchronizerAliasManager = aliasManager,
    participantId = participantId,
    parameters = parameters,
    loggerFactory = loggerFactory,
  )(ec)

  private val reassignmentCoordination: ReassignmentCoordination =
    ReassignmentCoordination(
      parameters.reassignmentTimeProofFreshnessProportion,
      syncPersistentStateManager,
      connectedSynchronizersLookup.get,
      syncCrypto,
      loggerFactory,
    )(ec)

  val protocolVersionGetter: Traced[SynchronizerId] => Option[ProtocolVersion] =
    (tracedSynchronizerId: Traced[SynchronizerId]) =>
      syncPersistentStateManager.protocolVersionFor(tracedSynchronizerId.value)

  override def getProtocolVersionForSynchronizer(
      synchronizerId: Traced[SynchronizerId]
  ): Option[ProtocolVersion] =
    protocolVersionGetter(synchronizerId)

  if (isActive()) {
    TraceContext.withNewTraceContext { implicit traceContext =>
      initializeState()
    }
  }

  private val repairServiceDAMLe =
    new DAMLe(
      pkgId => traceContext => packageService.value.getPackage(pkgId)(traceContext),
      engine,
      parameters.engine.validationPhaseLogging,
      loggerFactory,
    )

  private val connectQueue = {
    val queueName = "sync-service-connect-and-repair-queue"

    new SimpleExecutionQueue(
      queueName,
      futureSupervisor,
      timeouts,
      loggerFactory,
      crashOnFailure = parameters.exitOnFatalFailures,
    )
  }

  val repairService: RepairService = new RepairService(
    participantId,
    syncCrypto,
    packageService.value.packageDependencyResolver,
    repairServiceDAMLe,
    participantNodePersistentState.map(_.contractStore),
    ledgerApiIndexer.asEval(TraceContext.empty),
    aliasManager,
    parameters,
    Storage.threadsAvailableForWriting(storage),
    new SynchronizerLookup {
      override def isConnected(synchronizerId: SynchronizerId): Boolean =
        connectedSynchronizersLookup.isConnected(synchronizerId)

      override def isConnectedToAnySynchronizer: Boolean =
        connectedSynchronizersMap.nonEmpty

      override def persistentStateFor(
          synchronizerId: SynchronizerId
      ): Option[SyncPersistentState] =
        syncPersistentStateManager.get(synchronizerId)

      override def topologyFactoryFor(
          synchronizerId: SynchronizerId
      )(implicit traceContext: TraceContext): Option[TopologyComponentFactory] =
        protocolVersionGetter(Traced(synchronizerId)) match {
          case Some(protocolVersion) =>
            syncPersistentStateManager.topologyFactoryFor(synchronizerId, protocolVersion)

          case None =>
            logger.warn(s"Unable to get protocol version for synchronizer $synchronizerId")
            None
        }

    },
    // Share the sync service queue with the repair service, so that repair operations cannot run concurrently with
    // synchronizer connections.
    connectQueue,
    loggerFactory,
  )

  val partyReplicatorO: Option[PartyReplicator] =
    parameters.unsafeOnlinePartyReplication.map(_ =>
      new PartyReplicator(participantId, this, parameters.processingTimeouts, loggerFactory)
    )

  private val migrationService =
    new SynchronizerMigration(
      aliasManager,
      synchronizerConnectionConfigStore,
      stateInspection,
      repairService,
      prepareSynchronizerConnectionForMigration,
      sequencerInfoLoader,
      parameters.batchingConfig,
      parameters.processingTimeouts,
      loggerFactory,
    )

  val dynamicSynchronizerParameterGetter =
    new CantonDynamicSynchronizerParameterGetter(
      syncCrypto,
      syncPersistentStateManager.protocolVersionFor,
      aliasManager,
      loggerFactory,
    )

  private def trackSubmission(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
  ): Unit =
    commandProgressTracker
      .findHandle(
        submitterInfo.commandId,
        submitterInfo.applicationId,
        submitterInfo.actAs,
        submitterInfo.submissionId,
      )
      .recordTransactionImpact(transaction)

  // Submit a transaction (write service implementation)
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      optSynchronizerId: Option[SynchronizerId],
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
      trackSubmission(submitterInfo, transaction)
      submitTransactionF(
        submitterInfo,
        optSynchronizerId,
        transactionMeta,
        transaction,
        keyResolver,
        disclosedContracts,
      )
    }.map(result =>
      result.map { _ =>
        // It's OK to throw away the asynchronous result because its errors were already logged in `submitTransactionF`.
        // We merely retain it until here so that the span ends only after the asynchronous computation
        SubmissionResult.Acknowledged
      }.merge
    ).asJava
  }

  lazy val stateInspection = new SyncStateInspection(
    syncPersistentStateManager,
    participantNodePersistentState,
    parameters.processingTimeouts,
    new JournalGarbageCollectorControl {
      override def disable(
          synchronizerId: SynchronizerId
      )(implicit traceContext: TraceContext): Future[Unit] =
        connectedSynchronizersMap
          .get(synchronizerId)
          .map(_.addJournalGarageCollectionLock())
          .getOrElse(Future.unit)
      override def enable(
          synchronizerId: SynchronizerId
      )(implicit traceContext: TraceContext): Unit =
        connectedSynchronizersMap
          .get(synchronizerId)
          .foreach(_.removeJournalGarageCollectionLock())
    },
    connectedSynchronizersLookup,
    syncCrypto,
    participantId,
    futureSupervisor,
    loggerFactory,
  )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: LedgerSubmissionId,
      _pruneAllDivulgedContracts: Boolean, // Canton always prunes divulged contracts ignoring this flag
  ): CompletionStage[PruningResult] =
    withNewTrace("CantonSyncService.prune") { implicit traceContext => span =>
      span.setAttribute("submission_id", submissionId)
      pruneInternally(pruneUpToInclusive)
        .fold(
          err => PruningResult.NotPruned(err.asGrpcStatus),
          _ => PruningResult.ParticipantPruned,
        )
        .onShutdown(
          PruningResult.NotPruned(GrpcErrors.AbortedDueToShutdown.Error().asGrpcStatus)
        )
    }.asJava

  def pruneInternally(
      pruneUpToInclusive: Offset
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    (for {
      _pruned <- pruningProcessor.pruneLedgerEvents(pruneUpToInclusive)
    } yield ()).transform(pruningErrorToCantonError)

  private def pruningErrorToCantonError(pruningResult: Either[LedgerPruningError, Unit])(implicit
      traceContext: TraceContext
  ): Either[PruningServiceError, Unit] = pruningResult match {
    case Left(err @ LedgerPruningNothingToPrune) =>
      logger.info(
        s"Could not locate pruning point: ${err.message}. Considering success for idempotency"
      )
      Either.unit
    case Left(err: LedgerPruningOffsetUnsafeToPrune) =>
      logger.info(s"Unsafe to prune: ${err.message}")
      Left(
        PruningServiceError.UnsafeToPrune.Error(
          err.cause,
          err.message,
          err.lastSafeOffset.fold("")(_.toDecimalString),
        )
      )
    case Left(err: LedgerPruningOffsetUnsafeSynchronizer) =>
      logger.info(s"Unsafe to prune ${err.synchronizerId}: ${err.message}")
      Left(
        PruningServiceError.UnsafeToPrune.Error(
          s"no suitable offset for synchronizer ${err.synchronizerId}",
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
      optSynchronizerId: Option[SynchronizerId],
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
    } else if (!existsReadySynchronizer) {
      Future.successful(
        processSubmissionError(SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error())
      )
    } else {
      val submittedFF = transactionRoutingProcessor.submitTransaction(
        submitterInfo,
        optSynchronizerId,
        transactionMeta,
        keyResolver,
        transaction,
        explicitlyDisclosedContracts,
      )
      submittedFF.value.unwrap.transform { result =>
        val loggedResult = result match {
          case Success(UnlessShutdown.Outcome(Right(sequencedF))) =>
            // Reply with ACK as soon as the submission has been registered as in-flight,
            // and asynchronously send it to the sequencer.
            logger.debug(s"Command ${submitterInfo.commandId} is now in-flight.")
            val loggedF = sequencedF.transformIntoSuccess { result =>
              result match {
                case Success(UnlessShutdown.Outcome(submissionResult)) =>
                  submissionResult match {
                    case TransactionSubmitted =>
                      logger.debug(
                        s"Successfully submitted transaction ${submitterInfo.commandId}."
                      )
                    case TransactionSubmissionFailure =>
                      logger.info(
                        s"Failed to submit transaction ${submitterInfo.commandId}"
                      )
                    case TransactionSubmissionUnknown(maxSequencingTime) =>
                      logger.info(
                        s"Unknown state of transaction submission ${submitterInfo.commandId}. Please wait until the max sequencing time $maxSequencingTime has elapsed."
                      )
                  }
                case Success(UnlessShutdown.AbortedDueToShutdown) =>
                  logger.debug(
                    s"Transaction submission aborted due to shutdown ${submitterInfo.commandId}."
                  )
                case Failure(ex) =>
                  logger.error(s"Command submission for ${submitterInfo.commandId} failed", ex)
              }
              UnlessShutdown.unit
            }
            Right(loggedF)
          case Success(UnlessShutdown.Outcome(Left(submissionError))) =>
            processSubmissionError(submissionError)
          case Failure(PassiveInstanceException(_)) |
              Success(UnlessShutdown.AbortedDueToShutdown) =>
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

  override def allocateParty(
      hint: LfPartyId,
      rawSubmissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    partyAllocation.allocate(hint, rawSubmissionId)

  override def uploadDar(dars: Seq[ByteString], submissionId: Ref.SubmissionId)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    withSpan("CantonSyncService.uploadPackages") { implicit traceContext => span =>
      if (!isActive()) {
        logger.debug(s"Rejecting package upload on passive replica.")
        Future.successful(SyncServiceError.Synchronous.PassiveNode)
      } else {
        span.setAttribute("submission_id", submissionId)
        packageService.value
          .upload(
            dars = dars.map(UploadDarData(_, Some("uploaded-via-ledger-api"), None)),
            submissionIdO = Some(submissionId),
            vetAllPackages = true,
            synchronizeVetting = synchronizeVettingOnConnectedSynchronizers,
          )
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
          .valueOr(err => SubmissionResult.SynchronousError(err.asGrpcStatus))
      }
    }

  override def validateDar(dar: ByteString, darName: String)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    withSpan("CantonSyncService.validateDar") { implicit traceContext => _ =>
      if (!isActive()) {
        logger.debug(s"Rejecting DAR validation request on passive replica.")
        Future.successful(SyncServiceError.Synchronous.PassiveNode)
      } else {
        packageService.value
          .validateDar(dar, darName)
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
          .valueOr(err => SubmissionResult.SynchronousError(err.asGrpcStatus))
      }
    }

  override def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    packageService.value
      .getLfArchive(packageId)
      .failOnShutdownTo(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError)

  override def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    packageService.value
      .listPackages()
      .failOnShutdownTo(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError)

  override def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata = packageService.value.packageMetadataView.getSnapshot

  /** Executes ordered sequence of steps to recover any state that might have been lost if the
    * participant previously crashed. Needs to be invoked after the input stores have been created,
    * but before they are made available to dependent components.
    */
  private def recoverParticipantNodeState()(implicit traceContext: TraceContext): Unit = {
    // also resume pending party notifications
    val resumePendingF = partyNotifier.resumePending()

    parameters.processingTimeouts.unbounded
      .awaitUS(
        "Wait for party-notifier recovery to finish"
      )(resumePendingF)
      .discard
  }

  def initializeState()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Invoke crash recovery or initialize active participant")

    // Important to invoke recovery before we do anything else with persisted stores.
    recoverParticipantNodeState()
  }

  /** Returns the ready synchronizers this sync service is connected to. */
  def readySynchronizers: Map[SynchronizerAlias, (SynchronizerId, SubmissionReady)] =
    connectedSynchronizersMap
      .to(LazyList)
      .mapFilter {
        case (id, sync) if sync.ready =>
          aliasManager.aliasForSynchronizerId(id).map(_ -> ((id, sync.readyForSubmission)))
        case _ => None
      }
      .toMap

  /** Returns the synchronizers this sync service is configured with. */
  def registeredSynchronizers: Seq[StoredSynchronizerConnectionConfig] =
    synchronizerConnectionConfigStore.getAll()

  /** Returns the pure crypto operations used for the sync protocol */
  def pureCryptoApi: CryptoPureApi = syncCrypto.pureCrypto

  /** Lookup a time tracker for the given `synchronizerId`. A time tracker will only be returned if
    * the synchronizer is registered and connected.
    */
  def lookupSynchronizerTimeTracker(
      synchronizerId: SynchronizerId
  ): Option[SynchronizerTimeTracker] =
    connectedSynchronizersMap.get(synchronizerId).map(_.timeTracker)

  def lookupTopologyClient(
      synchronizerId: SynchronizerId
  ): Option[SynchronizerTopologyClientWithInit] =
    connectedSynchronizersMap.get(synchronizerId).map(_.topologyClient)

  /** Adds a new synchronizer to the sync service's configuration.
    *
    * NOTE: Does not automatically connect the sync service to the new synchronizer.
    *
    * @param config
    *   The synchronizer configuration.
    * @return
    *   Error or unit.
    */
  def addSynchronizer(
      config: SynchronizerConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      _ <- validateSequencerConnection(config, sequencerConnectionValidation)
      _ <- synchronizerConnectionConfigStore
        .put(config, SynchronizerConnectionConfigStore.Active)
        .leftMap(e => SyncServiceError.SyncServiceAlreadyAdded.Error(e.alias): SyncServiceError)
    } yield ()

  private def validateSequencerConnection(
      config: SynchronizerConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    sequencerInfoLoader
      .validateSequencerConnection(
        config.synchronizerAlias,
        config.synchronizerId,
        config.sequencerConnections,
        sequencerConnectionValidation,
      )
      .leftMap(SyncServiceError.SyncServiceInconsistentConnectivity.Error(_): SyncServiceError)

  /** Modifies the settings of the synchronizer connection
    *
    * NOTE: This does not automatically reconnect to the synchronizer.
    */
  def modifySynchronizer(
      config: SynchronizerConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      _ <- validateSequencerConnection(config, sequencerConnectionValidation)
      _ <- synchronizerConnectionConfigStore
        .replace(config)
        .leftMap(e =>
          SyncServiceError.SyncServiceUnknownSynchronizer.Error(e.alias): SyncServiceError
        )
    } yield ()

  /** Migrates contracts from a source synchronizer to target synchronizer by re-associating them in
    * the participant's persistent store. Prune some of the synchronizer stores after the migration.
    *
    * The migration only starts when certain preconditions are fulfilled:
    *   - the participant is disconnected from the source and target synchronizer
    *   - there are neither in-flight submissions nor dirty requests
    *
    * You can force the migration in case of in-flight transactions but it may lead to a ledger
    * fork. Consider:
    *   - Transaction involving participants P1 and P2 that create a contract c
    *   - P1 migrates (D1 -> D2) when processing is done, P2 when it is in-flight
    *   - Final state:
    *     - P1 has the contract on D2 (it was created and migrated)
    *     - P2 does have the contract because it will not process the mediator verdict
    *
    * Instead of forcing a migration when there are in-flight transactions reconnect all
    * participants to the source synchronizer, halt activity and let the in-flight transactions
    * complete or time out.
    *
    * Using the force flag should be a last resort, that is for disaster recovery when the source
    * synchronizer is unrecoverable.
    */
  def migrateSynchronizer(
      source: Source[SynchronizerAlias],
      target: Target[SynchronizerConnectionConfig],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def allSynchronizersMustBeOffline(): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
      connectedSynchronizersMap.toSeq.map(_._2.synchronizerHandle.synchronizerAlias) match {
        case Nil =>
          EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())

        case aliases =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            SyncServiceError.SyncServiceSynchronizersMustBeOffline.Error(aliases)
          )
      }
    for {
      _ <- allSynchronizersMustBeOffline()

      targetSynchronizerInfo <- migrationService.isSynchronizerMigrationPossible(
        source,
        target,
        force = force,
      )

      _ <-
        connectQueue.executeEUS(
          migrationService
            .migrateSynchronizer(source, target, targetSynchronizerInfo.map(_.synchronizerId))
            .leftMap[SyncServiceError](
              SyncServiceError.SyncServiceMigrationError(source, target.map(_.synchronizerAlias), _)
            ),
          "migrate synchronizer",
        )

      _ <- purgeDeactivatedSynchronizer(source.unwrap)
    } yield ()
  }

  /* Verify that specified synchronizer has inactive status and prune synchronizer stores.
   */
  def purgeDeactivatedSynchronizer(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .synchronizerIdForAlias(synchronizerAlias)
          .toRight(SyncServiceError.SyncServiceUnknownSynchronizer.Error(synchronizerAlias))
      )
      _ = logger.info(
        s"Purging deactivated synchronizer with alias $synchronizerAlias with synchronizer id $synchronizerId"
      )
      _ <-
        pruningProcessor
          .purgeInactiveSynchronizer(synchronizerId)
          .transform(
            pruningErrorToCantonError(_).leftMap(
              SyncServicePurgeSynchronizerError(synchronizerAlias, _): SyncServiceError
            )
          )
    } yield ()

  /** Reconnect configured synchronizers
    *
    * @param ignoreFailures
    *   If true, a failure will not interrupt reconnects
    * @param isTriggeredManually
    *   True if the call of this method is triggered by an explicit call to the connectivity
    *   service, false if the call of this method is triggered by a node restart or transition to
    *   active
    *
    * @param mustBeActive
    *   If true, only executes if the instance is active
    * @return
    *   The list of connected synchronizers
    */
  def reconnectSynchronizers(
      ignoreFailures: Boolean,
      isTriggeredManually: Boolean,
      mustBeActive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[SynchronizerAlias]] =
    if (isActive() || !mustBeActive)
      connectQueue.executeEUS(
        performReconnectSynchronizers(
          ignoreFailures = ignoreFailures,
          isTriggeredManually = isTriggeredManually,
        ),
        "reconnect synchronizers",
      )
    else {
      logger.info("Not reconnecting to synchronizers as instance is passive")
      EitherT.leftT(SyncServiceError.SyncServicePassiveReplica.Error())
    }

  private def performReconnectSynchronizers(ignoreFailures: Boolean, isTriggeredManually: Boolean)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[SynchronizerAlias]] = {

    // TODO(i2833): do this in parallel to speed up start-up once this is stable enough
    //  This will need additional synchronization in performSynchronizerConnection
    def go(
        connected: List[SynchronizerAlias],
        open: List[SynchronizerAlias],
    ): EitherT[FutureUnlessShutdown, SyncServiceError, List[SynchronizerAlias]] =
      open match {
        case Nil => EitherT.rightT(connected)
        case con :: rest =>
          for {
            succeeded <- performSynchronizerConnectionOrHandshake(
              con,
              connectSynchronizer = ConnectSynchronizer.ReconnectSynchronizers,
            ).transform {
              case Left(SyncServiceFailedSynchronizerConnection(_, parent)) if ignoreFailures =>
                // if the error is retryable, we'll reschedule an automatic retry so this synchronizer gets connected eventually
                if (parent.retryable.nonEmpty) {
                  logger.warn(
                    s"Skipping failing synchronizer $con after ${parent.code
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
                    clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava),
                    ConnectSynchronizer.Connect,
                  )
                } else {
                  logger.warn(
                    s"Skipping failing synchronizer $con after ${parent.code
                        .toMsg(parent.cause, traceContext.traceId, limit = None)}. Will not schedule retry. Please connect it manually."
                  )
                }
                Right(false)
              case Left(err) =>
                // disconnect from pending connections on failure
                val failures = connected.mapFilter(performSynchronizerDisconnect(_).left.toOption)
                if (failures.nonEmpty) {
                  logger.error(s"Failed to disconnect from synchronizers: $failures")
                }
                Left(err)
              case Right(_) => Right(true)
            }
            res <- go(if (succeeded) connected :+ con else connected, rest)
          } yield res
      }

    def startConnectedSynchronizers(
        synchronizers: Seq[SynchronizerAlias]
    ): EitherT[Future, SyncServiceError, Unit] = {
      // we need to start all synchronizers concurrently in order to avoid the reassignment processing
      // to hang
      val futE = Future.traverse(synchronizers)(synchronizer =>
        (for {
          connectedSynchronizer <- EitherT.fromOption[Future](
            connectedSynchronizerForAlias(synchronizer),
            SyncServiceError.SyncServiceUnknownSynchronizer.Error(synchronizer),
          )
          _ <- startConnectedSynchronizer(synchronizer, connectedSynchronizer)
        } yield ()).value.map(v => (synchronizer, v))
      )
      EitherT(futE.map { res =>
        val failed = res.collect { case (_, Left(err)) => err }
        NonEmpty.from(failed) match {
          case None => Right(())
          case Some(lst) =>
            synchronizers.foreach(
              performSynchronizerDisconnect(_).discard[Either[SyncServiceError, Unit]]
            )
            Left(SyncServiceError.SyncServiceStartupError.CombinedStartError(lst))
        }
      })
    }

    val connectedSynchronizers =
      connectedSynchronizersMap.keys
        .to(LazyList)
        .mapFilter(aliasManager.aliasForSynchronizerId)
        .toSet

    def shouldConnectTo(config: StoredSynchronizerConnectionConfig): Boolean = {
      val alreadyConnected = connectedSynchronizers.contains(
        config.config.synchronizerAlias
      )

      val manualConnectRequired = config.config.manualConnect

      config.status.isActive && (!manualConnectRequired || isTriggeredManually) && !alreadyConnected
    }

    for {
      configs <- EitherT.pure[FutureUnlessShutdown, SyncServiceError](
        synchronizerConnectionConfigStore
          .getAll()
          .collect {
            case storedConfig if shouldConnectTo(storedConfig) =>
              storedConfig.config.synchronizerAlias
          }
      )

      _ = logger.info(
        s"Reconnecting to synchronizers ${configs.map(_.unwrap)}. Already connected: $connectedSynchronizers"
      )
      // step connect
      connected <- go(List(), configs.toList)
      _ = if (configs.nonEmpty) {
        if (connected.nonEmpty)
          logger.info("Starting connected-synchronizer for global reconnect of synchronizers")
        else
          logger.info("Not starting any connected-synchronizer as none can be contacted")
      }
      // step subscribe
      _ <- startConnectedSynchronizers(connected).mapK(FutureUnlessShutdown.outcomeK)
    } yield {
      if (connected != configs)
        logger.info(
          s"Successfully re-connected to a subset of synchronizers $connected, failed to connect to ${configs.toSet -- connected.toSet}"
        )
      else
        logger.info(s"Successfully re-connected to synchronizers $connected")
      connected
    }
  }

  /** Start processing on the connected synchronizer. */
  private def startConnectedSynchronizer(
      synchronizerAlias: SynchronizerAlias,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Unit] =
    EitherT(connectedSynchronizer.start())
      .leftMap(error =>
        SyncServiceError.SyncServiceStartupError.InitError(synchronizerAlias, error)
      )
      .onShutdown(
        Left(
          SyncServiceError.SyncServiceStartupError
            .InitError(synchronizerAlias, AbortedDueToShutdownError("Aborted due to shutdown"))
        )
      )

  /** Connect the sync service to the given synchronizer. This method makes sure there can only be
    * one connection in progress at a time.
    */
  def connectSynchronizer(
      synchronizerAlias: SynchronizerAlias,
      keepRetrying: Boolean,
      connectSynchronizer: ConnectSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Boolean] =
    synchronizerConnectionConfigByAlias(synchronizerAlias)
      .mapK(FutureUnlessShutdown.outcomeK)
      .leftMap(_ => SyncServiceError.SyncServiceUnknownSynchronizer.Error(synchronizerAlias))
      .flatMap { _ =>
        val initial = if (keepRetrying) {
          // we're remembering that we have been trying to reconnect here
          attemptReconnect
            .put(
              synchronizerAlias,
              AttemptReconnect(
                synchronizerAlias,
                clock.now,
                parameters.sequencerClient.startupConnectionRetryDelay.unwrap,
                traceContext,
              ),
            )
            .isEmpty
        } else true
        attemptSynchronizerConnection(
          synchronizerAlias,
          keepRetrying = keepRetrying,
          initial = initial,
          connectSynchronizer = connectSynchronizer,
        )
      }

  private def attemptSynchronizerConnection(
      synchronizerAlias: SynchronizerAlias,
      keepRetrying: Boolean,
      initial: Boolean,
      connectSynchronizer: ConnectSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Boolean] =
    connectQueue.executeEUS(
      if (keepRetrying && !attemptReconnect.isDefinedAt(synchronizerAlias)) {
        EitherT.rightT[FutureUnlessShutdown, SyncServiceError](false)
      } else {
        performSynchronizerConnectionOrHandshake(
          synchronizerAlias,
          connectSynchronizer,
        ).transform {
          case Left(SyncServiceError.SyncServiceFailedSynchronizerConnection(_, err))
              if keepRetrying && err.retryable.nonEmpty =>
            if (initial)
              logger.warn(s"Initial connection attempt to $synchronizerAlias failed with ${err.code
                  .toMsg(err.cause, traceContext.traceId, limit = None)}. Will keep on trying.")
            else
              logger.info(
                s"Initial connection attempt to $synchronizerAlias failed. Will keep on trying."
              )
            scheduleReconnectAttempt(
              clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava),
              ConnectSynchronizer.Connect,
            )
            Right(false)
          case Right(()) =>
            resolveReconnectAttempts(synchronizerAlias)
            Right(true)
          case Left(x) =>
            resolveReconnectAttempts(synchronizerAlias)
            Left(x)
        }
      },
      s"connect to $synchronizerAlias",
    )

  private def scheduleReconnectAttempt(
      timestamp: CantonTimestamp,
      connectSynchronizer: ConnectSynchronizer,
  ): Unit = {
    def mergeLarger(cur: Option[CantonTimestamp], ts: CantonTimestamp): Option[CantonTimestamp] =
      cur match {
        case None => Some(ts)
        case Some(old) => Some(ts.max(old))
      }

    def reconnectAttempt(ts: CantonTimestamp): Unit = {
      val (reconnect, nextO) =
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
      reconnect.foreach { item =>
        implicit val traceContext: TraceContext = item.trace
        val synchronizerAlias = item.alias
        logger.debug(s"Starting background reconnect attempt for $synchronizerAlias")
        EitherTUtil.doNotAwaitUS(
          attemptSynchronizerConnection(
            item.alias,
            keepRetrying = true,
            initial = false,
            connectSynchronizer = connectSynchronizer,
          ),
          s"Background reconnect to $synchronizerAlias",
        )
      }
      nextO.foreach(scheduleReconnectAttempt(_, connectSynchronizer))
    }

    clock.scheduleAt(reconnectAttempt, timestamp).discard
  }

  def synchronizerConnectionConfigByAlias(
      synchronizerAlias: SynchronizerAlias
  ): EitherT[Future, MissingConfigForAlias, StoredSynchronizerConnectionConfig] =
    EitherT.fromEither[Future](synchronizerConnectionConfigStore.get(synchronizerAlias))

  private def performSynchronizerConnectionOrHandshake(
      synchronizerAlias: SynchronizerAlias,
      connectSynchronizer: ConnectSynchronizer,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    connectSynchronizer match {
      case ConnectSynchronizer.HandshakeOnly =>
        performSynchronizerHandshake(synchronizerAlias, skipStatusCheck = skipStatusCheck)
      case _ =>
        performSynchronizerConnection(
          synchronizerAlias,
          startConnectedSynchronizerProcessing = connectSynchronizer.startConnectedSynchronizer,
          skipStatusCheck = skipStatusCheck,
        )
    }

  /** Perform handshake with the given synchronizer. */
  private def performSynchronizerHandshake(
      synchronizerAlias: SynchronizerAlias,
      skipStatusCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    if (
      aliasManager
        .synchronizerIdForAlias(synchronizerAlias)
        .exists(connectedSynchronizersMap.contains)
    ) {
      logger.debug(s"Synchronizer ${synchronizerAlias.unwrap} already registered")
      EitherT.rightT(())
    } else {

      logger.debug(s"About to perform handshake with synchronizer: ${synchronizerAlias.unwrap}")
      for {
        synchronizerConnectionConfig <- synchronizerConnectionConfigByAlias(synchronizerAlias)
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[SyncServiceError] { case MissingConfigForAlias(alias) =>
            SyncServiceError.SyncServiceUnknownSynchronizer.Error(alias)
          }
        // do not connect to a synchronizer that is not active
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          synchronizerConnectionConfig.status.isActive || skipStatusCheck,
          SyncServiceError.SyncServiceSynchronizerIsNotActive
            .Error(synchronizerAlias, synchronizerConnectionConfig.status): SyncServiceError,
        )
        _ = logger.debug(
          s"Performing handshake with synchronizer with config: ${synchronizerConnectionConfig.config}"
        )
        synchronizerHandle <- EitherT(
          synchronizerRegistry.connect(synchronizerConnectionConfig.config)
        )
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedSynchronizerConnection(synchronizerAlias, err)
          )

        _ = synchronizerHandle.close()
      } yield ()
    }

  /** Connect the sync service to the given synchronizer. */
  private def performSynchronizerConnection(
      synchronizerAlias: SynchronizerAlias,
      startConnectedSynchronizerProcessing: Boolean,
      skipStatusCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def connect(
        config: SynchronizerConnectionConfig
    ): EitherT[FutureUnlessShutdown, SyncServiceError, SynchronizerHandle] =
      EitherT(synchronizerRegistry.connect(config)).leftMap(err =>
        SyncServiceError.SyncServiceFailedSynchronizerConnection(synchronizerAlias, err)
      )

    def handleCloseDegradation(connectedSynchronizer: ConnectedSynchronizer, fatal: Boolean)(
        err: CantonError
    ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
      if (fatal && parameters.exitOnFatalFailures) {
        FatalError.exitOnFatalError(err, logger)
      } else {
        // If the error is not fatal or the crash on fatal failures flag is off, then we report the unhealthy state and disconnect from the synchronizer
        connectedSynchronizer.failureOccurred(err)
        disconnectSynchronizer(synchronizerAlias)
      }

    if (
      aliasManager
        .synchronizerIdForAlias(synchronizerAlias)
        .exists(connectedSynchronizersMap.contains)
    ) {
      logger.debug(s"Already connected to synchronizer: ${synchronizerAlias.unwrap}")
      resolveReconnectAttempts(synchronizerAlias)
      EitherT.rightT(())
    } else {

      logger.debug(s"About to connect to synchronizer: ${synchronizerAlias.unwrap}")
      val connectedSynchronizerMetrics = metrics.connectedSynchronizerMetrics(synchronizerAlias)

      val ret: EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = for {

        synchronizerConnectionConfig <- synchronizerConnectionConfigByAlias(synchronizerAlias)
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[SyncServiceError] { case MissingConfigForAlias(alias) =>
            SyncServiceError.SyncServiceUnknownSynchronizer.Error(alias)
          }
        // do not connect to a synchronizer that is not active
        _ <- EitherT.cond[FutureUnlessShutdown](
          synchronizerConnectionConfig.status.isActive || skipStatusCheck,
          (),
          SyncServiceError.SyncServiceSynchronizerIsNotActive
            .Error(synchronizerAlias, synchronizerConnectionConfig.status): SyncServiceError,
        )
        _ = logger.debug(
          s"Connecting to synchronizer with config: ${synchronizerConnectionConfig.config}"
        )
        synchronizerHandle <- connect(synchronizerConnectionConfig.config)

        synchronizerId = synchronizerHandle.synchronizerId
        synchronizerLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)
        persistent = synchronizerHandle.syncPersistentState

        synchronizerCrypto = syncCrypto.tryForSynchronizer(
          synchronizerId,
          synchronizerHandle.staticParameters,
        )

        // Used to manage (and abort!) all promises related to the synchronizer
        // To be closed by ConnectedSynchronizer
        promiseUSFactory: DefaultPromiseUnlessShutdownFactory =
          new DefaultPromiseUnlessShutdownFactory(timeouts, loggerFactory)

        ephemeral <- EitherT.right[SyncServiceError](
          syncEphemeralStateFactory
            .createFromPersistent(
              persistent,
              ledgerApiIndexer.asEval,
              participantNodePersistentState.map(_.contractStore),
              participantNodeEphemeralState,
              () => {
                val tracker = SynchronizerTimeTracker(
                  synchronizerConnectionConfig.config.timeTracker,
                  clock,
                  synchronizerHandle.sequencerClient,
                  synchronizerHandle.staticParameters.protocolVersion,
                  timeouts,
                  synchronizerLoggerFactory,
                )
                synchronizerHandle.topologyClient.setSynchronizerTimeTracker(tracker)
                tracker
              },
              promiseUSFactory,
              connectedSynchronizerMetrics,
              parameters.cachingConfigs.sessionEncryptionKeyCache,
              participantId,
            )
        )

        missingKeysAlerter = new MissingKeysAlerter(
          participantId,
          synchronizerId,
          synchronizerHandle.topologyClient,
          synchronizerCrypto.crypto.cryptoPrivateStore,
          synchronizerLoggerFactory,
        )

        connectedSynchronizer <- EitherT.right(
          connectedSynchronizerFactory.create(
            synchronizerId,
            synchronizerHandle,
            participantId,
            engine,
            parameters,
            participantNodePersistentState,
            persistent,
            ephemeral,
            packageService,
            synchronizerCrypto,
            identityPusher,
            synchronizerHandle.topologyFactory
              .createTopologyProcessorFactory(
                synchronizerHandle.staticParameters,
                partyNotifier,
                missingKeysAlerter,
                synchronizerHandle.topologyClient,
                ephemeral.recordOrderPublisher,
                synchronizerHandle.syncPersistentState.sequencedEventStore,
                ledgerApiIndexer.asEval.value.ledgerApiStore.value,
              ),
            missingKeysAlerter,
            reassignmentCoordination,
            commandProgressTracker,
            clock,
            promiseUSFactory,
            connectedSynchronizerMetrics,
            futureSupervisor,
            synchronizerLoggerFactory,
            testingConfig,
          )
        )

        _ = connectedSynchronizerHealth.set(connectedSynchronizer)
        _ = ephemeralHealth.set(connectedSynchronizer.ephemeral)
        _ = sequencerClientHealth.set(connectedSynchronizer.sequencerClient.healthComponent)
        _ = acsCommitmentProcessorHealth.set(
          connectedSynchronizer.acsCommitmentProcessor.healthComponent
        )
        _ = connectedSynchronizer.resolveUnhealthy()

        _ = connectedSynchronizersMap += (synchronizerId -> connectedSynchronizer)

        // Start sequencer client subscription only after synchronizer has been added to connectedSynchronizersMap, e.g. to
        // prevent sending PartyAddedToParticipantEvents before the synchronizer is available for command submission. (#2279)
        _ <-
          if (startConnectedSynchronizerProcessing) {
            logger.info(
              s"Connected to synchronizer and starting synchronisation: $synchronizerAlias"
            )
            startConnectedSynchronizer(synchronizerAlias, connectedSynchronizer).mapK(
              FutureUnlessShutdown.outcomeK
            )
          } else {
            logger.info(
              s"Connected to synchronizer: $synchronizerAlias, without starting synchronisation"
            )
            EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
          }
        _ = synchronizerHandle.sequencerClient.completion.onComplete {
          case Success(UnlessShutdown.Outcome(denied: CloseReason.PermissionDenied)) =>
            handleCloseDegradation(connectedSynchronizer, fatal = false)(
              SyncServiceSynchronizerDisabledUs.Error(synchronizerAlias, denied.cause)
            ).discard
          case Success(UnlessShutdown.Outcome(CloseReason.BecamePassive)) =>
            handleCloseDegradation(connectedSynchronizer, fatal = false)(
              SyncServiceBecamePassive.Error(synchronizerAlias)
            ).discard
          case Success(UnlessShutdown.Outcome(error: CloseReason.UnrecoverableError)) =>
            if (isClosing)
              disconnectSynchronizer(synchronizerAlias).discard
            else
              handleCloseDegradation(connectedSynchronizer, fatal = true)(
                SyncServiceSynchronizerDisconnect.UnrecoverableError(synchronizerAlias, error.cause)
              ).discard
          case Success(UnlessShutdown.Outcome(error: CloseReason.UnrecoverableException)) =>
            handleCloseDegradation(connectedSynchronizer, fatal = true)(
              SyncServiceSynchronizerDisconnect.UnrecoverableException(
                synchronizerAlias,
                error.throwable,
              )
            ).discard
          case Success(UnlessShutdown.Outcome(CloseReason.ClientShutdown)) =>
            logger.info(s"$synchronizerAlias disconnected because sequencer client was closed")
            disconnectSynchronizer(synchronizerAlias).discard
          case Success(UnlessShutdown.AbortedDueToShutdown) =>
            logger.info(s"$synchronizerAlias disconnected because of shutdown")
            disconnectSynchronizer(synchronizerAlias).discard
          case Failure(exception) =>
            handleCloseDegradation(connectedSynchronizer, fatal = true)(
              SyncServiceSynchronizerDisconnect.UnrecoverableException(synchronizerAlias, exception)
            ).discard
        }
      } yield {
        // remove this one from the reconnect attempt list, as we are successfully connected now
        this.resolveReconnectAttempts(synchronizerAlias)
      }

      def disconnectOn(): Unit =
        // only invoke synchronizer disconnect if we actually got so far that the synchronizer id has been read from the remote node
        if (aliasManager.synchronizerIdForAlias(synchronizerAlias).nonEmpty)
          performSynchronizerDisconnect(
            synchronizerAlias
          ).discard // Ignore Lefts because we don't know to what extent the connection succeeded.

      def handleOutcome(
          outcome: UnlessShutdown[Either[SyncServiceError, Unit]]
      ): UnlessShutdown[Either[SyncServiceError, Unit]] =
        outcome match {
          case x @ UnlessShutdown.Outcome(Right(())) =>
            aliasManager.synchronizerIdForAlias(synchronizerAlias).foreach { synchronizerId =>
              connectionListeners.get().foreach(_(Traced(synchronizerId)))
            }
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
              s"performing synchronizer connection for ${synchronizerAlias.unwrap} failed with an unhandled error",
              err,
            )
            err
          },
        )
      )
    }
  }

  /** Disconnect the given synchronizer from the sync service. */
  def disconnectSynchronizer(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    resolveReconnectAttempts(synchronizerAlias)
    connectQueue.executeE(
      EitherT.fromEither(performSynchronizerDisconnect(synchronizerAlias)),
      s"disconnect from $synchronizerAlias",
    )
  }

  def logout(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      synchronizerId <- EitherT.fromOption[FutureUnlessShutdown](
        aliasManager.synchronizerIdForAlias(synchronizerAlias),
        SyncServiceUnknownSynchronizer.Error(synchronizerAlias).asGrpcError.getStatus,
      )
      _ <- connectedSynchronizersMap
        .get(synchronizerId)
        .fold(EitherT.pure[FutureUnlessShutdown, Status] {
          logger.info(show"Nothing to do, as we are not connected to $synchronizerAlias")
          ()
        })(connectedSynchronizer => connectedSynchronizer.logout())
    } yield ()

  private def performSynchronizerDisconnect(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Either[SyncServiceError, Unit] = {
    logger.info(show"Disconnecting from $synchronizerAlias")
    (for {
      synchronizerId <- aliasManager.synchronizerIdForAlias(synchronizerAlias)
    } yield {
      connectedSynchronizersMap.remove(synchronizerId) match {
        case Some(connectedSynchronizer) =>
          Try(LifeCycle.close(connectedSynchronizer)(logger)) match {
            case Success(_) =>
              logger.info(show"Disconnected from $synchronizerAlias")
            case Failure(ex) =>
              if (parameters.exitOnFatalFailures)
                FatalError.exitOnFatalError(
                  show"Failed to disconnect from $synchronizerAlias due to an exception",
                  ex,
                  logger,
                )
              else throw ex
          }
        case None =>
          logger.info(show"Nothing to do, as we are not connected to $synchronizerAlias")
      }
    }).toRight(SyncServiceError.SyncServiceUnknownSynchronizer.Error(synchronizerAlias))
  }

  /** Disconnect from all connected synchronizers. */
  def disconnectSynchronizers()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    val connectedSynchronizers =
      connectedSynchronizersMap.keys.toList.mapFilter(aliasManager.aliasForSynchronizerId).distinct
    connectedSynchronizers.parTraverse_(disconnectSynchronizer)
  }

  /** prepares a synchronizer connection for migration: connect and wait until the topology state
    * has been pushed so we don't deploy against an empty synchronizer
    */
  private def prepareSynchronizerConnectionForMigration(
      aliasT: Traced[SynchronizerAlias]
  ): EitherT[FutureUnlessShutdown, SynchronizerMigrationError, Unit] = aliasT.withTraceContext {
    implicit tx => alias =>
      logger.debug(s"Preparing connection to $alias for migration")
      (for {
        _ <- performSynchronizerConnectionOrHandshake(
          alias,
          ConnectSynchronizer.Connect,
          skipStatusCheck = true,
        )
        success <- identityPusher
          .awaitIdle(alias, timeouts.unbounded.unwrap)
          .leftMap(reg => SyncServiceError.SyncServiceFailedSynchronizerConnection(alias, reg))
        // now, tick the synchronizer so we can be sure to have a tick that includes the topology changes
        syncService <- EitherT.fromEither[FutureUnlessShutdown](
          connectedSynchronizerForAlias(alias).toRight(
            SyncServiceError.SyncServiceUnknownSynchronizer.Error(alias)
          )
        )
        tick = syncService.topologyClient.approximateTimestamp
        _ = logger.debug(s"Awaiting tick at $tick from $alias for migration")
        _ <- EitherT.right(
          FutureUnlessShutdown.outcomeF(
            syncService.timeTracker.awaitTick(tick).fold(Future.unit)(_.void)
          )
        )
        _ <- repairService
          .awaitCleanSequencerTimestamp(syncService.synchronizerId, tick)
          .leftMap(err =>
            SyncServiceError.SyncServiceInternalError.CleanHeadAwaitFailed(alias, tick, err)
          )
        _ = logger.debug(
          s"Received timestamp from $alias for migration and advanced clean-head to it"
        )
        _ <- EitherT.fromEither[FutureUnlessShutdown](performSynchronizerDisconnect(alias))
      } yield success)
        .leftMap[SynchronizerMigrationError](err =>
          SynchronizerMigrationError.MigrationParentError(alias, err)
        )
        .flatMap { success =>
          EitherT.cond[FutureUnlessShutdown](
            success,
            (),
            SynchronizerMigrationError.InternalError.Generic(
              "Failed to successfully dispatch topology state to target synchronizer"
            ): SynchronizerMigrationError,
          )
        }
  }

  // Canton assumes that as long as the CantonSyncService is up we are "read"-healthy. We could consider lack
  // of storage readability as a way to be read-unhealthy, but as participants share the database backend with
  // the ledger-api-server and indexer, database-non-availability is already flagged upstream.
  override def currentHealth(): HealthStatus = HealthStatus.healthy

  // Write health requires the ability to transact, i.e. connectivity to at least one synchronizer and HA-activeness.
  def currentWriteHealth(): HealthStatus =
    if (existsReadySynchronizer && isActive()) HealthStatus.healthy else HealthStatus.unhealthy

  def computeTotalLoad: Int = connectedSynchronizersMap.foldLeft(0) {
    case (acc, (_, connectedSynchronizer)) =>
      acc + connectedSynchronizer.numberOfDirtyRequests()
  }

  private val emitWarningOnDetailLoggingAndHighLoad =
    (parameters.general.loggingConfig.eventDetails || parameters.general.loggingConfig.api.messagePayloads) && parameters.general.loggingConfig.api.warnBeyondLoad.nonEmpty

  def checkOverloaded(traceContext: TraceContext): Option[state.SubmissionResult] = {
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
      _ <- synchronizerConnectionConfigStore.refreshCache()
      _ <- resourceManagementService.refreshCache()
    } yield ()

  override def onClosed(): Unit = {
    val instances = Seq(
      connectQueue,
      migrationService,
      repairService,
      pruningProcessor,
    ) ++ partyReplicatorO.toList ++ syncCrypto.ips.allSynchronizers.toSeq ++ connectedSynchronizersMap.values.toSeq ++ Seq(
      transactionRoutingProcessor,
      synchronizerRegistry,
      synchronizerConnectionConfigStore,
      syncPersistentStateManager,
      // As currently we stop the persistent state in here as a next step,
      // and as we need the indexer to terminate before the persistent state and after the sources which are pushing to the indexing queue(connected synchronizers, inFlightSubmissionTracker etc),
      // we need to terminate the indexer right here
      (() => ledgerApiIndexer.closeCurrent()): AutoCloseable,
      participantNodePersistentState.value,
      connectedSynchronizerHealth,
      ephemeralHealth,
      sequencerClientHealth,
      acsCommitmentProcessorHealth,
    )

    LifeCycle.close(instances*)(logger)
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
      logger.debug(s"Received submit-reassignment $commandId from ledger-api server")

      /* @param synchronizer For unassignment this should be the source synchronizer, for assignment this is the target synchronizer
       */
      def doReassignment[E <: ReassignmentProcessorError, T](
          synchronizerId: SynchronizerId
      )(
          reassign: ConnectedSynchronizer => EitherT[Future, E, FutureUnlessShutdown[T]]
      )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
        for {
          connectedSynchronizer <- EitherT.fromOption[Future](
            readyConnectedSynchronizerById(synchronizerId),
            ifNone = RequestValidationErrors.InvalidArgument
              .Reject(s"Synchronizer id not found: $synchronizerId"): DamlError,
          )
          _ <- reassign(connectedSynchronizer)
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
        .leftMap(error => SubmissionResult.SynchronousError(error.asGrpcStatus))
        .merge

      def getProtocolVersion(synchronizerId: SynchronizerId): Future[ProtocolVersion] =
        protocolVersionGetter(Traced(synchronizerId)) match {
          case Some(protocolVersion) => Future.successful(protocolVersion)
          case None =>
            Future.failed(
              RequestValidationErrors.InvalidArgument
                .Reject(s"Synchronizer id's protocol version not found: $synchronizerId")
                .asGrpcError
            )
        }

      reassignmentCommand match {
        case unassign: ReassignmentCommand.Unassign =>
          for {
            targetProtocolVersion <- getProtocolVersion(unassign.targetSynchronizer.unwrap).map(
              Target(_)
            )
            submissionResult <- doReassignment(
              synchronizerId = unassign.sourceSynchronizer.unwrap
            )(
              _.submitUnassignment(
                submitterMetadata = ReassignmentSubmitterMetadata(
                  submitter = submitter,
                  applicationId = applicationId,
                  submittingParticipant = participantId,
                  commandId = commandId,
                  submissionId = submissionId,
                  workflowId = workflowId,
                ),
                contractId = unassign.contractId,
                targetSynchronizer = unassign.targetSynchronizer,
                targetProtocolVersion = targetProtocolVersion,
              )
            )
          } yield submissionResult

        case assign: ReassignmentCommand.Assign =>
          doReassignment(
            synchronizerId = assign.targetSynchronizer.unwrap
          )(
            _.submitAssignment(
              submitterMetadata = ReassignmentSubmitterMetadata(
                submitter = submitter,
                applicationId = applicationId,
                submittingParticipant = participantId,
                commandId = commandId,
                submissionId = submissionId,
                workflowId = workflowId,
              ),
              reassignmentId = ReassignmentId(assign.sourceSynchronizer, assign.unassignId),
            )
          )
      }
    }.asJava
  }

  override def getConnectedSynchronizers(
      request: SyncService.ConnectedSynchronizerRequest
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SyncService.ConnectedSynchronizerResponse] = {
    def getSnapshot(
        synchronizerAlias: SynchronizerAlias,
        synchronizerId: SynchronizerId,
    ): FutureUnlessShutdown[TopologySnapshot] =
      syncCrypto.ips
        .forSynchronizer(synchronizerId)
        .toFutureUS(
          new Exception(
            s"Failed retrieving SynchronizerTopologyClient for synchronizer `$synchronizerId` with alias $synchronizerAlias"
          )
        )
        .map(_.currentSnapshotApproximation)

    val result = readySynchronizers
      // keep only healthy synchronizers
      .collect {
        case (synchronizerAlias, (synchronizerId, submissionReady)) if submissionReady.unwrap =>
          for {
            topology <- getSnapshot(synchronizerAlias, synchronizerId)
            partyWithAttributes <- topology.hostedOn(
              Set(request.party),
              participantId = request.participantId.getOrElse(participantId),
            )
          } yield partyWithAttributes
            .get(request.party)
            .map(attributes =>
              ConnectedSynchronizerResponse.ConnectedSynchronizer(
                synchronizerAlias,
                synchronizerId,
                attributes.permission,
              )
            )
      }.toSeq

    FutureUnlessShutdown.sequence(result).map(_.flatten).map(ConnectedSynchronizerResponse.apply)
  }

  override def incompleteReassignmentOffsets(
      validAt: Offset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Vector[Offset]] =
    syncPersistentStateManager.getAll.values.toList
      .parTraverse {
        _.reassignmentStore.findIncomplete(
          sourceSynchronizer = None,
          validAt = validAt,
          stakeholders = NonEmpty.from(stakeholders),
          limit = NonNegativeInt.maxValue,
        )
      }
      .map(
        _.flatten
          .map(_.reassignmentEventGlobalOffset.globalOffset)
          .toVector
      )

  override def selectRoutingSynchronizer(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      disclosedContractIds: List[LfContractId],
      optSynchronizerId: Option[SynchronizerId],
      // TODO(#23334): Wire-up in topology-aware package selection roll-out
      _transactionUsedForExternalSigning: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, SynchronizerRank] =
    if (existsReadySynchronizer) {
      val synchronizerState = RoutingSynchronizerState(connectedSynchronizersLookup)
      transactionRoutingProcessor
        .selectRoutingSynchronizer(
          submitterInfo,
          transaction,
          synchronizerState,
          CantonTimestamp(transactionMeta.ledgerEffectiveTime),
          disclosedContractIds,
          optSynchronizerId,
        )
        .leftWiden[CantonBaseError]
    } else
      EitherT.leftT[FutureUnlessShutdown, SynchronizerRank](
        SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error()
      )
}

object CantonSyncService {
  sealed trait ConnectSynchronizer extends Product with Serializable {

    /** Wether to start processing for a connected synchronizer. */
    def startConnectedSynchronizer: Boolean

    // Whether the synchronizer is added in the `connectedSynchronizersMap` map
    def markSynchronizerAsConnected: Boolean
  }

  object ConnectSynchronizer {
    // Normal use case: do everything
    case object Connect extends ConnectSynchronizer {
      override def startConnectedSynchronizer: Boolean = true

      override def markSynchronizerAsConnected: Boolean = true
    }

    /*
    This is used with reconnectSynchronizers.
    Because of the comment
      we need to start all synchronizers concurrently in order to avoid the reassignment processing
    then we need to be able to delay starting the processing on the connected synchronizer.
     */
    case object ReconnectSynchronizers extends ConnectSynchronizer {
      override def startConnectedSynchronizer: Boolean = false

      override def markSynchronizerAsConnected: Boolean = true
    }

    /*
      Used when we only want to do the handshake (get the synchronizer parameters) and do not connect to the synchronizer.
      Use case: major upgrade for early mainnet (we want to be sure we don't process any transaction before
      the ACS is imported).
     */
    case object HandshakeOnly extends ConnectSynchronizer {
      override def startConnectedSynchronizer: Boolean = false

      override def markSynchronizerAsConnected: Boolean = false
    }
  }

  trait Factory[+T <: CantonSyncService] {
    def create(
        participantId: ParticipantId,
        synchronizerRegistry: SynchronizerRegistry,
        synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
        synchronizerAliasManager: SynchronizerAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncPersistentStateManager: SyncPersistentStateManager,
        packageService: Eval[PackageService],
        partyOps: PartyOps,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiParticipantProvider,
        engine: Engine,
        commandProgressTracker: CommandProgressTracker,
        syncEphemeralStateFactory: SyncEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        pruningProcessor: PruningProcessor,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        exitOnFatalFailures: Boolean,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
        ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
        connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
    )(implicit ec: ExecutionContextExecutor, mat: Materializer, tracer: Tracer): T
  }

  object DefaultFactory extends Factory[CantonSyncService] {
    override def create(
        participantId: ParticipantId,
        synchronizerRegistry: SynchronizerRegistry,
        synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
        synchronizerAliasManager: SynchronizerAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncPersistentStateManager: SyncPersistentStateManager,
        packageService: Eval[PackageService],
        partyOps: PartyOps,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiParticipantProvider,
        engine: Engine,
        commandProgressTracker: CommandProgressTracker,
        syncEphemeralStateFactory: SyncEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        pruningProcessor: PruningProcessor,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        exitOnFatalFailures: Boolean,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
        ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
        connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
    )(implicit
        ec: ExecutionContextExecutor,
        mat: Materializer,
        tracer: Tracer,
    ): CantonSyncService =
      new CantonSyncService(
        participantId,
        synchronizerRegistry,
        synchronizerConnectionConfigStore,
        synchronizerAliasManager,
        participantNodePersistentState,
        participantNodeEphemeralState,
        syncPersistentStateManager,
        packageService,
        partyOps,
        identityPusher,
        partyNotifier,
        syncCrypto,
        pruningProcessor,
        engine,
        commandProgressTracker,
        syncEphemeralStateFactory,
        clock,
        resourceManagementService,
        cantonParameterConfig,
        ConnectedSynchronizer.DefaultFactory,
        storage,
        metrics,
        sequencerInfoLoader,
        () => storage.isActive,
        futureSupervisor,
        loggerFactory,
        testingConfig,
        ledgerApiIndexer,
        connectedSynchronizersLookupContainer,
      )
  }
}
