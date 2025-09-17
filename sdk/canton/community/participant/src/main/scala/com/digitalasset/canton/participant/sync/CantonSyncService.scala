// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApiParticipantProvider}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Offset,
  ReassignmentSubmitterMetadata,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.*
import com.digitalasset.canton.error.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
}
import com.digitalasset.canton.health.MutableHealthComponent
import com.digitalasset.canton.ledger.api.health.HealthStatus
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
import com.digitalasset.canton.participant.admin.repair.{CommitmentsService, RepairService}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionFailure,
  TransactionSubmissionUnknown,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.TopologyPackageMapBuilder
import com.digitalasset.canton.participant.protocol.submission.routing.{
  AdmissibleSynchronizersComputation,
  RoutingSynchronizerStateFactory,
  TransactionRoutingProcessor,
}
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownAlias
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  PartyAllocationCannotDetermineSynchronizer,
  PartyAllocationNoSynchronizerError,
  SyncServicePurgeSynchronizerError,
}
import com.digitalasset.canton.participant.sync.SynchronizerConnectionsManager.{
  ConnectSynchronizer,
  ConnectionListener,
}
import com.digitalasset.canton.participant.synchronizer.*
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.Schedulers
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
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
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, SubmissionId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Right, Success}

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
    private[canton] val packageService: PackageService,
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
    metrics: ParticipantMetrics,
    sequencerInfoLoader: SequencerInfoLoader,
    val isActive: () => Boolean,
    declarativeChangeTrigger: () => Unit,
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
    with InternalIndexServiceProviderImpl {

  private val connectionsManager = new SynchronizerConnectionsManager(
    participantId,
    synchronizerRegistry,
    synchronizerConnectionConfigStore,
    aliasManager,
    participantNodePersistentState,
    participantNodeEphemeralState,
    syncPersistentStateManager,
    packageService,
    identityPusher,
    partyNotifier,
    syncCrypto,
    engine,
    commandProgressTracker,
    syncEphemeralStateFactory,
    clock,
    resourceManagementService,
    parameters,
    connectedSynchronizerFactory,
    metrics,
    isActive,
    declarativeChangeTrigger,
    futureSupervisor,
    loggerFactory,
    testingConfig,
    ledgerApiIndexer,
    connectedSynchronizersLookupContainer,
  )

  private def connectedSynchronizersLookup: ConnectedSynchronizersLookup =
    connectionsManager.connectedSynchronizers

  import ShowUtil.*

  def connectedSynchronizerHealth: MutableHealthComponent =
    connectionsManager.connectedSynchronizerHealth
  def ephemeralHealth: MutableHealthComponent = connectionsManager.ephemeralHealth
  def sequencerClientHealth: MutableHealthComponent = connectionsManager.sequencerClientHealth
  def acsCommitmentProcessorHealth: MutableHealthComponent =
    connectionsManager.acsCommitmentProcessorHealth

  val maxDeduplicationDuration: NonNegativeFiniteDuration =
    participantNodePersistentState.value.settingsStore.settings.maxDeduplicationDuration
      .getOrElse(throw new RuntimeException("Max deduplication duration is not available"))

  def subscribeToConnections(subscriber: ConnectionListener): Unit =
    connectionsManager.subscribeToConnections(subscriber)

  protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

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
  // TODO(i25076) remove this waiting logic once topology events are published on the ledger api
  val synchronizeVettingOnConnectedSynchronizers: PackageVettingSynchronization =
    new PackageVettingSynchronization {
      override def sync(packages: Set[PackageId])(implicit
          traceContext: TraceContext
      ): EitherT[Future, ParticipantTopologyManagerError, Unit] =
        // wait for packages to be vetted on the currently connected synchronizers
        EitherT
          .right[ParticipantTopologyManagerError](
            connectedSynchronizersLookup.snapshot.toSeq.parTraverse {
              case (psid, connectedSynchronizer) =>
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
                  .map(psid -> _)
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

  /** Return the active PSId corresponding to the given id, if any. Since at most one synchronizer
    * connection per LSId can be active, this is well-defined.
    */
  def activePSIdForLSId(
      id: SynchronizerId
  ): Option[PhysicalSynchronizerId] =
    synchronizerConnectionConfigStore
      .getActive(id)
      .toOption
      .flatMap(_.configuredPSId.toOption)

  // A connected synchronizer is ready if recovery has succeeded
  private[canton] def readyConnectedSynchronizerById(
      synchronizerId: SynchronizerId
  ): Option[ConnectedSynchronizer] =
    connectionsManager.readyConnectedSynchronizerById(synchronizerId)

  private[canton] def connectedSynchronizerForAlias(
      alias: SynchronizerAlias
  ): Option[ConnectedSynchronizer] = connectionsManager.connectedSynchronizerForAlias(alias)

  private val admissibleSynchronizers =
    new AdmissibleSynchronizersComputation(participantId, loggerFactory)
  private val topologyPackageMapBuilder = new TopologyPackageMapBuilder(
    admissibleSynchronizersComputation = admissibleSynchronizers,
    loggerFactory = loggerFactory,
  )

  private val transactionRoutingProcessor = TransactionRoutingProcessor(
    connectedSynchronizersLookup = connectedSynchronizersLookup,
    cryptoPureApi = syncCrypto.pureCrypto,
    synchronizerConnectionConfigStore = synchronizerConnectionConfigStore,
    participantId = participantId,
    parameters = parameters,
    loggerFactory = loggerFactory,
  )(ec)

  if (isActive()) {
    TraceContext.withNewTraceContext("initialize_state") { implicit traceContext =>
      initializeState()
    }
  }

  private val contractAuthenticator = ContractAuthenticator(syncCrypto.pureCrypto)

  val repairService: RepairService = new RepairService(
    participantId,
    syncCrypto,
    packageService.packageDependencyResolver,
    contractAuthenticator,
    participantNodePersistentState.map(_.contractStore),
    ledgerApiIndexer.asEval(TraceContext.empty),
    aliasManager,
    parameters,
    syncPersistentStateManager,
    connectedSynchronizersLookup,
    connectionsManager.connectQueue,
    loggerFactory,
  )

  private val migrationService =
    new SynchronizerMigration(
      aliasManager,
      synchronizerConnectionConfigStore,
      stateInspection,
      repairService,
      prepareSynchronizerConnectionForMigration,
      sequencerInfoLoader,
      parameters.processingTimeouts,
      loggerFactory,
    )

  val commitmentsService: CommitmentsService = new CommitmentsService(
    ledgerApiIndexer.asEval(TraceContext.empty),
    parameters,
    syncPersistentStateManager,
    connectedSynchronizersLookup,
    loggerFactory,
  )

  val dynamicSynchronizerParameterGetter =
    new CantonDynamicSynchronizerParameterGetter(
      syncCrypto,
      aliasManager,
      synchronizerConnectionConfigStore,
      loggerFactory,
    )

  private def trackSubmission(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
  ): Unit =
    commandProgressTracker
      .findHandle(
        submitterInfo.commandId,
        submitterInfo.userId,
        submitterInfo.actAs,
        submitterInfo.submissionId,
      )
      .recordTransactionImpact(transaction)

  // Submit a transaction (write service implementation)
  override def submitTransaction(
      transaction: LfSubmittedTransaction,
      synchronizerRank: SynchronizerRank,
      routingSynchronizerState: RoutingSynchronizerState,
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      _estimatedInterpretationCost: Long,
      keyResolver: LfKeyResolver,
      processedDisclosedContracts: ImmArray[LfFatContractInst],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] = {
    import scala.jdk.FutureConverters.*
    withSpan("CantonSyncService.submitTransaction") { implicit traceContext => span =>
      span.setAttribute("command_id", submitterInfo.commandId)
      logger.debug(s"Received submit-transaction ${submitterInfo.commandId} from ledger-api server")

      trackSubmission(submitterInfo, transaction)
      submitTransactionF(
        synchronizerRank = synchronizerRank,
        routingSynchronizerState = routingSynchronizerState,
        transaction = transaction,
        submitterInfo = submitterInfo,
        transactionMeta = transactionMeta,
        keyResolver = keyResolver,
        explicitlyDisclosedContracts = processedDisclosedContracts,
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
    synchronizerConnectionConfigStore,
    parameters.processingTimeouts,
    new JournalGarbageCollectorControl {
      override def disable(
          synchronizerId: PhysicalSynchronizerId
      )(implicit traceContext: TraceContext): Future[Unit] =
        connectedSynchronizersLookup
          .get(synchronizerId)
          .map(_.addJournalGarageCollectionLock())
          .getOrElse(Future.unit)

      override def enable(
          synchronizerId: PhysicalSynchronizerId
      )(implicit traceContext: TraceContext): Unit =
        connectedSynchronizersLookup
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
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    pruningProcessor.pruneLedgerEvents(pruneUpToInclusive).transform(pruningErrorToCantonError)

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
      synchronizerRank: SynchronizerRank,
      routingSynchronizerState: RoutingSynchronizerState,
      transaction: LfSubmittedTransaction,
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      explicitlyDisclosedContracts: ImmArray[LfFatContractInst],
  )(implicit
      traceContext: TraceContext
  ): Future[Either[SubmissionResult, FutureUnlessShutdown[_]]] = {

    def processSubmissionError(
        error: TransactionError
    ): Either[SubmissionResult, FutureUnlessShutdown[_]] = {
      error.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "userId" -> submitterInfo.userId)
      )
      Left(SubmissionResult.SynchronousError(error.rpcStatus()))
    }

    if (isClosing) {
      Future.successful(processSubmissionError(SubmissionDuringShutdown.Rejection()))
    } else if (!isActive()) {
      // this is the only error we can not really return with a rejection, as this is the passive replica ...
      val err = SyncServiceInjectionError.PassiveReplica.Error(
        submitterInfo.userId,
        submitterInfo.commandId,
      )
      err.logWithContext(
        Map("commandId" -> submitterInfo.commandId, "userId" -> submitterInfo.userId)
      )
      Future.successful(Left(SubmissionResult.SynchronousError(err.rpcStatus())))
    } else if (!routingSynchronizerState.existsReadySynchronizer()) {
      Future.successful(
        processSubmissionError(SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error())
      )
    } else {

      val submittedFF = for {
        metadata <- EitherT
          .fromEither[FutureUnlessShutdown](
            TransactionMetadata.fromTransactionMeta(
              metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
              metaPreparationTime = transactionMeta.preparationTime,
              metaOptNodeSeeds = transactionMeta.optNodeSeeds,
            )
          )
          .leftMap(RoutingInternalError.IllformedTransaction.apply)

        // TODO(#25385):: Consider removing this check as it is redundant
        //                      (performed as well in normalizeAndCheck)
        // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
            case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
              MalformedInputErrors.InvalidPartyIdentifier.Error(err)
          }
        )

        // TODO(#25385):: Consider moving before SyncService, so that the result of command interpretation
        //                      is already sanity checked wrt Canton TX normalization rules
        wfTransaction <- EitherT.fromEither[FutureUnlessShutdown](
          WellFormedTransaction
            .check(transaction, metadata, WithoutSuffixes)
            .leftMap(RoutingInternalError.IllformedTransaction.apply)
        )
        submitted <- transactionRoutingProcessor.submitTransaction(
          submitterInfo = submitterInfo,
          synchronizerRankTarget = synchronizerRank,
          synchronizerState = routingSynchronizerState,
          wfTransaction = wfTransaction,
          transactionMeta = transactionMeta,
          keyResolver = keyResolver,
          explicitlyDisclosedContracts = explicitlyDisclosedContracts,
        )
      } yield submitted

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
              submitterInfo.userId,
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
      synchronizerIdO: Option[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SubmissionResult] = {
    lazy val onlyConnectedSynchronizer =
      connectedSynchronizersLookup.snapshot.toSeq match {
        case Seq((synchronizerId, _)) => Right(synchronizerId)
        case Seq() =>
          Left(
            SubmissionResult.SynchronousError(
              PartyAllocationNoSynchronizerError.Error(rawSubmissionId).asGrpcStatus
            )
          )
        case _otherwise =>
          Left(
            SubmissionResult.SynchronousError(
              PartyAllocationCannotDetermineSynchronizer
                .Error(hint)
                .asGrpcStatus
            )
          )
      }

    val specifiedSynchronizer =
      synchronizerIdO.map(lsid =>
        connectedSynchronizersLookup
          .get(lsid)
          .map(_.psid)
          .toRight(
            SubmissionResult.SynchronousError(
              SyncServiceInjectionError.NotConnectedToSynchronizer
                .Error(lsid.toProtoPrimitive)
                .rpcStatus()
            )
          )
      )

    val synchronizerIdOrDetectionError =
      specifiedSynchronizer.getOrElse(onlyConnectedSynchronizer)

    synchronizerIdOrDetectionError
      .map(partyAllocation.allocate(hint, rawSubmissionId, _))
      .leftMap(FutureUnlessShutdown.pure)
      .merge
  }

  override def uploadDar(dars: Seq[ByteString], submissionId: Ref.SubmissionId)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    withSpan("CantonSyncService.uploadPackages") { implicit traceContext => span =>
      if (!isActive()) {
        logger.debug(s"Rejecting package upload on passive replica.")
        Future.successful(SyncServiceError.Synchronous.PassiveNode)
      } else {
        span.setAttribute("submission_id", submissionId)
        packageService
          .upload(
            dars = dars.map(UploadDarData(_, Some("uploaded-via-ledger-api"), None)),
            submissionIdO = Some(submissionId),
            vetAllPackages = true,
            synchronizeVetting = synchronizeVettingOnConnectedSynchronizers,
          )
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error()))
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
        packageService
          .validateDar(dar, darName)
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error()))
          .valueOr(err => SubmissionResult.SynchronousError(err.asGrpcStatus))
      }
    }

  override def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    packageService
      .getLfArchive(packageId)
      .failOnShutdownTo(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  override def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    packageService
      .listPackages()
      .failOnShutdownTo(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  def getPackageMetadataView: PackageMetadataView = packageService.getPackageMetadataView

  override def getPackageMetadataSnapshot(implicit
      errorLoggingContext: ErrorLoggingContext
  ): PackageMetadata = getPackageMetadataView.getSnapshot

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
  def readySynchronizers: Map[SynchronizerAlias, (PhysicalSynchronizerId, SubmissionReady)] =
    connectionsManager.readySynchronizers

  /** Returns the synchronizers this sync service is configured with. */
  def registeredSynchronizers: Seq[StoredSynchronizerConnectionConfig] =
    synchronizerConnectionConfigStore.getAll()

  /** Returns the pure crypto operations used for the sync protocol */
  def pureCryptoApi: CryptoPureApi = syncCrypto.pureCrypto

  /** Lookup a time tracker for the given `synchronizer`. A time tracker will only be returned if
    * the synchronizer is registered and connected.
    */
  def lookupSynchronizerTimeTracker(
      synchronizer: Synchronizer
  ): Either[String, SynchronizerTimeTracker] =
    connectionsManager.lookupSynchronizerTimeTracker(synchronizer)

  def lookupTopologyClient(
      synchronizerId: PhysicalSynchronizerId
  ): Option[SynchronizerTopologyClientWithInit] =
    connectionsManager.lookupTopologyClient(synchronizerId)

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
      _ <- EitherT
        .rightT[FutureUnlessShutdown, SyncServiceError](
          synchronizerConnectionConfigStore
            .getAllFor(config.synchronizerAlias)
            .fold(_ => Seq.empty[StoredSynchronizerConnectionConfig], _.forgetNE)
        )
        .flatMap { configs =>
          val activeForAlias = configs.filter(_.status == SynchronizerConnectionConfigStore.Active)
          activeForAlias match {
            case Seq() =>
              synchronizerConnectionConfigStore
                .put(
                  config,
                  SynchronizerConnectionConfigStore.Active,
                  configuredPSId = UnknownPhysicalSynchronizerId,
                  synchronizerPredecessor = None,
                )
                .leftMap(e =>
                  SyncServiceError.SynchronizerRegistration
                    .Error(config.synchronizerAlias, e.message): SyncServiceError
                )

            case Seq(storedConfig) =>
              EitherT
                .fromEither[FutureUnlessShutdown](
                  config
                    .subsumeMerge(storedConfig.config)
                    .leftMap(err =>
                      SyncServiceError.SynchronizerRegistration
                        .Error(config.synchronizerAlias, err): SyncServiceError
                    )
                )
                .flatMap(
                  synchronizerConnectionConfigStore
                    .replace(storedConfig.configuredPSId, _)
                    .leftMap(err =>
                      SyncServiceError.SynchronizerRegistration
                        .Error(config.synchronizerAlias, err.message): SyncServiceError
                    )
                )

            case many =>
              EitherT.leftT[FutureUnlessShutdown, Unit](
                SyncServiceError.SynchronizerRegistration
                  .Error(
                    config.synchronizerAlias,
                    s"Unexpectedly found several active connections for alias: ${many.map(_.configuredPSId)}",
                  ): SyncServiceError
              )
          }
        }
    } yield ()

  private def validateSequencerConnection(
      config: SynchronizerConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    sequencerInfoLoader // TODO(i27622): use the connection pool to validate the config
      .validateSequencerConnection(
        config.synchronizerAlias,
        config.synchronizerId,
        config.sequencerConnections,
        sequencerConnectionValidation,
      )
      .leftMap(SyncServiceError.SyncServiceInconsistentConnectivity.Error(_): SyncServiceError)

  /** Modifies the settings of the synchronizer connection
    *
    * @param psidO
    *   If empty, the request will update the single active connection for the alias in `config`
    *   NOTE: This does not automatically reconnect to the synchronizer.
    */
  def modifySynchronizer(
      psidO: Option[PhysicalSynchronizerId],
      config: SynchronizerConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      _ <- validateSequencerConnection(config, sequencerConnectionValidation)

      connectionIdToUpdateE = psidO match {
        case Some(psid) => KnownPhysicalSynchronizerId(psid).asRight[SyncServiceError]
        case None =>
          synchronizerConnectionConfigStore
            .getActive(config.synchronizerAlias)
            .map(_.configuredPSId)
            .leftMap(err =>
              SyncServiceError.SyncServiceAliasResolution
                .Error(config.synchronizerAlias, err.message)
            )
      }
      connectionIdToUpdate <- EitherT.fromEither[FutureUnlessShutdown](connectionIdToUpdateE)

      _ <- synchronizerConnectionConfigStore
        .replace(connectionIdToUpdate, config)
        .leftMap(_ =>
          SyncServiceError.SyncServiceUnknownSynchronizer
            .Error(config.synchronizerAlias): SyncServiceError
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
      connectedSynchronizersLookup.snapshot.values
        .map(
          _.synchronizerHandle.synchronizerAlias
        )
        .toList match {
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
        connectionsManager.connectQueue.executeEUS(
          migrationService
            .migrateSynchronizer(
              source,
              target,
              targetSynchronizerInfo.map(_.psid),
            )
            .leftMap[SyncServiceError](
              SyncServiceError.SyncServiceMigrationError(source, target.map(_.synchronizerAlias), _)
            ),
          "migrate synchronizer",
        )

      _ <- purgeDeactivatedSynchronizer(source.unwrap)
    } yield ()
  }

  @VisibleForTesting
  def upgradeSynchronizerTo(
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    connectionsManager.upgradeSynchronizerTo(currentPSId, synchronizerSuccessor)

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
    connectionsManager.reconnectSynchronizers(ignoreFailures, isTriggeredManually, mustBeActive)

  /** Connect the sync service to the given synchronizer. This method makes sure there can only be
    * one connection in progress at a time.
    */
  def connectSynchronizer(
      synchronizerAlias: SynchronizerAlias,
      keepRetrying: Boolean,
      connectSynchronizer: ConnectSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Option[PhysicalSynchronizerId]] =
    connectionsManager.connectSynchronizer(synchronizerAlias, keepRetrying, connectSynchronizer)

  /** Get the synchronizer connection corresponding to the alias. Fail if no connection can be
    * found. If more than one connections are found, takes the highest one.
    *
    * @param synchronizerAlias
    *   Synchronizer alias
    * @param onlyActive
    *   Restrict connection to active ones (default).
    */
  def getSynchronizerConnectionConfigForAlias(
      synchronizerAlias: SynchronizerAlias,
      onlyActive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[SyncServiceError, StoredSynchronizerConnectionConfig] =
    synchronizerConnectionConfigStore.getAllFor(synchronizerAlias) match {
      case Left(_: UnknownAlias) =>
        SyncServiceError.SyncServiceUnknownSynchronizer.Error(synchronizerAlias).asLeft

      case Right(configs) =>
        val filteredConfigs = if (onlyActive) {
          val active = configs.filter(_.status.isActive)
          NonEmpty
            .from(active)
            .toRight(SyncServiceError.SyncServiceSynchronizerIsNotActive.Error(synchronizerAlias))
        } else configs.asRight

        filteredConfigs.map(_.maxBy1(_.configuredPSId))
    }

  /** Perform a handshake with the given synchronizer.
    * @param synchronizerId
    *   the physical synchronizer id of the synchronizer.
    * @return
    */
  def connectToPSIdWithHandshake(
      synchronizerId: PhysicalSynchronizerId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] =
    connectionsManager.connectToPSIdWithHandshake(synchronizerId)

  /** Disconnect the given synchronizer from the sync service. */
  def disconnectSynchronizer(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    connectionsManager.disconnectSynchronizer(synchronizerAlias)

  def logout(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] = connectionsManager.logout(synchronizerAlias)

  /** Disconnect from all connected synchronizers. */
  def disconnectSynchronizers()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    connectionsManager.disconnectSynchronizers()

  /** prepares a synchronizer connection for migration: connect and wait until the topology state
    * has been pushed so we don't deploy against an empty synchronizer
    */
  private def prepareSynchronizerConnectionForMigration(
      aliasT: Traced[SynchronizerAlias]
  ): EitherT[FutureUnlessShutdown, SynchronizerMigrationError, Unit] = aliasT.withTraceContext {
    implicit tx => alias =>
      logger.debug(s"Preparing connection to $alias for migration")
      (for {
        psid <-
          connectionsManager.performSynchronizerConnectionOrHandshake(
            alias,
            ConnectSynchronizer.Connect,
            skipStatusCheck = true,
          )

        success <- identityPusher
          .awaitIdle(psid, timeouts.unbounded.unwrap)
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
            syncService.timeTracker.awaitTick(tick).getOrElse(Future.unit)
          )
        )
        _ <- repairService
          .awaitCleanSequencerTimestamp(syncService.psid.logical, tick)
          .leftMap(err =>
            SyncServiceError.SyncServiceInternalError.CleanHeadAwaitFailed(alias, tick, err)
          )
        _ = logger.debug(
          s"Received timestamp from $alias for migration and advanced clean-head to it"
        )
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          connectionsManager.performSynchronizerDisconnect(alias)
        )
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
    connectionsManager.currentWriteHealth()

  def computeTotalLoad: Int = connectionsManager.computeTotalLoad

  def checkOverloaded(traceContext: TraceContext): Option[state.SubmissionResult] =
    connectionsManager.checkOverloaded(traceContext)

  def refreshCaches()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- synchronizerConnectionConfigStore.refreshCache()
      _ <- resourceManagementService.refreshCache()
    } yield ()

  override def onClosed(): Unit = {
    val instances = Seq(
      migrationService,
      repairService,
      commitmentsService,
      pruningProcessor,
      syncCrypto,
      connectionsManager,
      transactionRoutingProcessor,
      synchronizerRegistry,
      synchronizerConnectionConfigStore,
      syncPersistentStateManager,
      // As currently we stop the persistent state in here as a next step,
      // and as we need the indexer to terminate before the persistent state and after the sources which are pushing to the indexing queue(connected synchronizers, inFlightSubmissionTracker etc),
      // we need to terminate the indexer right here
      ledgerApiIndexer.currentAutoCloseable(),
      participantNodePersistentState.value,
    )

    LifeCycle.close(instances*)(logger)
  }

  override def toString: String = s"CantonSyncService($participantId)"

  override def submitReassignment(
      submitter: Party,
      userId: Ref.UserId,
      commandId: Ref.CommandId,
      submissionId: Option[SubmissionId],
      workflowId: Option[Ref.WorkflowId],
      reassignmentCommands: Seq[ReassignmentCommand],
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
              .Reject(s"Synchronizer id not found: $synchronizerId"): RpcError,
          )
          _ <- reassign(connectedSynchronizer)
            .leftMap(error =>
              RequestValidationErrors.InvalidArgument
                .Reject(
                  error.message
                ): RpcError // TODO(i13240): Improve reassignment-submission Ledger API errors
            )
            .mapK(FutureUnlessShutdown.outcomeK)
            .semiflatMap(Predef.identity)
            .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error()))
        } yield SubmissionResult.Acknowledged
      }
        .leftMap(error => SubmissionResult.SynchronousError(error.asGrpcStatus))
        .merge

      ReassignmentCommandsBatch.create(reassignmentCommands) match {
        case Right(unassigns: ReassignmentCommandsBatch.Unassignments) =>
          connectedSynchronizersLookup.psidFor(unassigns.target.unwrap) match {
            case Some(targetPSId) =>
              doReassignment(
                synchronizerId = unassigns.source.unwrap
              )(
                _.submitUnassignments(
                  submitterMetadata = ReassignmentSubmitterMetadata(
                    submitter = submitter,
                    userId = userId,
                    submittingParticipant = participantId,
                    commandId = commandId,
                    submissionId = submissionId,
                    workflowId = workflowId,
                  ),
                  contractIds = unassigns.contractIds,
                  targetSynchronizer = Target(targetPSId),
                )
              )

            case None =>
              Future.failed(
                RequestValidationErrors.InvalidArgument
                  .Reject(s"Unable to resolve ${unassigns.target} to a connected synchronizer id")
                  .asGrpcError
              )
          }

        case Right(assigns: ReassignmentCommandsBatch.Assignments) =>
          doReassignment(
            synchronizerId = assigns.target.unwrap
          )(
            _.submitAssignments(
              submitterMetadata = ReassignmentSubmitterMetadata(
                submitter = submitter,
                userId = userId,
                submittingParticipant = participantId,
                commandId = commandId,
                submissionId = submissionId,
                workflowId = workflowId,
              ),
              reassignmentId = assigns.reassignmentId,
            )
          )
        case Left(invalidBatch) =>
          Future.failed(
            RequestValidationErrors.InvalidArgument
              .Reject(s"The batch of reassignment commands was invalid: ${invalidBatch.error}")
              .asGrpcError
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
        synchronizerId: PhysicalSynchronizerId,
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
    MonadUtil
      .sequentialTraverse(
        syncPersistentStateManager.allKnownLSIds
          .flatMap(syncPersistentStateManager.reassignmentStore)
          .toSeq
      )(
        _.findIncomplete(
          sourceSynchronizer = None,
          validAt = validAt,
          stakeholders = NonEmpty.from(stakeholders),
          limit = NonNegativeInt.maxValue,
        )
      )
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
      transactionUsedForExternalSigning: Boolean,
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionError, SynchronizerRank] =
    if (synchronizerState.existsReadySynchronizer()) {
      // Capture the synchronizer state that should be used for the entire phase 1 of the transaction protocol
      transactionRoutingProcessor
        .selectRoutingSynchronizer(
          submitterInfo,
          transaction,
          synchronizerState,
          CantonTimestamp(transactionMeta.ledgerEffectiveTime),
          disclosedContractIds,
          optSynchronizerId,
          transactionUsedForExternalSigning,
        )
        .leftWiden[TransactionError]
    } else
      EitherT.leftT(
        SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error()
      )

  override def packageMapFor(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizer: Option[SynchronizerId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[LfPackageId]]]] =
    topologyPackageMapBuilder.packageMapFor(
      submitters,
      informees,
      vettingValidityTimestamp,
      prescribedSynchronizer,
      routingSynchronizerState,
    )

  override def computeHighestRankedSynchronizerFromAdmissible(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]],
      disclosedContractIds: List[LfContractId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, PhysicalSynchronizerId] =
    transactionRoutingProcessor
      .computeHighestRankedSynchronizerFromAdmissible(
        submitterInfo,
        transaction,
        transactionMeta,
        admissibleSynchronizers,
        disclosedContractIds,
        routingSynchronizerState,
      )

  override def getRoutingSynchronizerState(implicit
      traceContext: TraceContext
  ): RoutingSynchronizerState = {
    val syncCryptoPureApi: RoutingSynchronizerStateFactory.SyncCryptoPureApiLookup =
      (synchronizerId, staticSyncParameters) =>
        syncCrypto.forSynchronizer(synchronizerId, staticSyncParameters).map(_.pureCrypto)
    val routingState =
      RoutingSynchronizerStateFactory.create(
        connectedSynchronizersLookup,
        syncCryptoPureApi,
      )

    val connectedSynchronizers = routingState.connectedSynchronizers.keySet.mkString(", ")
    val topologySnapshotInfo = routingState.topologySnapshots.view
      .map { case (psid, loader) => s"$psid at ${loader.timestamp}" }
      .mkString(", ")

    logger.info(
      show"Routing state contains connected synchronizers $connectedSynchronizers and topology $topologySnapshotInfo"
    )

    routingState
  }
}

object CantonSyncService {
  trait Factory[+T <: CantonSyncService] {
    def create(
        participantId: ParticipantId,
        synchronizerRegistry: SynchronizerRegistry,
        synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
        synchronizerAliasManager: SynchronizerAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncPersistentStateManager: SyncPersistentStateManager,
        packageService: PackageService,
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
        triggerDeclarativeChange: () => Unit,
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
        packageService: PackageService,
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
        triggerDeclarativeChange: () => Unit,
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
        metrics,
        sequencerInfoLoader,
        () => storage.isActive,
        triggerDeclarativeChange,
        futureSupervisor,
        loggerFactory,
        testingConfig,
        ledgerApiIndexer,
        connectedSynchronizersLookupContainer,
      )
  }
}
