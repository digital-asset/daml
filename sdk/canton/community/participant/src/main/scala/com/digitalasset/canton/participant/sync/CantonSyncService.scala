// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoPureApi, SyncCryptoApiProvider}
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
import com.digitalasset.canton.ledger.participant.state.WriteService.ConnectedDomainResponse
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError
import com.digitalasset.canton.participant.admin.inspection.{
  JournalGarbageCollectorControl,
  SyncStateInspection,
}
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.admin.repair.RepairService.DomainLookup
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionFailure,
  TransactionSubmissionUnknown,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentCoordination,
}
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter
import com.digitalasset.canton.participant.pruning.{AcsCommitmentProcessor, PruningProcessor}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.MissingConfigForAlias
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectDomain
import com.digitalasset.canton.participant.sync.SyncDomain.SubmissionReady
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceDomainBecamePassive,
  SyncServiceDomainDisabledUs,
  SyncServiceDomainDisconnect,
  SyncServiceFailedDomainConnection,
  SyncServicePurgeDomainError,
}
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
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClientWithInit, TopologySnapshot}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.ReassignmentTag.Target
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
    private[canton] val packageService: Eval[PackageService],
    partyOps: PartyOps,
    identityPusher: ParticipantTopologyDispatcher,
    partyNotifier: LedgerServerPartyNotifier,
    val syncCrypto: SyncCryptoApiProvider,
    val pruningProcessor: PruningProcessor,
    engine: Engine,
    private[canton] val commandProgressTracker: CommandProgressTracker,
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
    testingConfig: TestingConfigInternal,
    ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
)(implicit ec: ExecutionContextExecutor, mat: Materializer, val tracer: Tracer)
    extends state.WriteService
    with WriteParticipantPruningService
    with FlagCloseable
    with Spanning
    with NamedLogging
    with HasCloseContext
    with InternalStateServiceProviderImpl {

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

  private type ConnectionListener = Traced[DomainId] => Unit

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
    partyOps,
    partyNotifier,
    parameters,
    isActive,
    connectedDomainsLookup,
    timeouts,
    loggerFactory,
  )

  /** Validates that the provided packages are vetted on the currently connected domains. */
  // TODO(#15087) remove this waiting logic once topology events are published on the ledger api
  val synchronizeVettingOnConnectedDomains: PackageVettingSynchronization =
    new PackageVettingSynchronization {
      override def sync(packages: Set[PackageId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
        // wait for packages to be vetted on the currently connected domains
        EitherT
          .right[ParticipantTopologyManagerError](
            connectedDomainsLookup.snapshot.toSeq.parTraverse { case (domainId, syncDomain) =>
              syncDomain.topologyClient
                .await(
                  _.determinePackagesWithNoVettingEntry(participantId, packages)
                    .map(_.isEmpty)
                    .onShutdown(false),
                  timeouts.network.duration,
                )
                // turn AbortedDuToShutdown into a verdict, as we don't want to turn
                // the overall result into AbortedDueToShutdown, just because one of
                // the domains disconnected in the meantime.
                .onShutdown(false)
                .map(domainId -> _)
            }
          )
          .mapK(FutureUnlessShutdown.outcomeK)
          .map { result =>
            result.foreach { case (domainId, successful) =>
              if (!successful)
                logger.info(
                  s"Waiting for vetting of packages $packages on domain $domainId either timed out or the domain got disconnected."
                )
            }
            result
          }
          .void
    }

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

  private[canton] def syncDomainForAlias(alias: DomainAlias): Option[SyncDomain] =
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

  private val reassignmentCoordination: ReassignmentCoordination =
    ReassignmentCoordination(
      parameters.reassignmentTimeProofFreshnessProportion,
      syncDomainPersistentStateManager,
      connectedDomainsLookup.get,
      syncCrypto,
      loggerFactory,
    )(ec)

  val protocolVersionGetter: Traced[DomainId] => Option[ProtocolVersion] =
    (tracedDomainId: Traced[DomainId]) =>
      syncDomainPersistentStateManager.protocolVersionFor(tracedDomainId.value)

  participantNodeEphemeralState.inFlightSubmissionTracker.registerDomainStateLookup(domainId =>
    connectedDomainsMap.get(domainId).map(_.ephemeral.inFlightSubmissionTrackerDomainState)
  )

  if (isActive()) {
    TraceContext.withNewTraceContext { implicit traceContext =>
      initializeState()
    }
  }

  private val repairServiceDAMLe =
    new DAMLe(
      pkgId => traceContext => packageService.value.getPackage(pkgId)(traceContext),
      None,
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
    ledgerApiIndexer.asEval(TraceContext.empty),
    aliasManager,
    parameters,
    Storage.threadsAvailableForWriting(storage),
    new DomainLookup {
      override def isConnected(domainId: DomainId): Boolean =
        connectedDomainsLookup.isConnected(domainId)

      override def isConnectedToAnyDomain: Boolean =
        connectedDomainsMap.nonEmpty

      override def persistentStateFor(domainId: DomainId): Option[SyncDomainPersistentState] =
        syncDomainPersistentStateManager.get(domainId)

      override def topologyFactoryFor(domainId: DomainId): Option[TopologyComponentFactory] =
        syncDomainPersistentStateManager.topologyFactoryFor(domainId)
    },
    // Share the sync service queue with the repair service, so that repair operations cannot run concurrently with
    // domain connections.
    connectQueue,
    loggerFactory,
  )

  private val migrationService =
    new SyncDomainMigration(
      aliasManager,
      domainConnectionConfigStore,
      stateInspection,
      repairService,
      prepareDomainConnectionForMigration,
      sequencerInfoLoader,
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
      optDomainId: Option[DomainId],
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
        optDomainId,
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
      override def enable(domainId: DomainId)(implicit traceContext: TraceContext): Unit =
        connectedDomainsMap
          .get(domainId)
          .foreach(_.removeJournalGarageCollectionLock())
    },
    connectedDomainsLookup,
    participantId,
    loggerFactory,
  )

  override def prune(
      pruneUpToInclusive: Offset,
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
      pruneUpToInclusive: Offset
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    (for {
      pruneUpToMultiDomainGlobalOffset <- EitherT
        .fromEither[FutureUnlessShutdown](UpstreamOffsetConvert.toGlobalOffset(pruneUpToInclusive))
        .leftMap { message =>
          LedgerPruningOffsetNonCantonFormat(
            s"Specified offset does not convert to a canton multi domain event log global offset: $message"
          )
        }
      _pruned <- pruningProcessor.pruneLedgerEvents(pruneUpToMultiDomainGlobalOffset)
    } yield ()).transform(pruningErrorToCantonError)

  private def pruningErrorToCantonError(pruningResult: Either[LedgerPruningError, Unit])(implicit
      traceContext: TraceContext
  ): Either[PruningServiceError, Unit] = pruningResult match {
    case Left(err @ LedgerPruningNothingToPrune) =>
      logger.info(
        s"Could not locate pruning point: ${err.message}. Considering success for idempotency"
      )
      Right(())
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
      optDomainId: Option[DomainId],
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
        optDomainId,
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

  override def allocateParty(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    partyAllocation.allocate(hint, displayName, rawSubmissionId)

  override def uploadDar(dar: ByteString, submissionId: Ref.SubmissionId)(implicit
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
            darBytes = dar,
            fileNameO = None,
            submissionIdO = Some(submissionId),
            vetAllPackages = true,
            synchronizeVetting = synchronizeVettingOnConnectedDomains,
          )
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
          .valueOr(err => SubmissionResult.SynchronousError(err.rpcStatus()))
      }
    }

  override def validateDar(dar: ByteString, darName: String)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    withSpan("CantonSyncService.validateDar") { implicit traceContext => _span =>
      if (!isActive()) {
        logger.debug(s"Rejecting DAR validation request on passive replica.")
        Future.successful(SyncServiceError.Synchronous.PassiveNode)
      } else {
        packageService.value
          .validateDar(dar, darName)
          .map(_ => SubmissionResult.Acknowledged)
          .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
          .valueOr(err => SubmissionResult.SynchronousError(err.rpcStatus()))
      }
    }

  override def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    packageService.value.getLfArchive(packageId)

  override def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    packageService.value.listPackages()

  override def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata = packageService.value.packageMetadataView.getSnapshot

  /** Executes ordered sequence of steps to recover any state that might have been lost if the participant previously
    * crashed. Needs to be invoked after the input stores have been created, but before they are made available to
    * dependent components.
    */
  private def recoverParticipantNodeState()(implicit traceContext: TraceContext): Unit = {
    // also resume pending party notifications
    val resumePendingF = partyNotifier.resumePending()

    parameters.processingTimeouts.unbounded.await(
      "Wait for party-notifier recovery to finish"
    )(resumePendingF)
  }

  def initializeState()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Invoke crash recovery or initialize active participant")

    // Important to invoke recovery before we do anything else with persisted stores.
    recoverParticipantNodeState()

    // Publish the init event that will increase the offset of the participant. Thus, the ledger api server will
    // not return responses that contain an offset being before the ledger begin. Only do so on brand new ledgers
    // without preexisting events.
    logger.debug("Publishing init event if ledger is brand new")
    parameters.processingTimeouts.default
      .await("Publish init event if ledger is brand new")(
        participantNodeEphemeralState.participantEventPublisher.publishInitNeededUpstreamOnlyIfFirst
          .onShutdown(logger.debug("Aborted publishing of init due to shutdown"))
      )
  }

  /** Returns the ready domains this sync service is connected to. */
  def readyDomains: Map[DomainAlias, (DomainId, SubmissionReady)] =
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

  def lookupTopologyClient(domainId: DomainId): Option[DomainTopologyClientWithInit] =
    connectedDomainsMap.get(domainId).map(_.topologyClient)

  /** Adds a new domain to the sync service's configuration.
    *
    * NOTE: Does not automatically connect the sync service to the new domain.
    *
    * @param config The domain configuration.
    * @return Error or unit.
    */
  def addDomain(
      config: DomainConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      _ <- validateSequencerConnection(config, sequencerConnectionValidation)
      _ <- domainConnectionConfigStore
        .put(config, DomainConnectionConfigStore.Active)
        .leftMap(e => SyncServiceError.SyncServiceAlreadyAdded.Error(e.alias): SyncServiceError)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  private def validateSequencerConnection(
      config: DomainConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    sequencerInfoLoader
      .validateSequencerConnection(
        config.domain,
        config.domainId,
        config.sequencerConnections,
        sequencerConnectionValidation,
      )
      .leftMap(SyncServiceError.SyncServiceInconsistentConnectivity.Error(_): SyncServiceError)

  /** Modifies the settings of the sync-service's configuration
    *
    * NOTE: This does not automatically reconnect the sync service.
    */
  def modifyDomain(
      config: DomainConnectionConfig,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      _ <- validateSequencerConnection(config, sequencerConnectionValidation)
      _ <- domainConnectionConfigStore
        .replace(config)
        .leftMap(e => SyncServiceError.SyncServiceUnknownDomain.Error(e.alias): SyncServiceError)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  /** Migrates contracts from a source domain to target domain by re-associating them in the participant's persistent store.
    * Prune some of the domain stores after the migration.
    *
    * The migration only starts when certain preconditions are fulfilled:
    * - the participant is disconnected from the source and target domain
    * - there are neither in-flight submissions nor dirty requests
    *
    * You can force the migration in case of in-flight transactions but it may lead to a ledger fork.
    * Consider:
    *  - Transaction involving participants P1 and P2 that create a contract c
    *  - P1 migrates (D1 -> D2) when processing is done, P2 when it is in-flight
    *  - Final state:
    *    - P1 has the contract on D2 (it was created and migrated)
    *    - P2 does have the contract because it will not process the mediator verdict
    *
    *  Instead of forcing a migration when there are in-flight transactions reconnect all participants to the source domain,
    *  halt activity and let the in-flight transactions complete or time out.
    *
    *  Using the force flag should be a last resort, that is for disaster recovery when the source domain is unrecoverable.
    */
  def migrateDomain(
      source: DomainAlias,
      target: DomainConnectionConfig,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def allDomainsMustBeOffline(): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
      connectedDomainsMap.toSeq.map(_._2.domainHandle.domainAlias) match {
        case Nil =>
          EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())

        case aliases =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            SyncServiceError.SyncServiceDomainsMustBeOffline.Error(aliases)
          )
      }
    for {
      _ <- allDomainsMustBeOffline()

      targetDomainInfo <- migrationService.isDomainMigrationPossible(source, target, force = force)

      _ <-
        connectQueue.executeEUS(
          migrationService
            .migrateDomain(source, target, targetDomainInfo.domainId)
            .leftMap[SyncServiceError](
              SyncServiceError.SyncServiceMigrationError(source, target.domain, _)
            ),
          "migrate domain",
        )

      _ <- purgeDeactivatedDomain(source)
    } yield ()
  }

  /* Verify that specified domain has inactive status and prune sync domain stores.
   */
  def purgeDeactivatedDomain(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    for {
      domainId <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .domainIdForAlias(domain)
          .toRight(SyncServiceError.SyncServiceUnknownDomain.Error(domain))
      )
      _ = logger.info(
        s"Purging deactivated domain with alias $domain with domain id $domainId"
      )
      _ <-
        pruningProcessor
          .purgeInactiveDomain(domainId)
          .transform(
            pruningErrorToCantonError(_).leftMap(
              SyncServicePurgeDomainError(domain, _): SyncServiceError
            )
          )
    } yield ()

  /** Reconnect to all configured domains that have autoStart = true */
  def reconnectDomains(
      ignoreFailures: Boolean,
      mustBeActive: Boolean = true,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Seq[DomainAlias]] =
    if (isActive() || !mustBeActive)
      connectQueue.executeEUS(
        performReconnectDomains(ignoreFailures),
        "reconnect domains",
      )
    else {
      logger.info("Not reconnecting to domains as instance is passive")
      EitherT.leftT(SyncServiceError.SyncServicePassiveReplica.Error())
    }

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
            succeeded <- performDomainConnectionOrHandshake(
              con,
              connectDomain = ConnectDomain.ReconnectDomains,
            ).transform {
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
                    clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava),
                    ConnectDomain.Connect,
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
                  logger.error(s"Failed to disconnect from domains: $failures")
                }
                Left(err)
              case Right(_) => Right(true)
            }
            res <- go(if (succeeded) connected :+ con else connected, rest)
          } yield res
      }

    def startDomains(domains: Seq[DomainAlias]): EitherT[Future, SyncServiceError, Unit] = {
      // we need to start all domains concurrently in order to avoid the reassignment processing
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
            Left(SyncServiceError.SyncServiceStartupError.CombinedStartError(lst))
        }
      })
    }

    val connectedDomains =
      connectedDomainsMap.keys.to(LazyList).mapFilter(aliasManager.aliasForDomainId).toSet

    def shouldConnectTo(config: StoredDomainConnectionConfig): Boolean =
      config.status.isActive && !config.config.manualConnect && !connectedDomains.contains(
        config.config.domain
      )

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
          s"Successfully re-connected to a subset of domains $connected, failed to connect to ${configs.toSet -- connected.toSet}"
        )
      else
        logger.info(s"Successfully re-connected to domains $connected")
      connected
    }
  }

  private def startDomain(alias: DomainAlias, syncDomain: SyncDomain)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncServiceError, Unit] =
    EitherT(syncDomain.startFUS())
      .leftMap(error => SyncServiceError.SyncServiceStartupError.InitError(alias, error))
      .onShutdown(
        Left(
          SyncServiceError.SyncServiceStartupError
            .InitError(alias, AbortedDueToShutdownError("Aborted due to shutdown"))
        )
      )

  /** Connect the sync service to the given domain.
    * This method makes sure there can only be one connection in progress at a time.
    */
  def connectDomain(
      domainAlias: DomainAlias,
      keepRetrying: Boolean,
      connectDomain: ConnectDomain,
  )(implicit
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
        attemptDomainConnection(
          domainAlias,
          keepRetrying = keepRetrying,
          initial = initial,
          connectDomain = connectDomain,
        )
      }

  private def attemptDomainConnection(
      domainAlias: DomainAlias,
      keepRetrying: Boolean,
      initial: Boolean,
      connectDomain: ConnectDomain,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Boolean] =
    connectQueue.executeEUS(
      if (keepRetrying && !attemptReconnect.isDefinedAt(domainAlias)) {
        EitherT.rightT[FutureUnlessShutdown, SyncServiceError](false)
      } else {
        performDomainConnectionOrHandshake(
          domainAlias,
          connectDomain,
        ).transform {
          case Left(SyncServiceError.SyncServiceFailedDomainConnection(_, err))
              if keepRetrying && err.retryable.nonEmpty =>
            if (initial)
              logger.warn(s"Initial connection attempt to $domainAlias failed with ${err.code
                  .toMsg(err.cause, traceContext.traceId, limit = None)}. Will keep on trying.")
            else
              logger.info(
                s"Initial connection attempt to $domainAlias failed. Will keep on trying."
              )
            scheduleReconnectAttempt(
              clock.now.plus(parameters.sequencerClient.startupConnectionRetryDelay.asJava),
              ConnectDomain.Connect,
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

  private def scheduleReconnectAttempt(
      timestamp: CantonTimestamp,
      connectDomain: ConnectDomain,
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
        val domainAlias = item.alias
        logger.debug(s"Starting background reconnect attempt for $domainAlias")
        EitherTUtil.doNotAwaitUS(
          attemptDomainConnection(
            item.alias,
            keepRetrying = true,
            initial = false,
            connectDomain = connectDomain,
          ),
          s"Background reconnect to $domainAlias",
        )
      }
      nextO.foreach(scheduleReconnectAttempt(_, connectDomain))
    }

    clock.scheduleAt(reconnectAttempt, timestamp).discard
  }

  def domainConnectionConfigByAlias(
      domainAlias: DomainAlias
  ): EitherT[Future, MissingConfigForAlias, StoredDomainConnectionConfig] =
    EitherT.fromEither[Future](domainConnectionConfigStore.get(domainAlias))

  private def performDomainConnectionOrHandshake(
      domainAlias: DomainAlias,
      connectDomain: ConnectDomain,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    connectDomain match {
      case ConnectDomain.HandshakeOnly =>
        performDomainHandshake(domainAlias, skipStatusCheck = skipStatusCheck)
      case _ =>
        performDomainConnection(
          domainAlias,
          startSyncDomain = connectDomain.startSyncDomain,
          skipStatusCheck = skipStatusCheck,
        )
    }

  /** Perform handshake with the given domain. */
  private def performDomainHandshake(
      domainAlias: DomainAlias,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    if (aliasManager.domainIdForAlias(domainAlias).exists(connectedDomainsMap.contains)) {
      logger.debug(s"Domain ${domainAlias.unwrap} already registered")
      EitherT.rightT(())
    } else {

      logger.debug(s"About to perform handshake with domain: ${domainAlias.unwrap}")
      for {
        domainConnectionConfig <- domainConnectionConfigByAlias(domainAlias)
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[SyncServiceError] { case MissingConfigForAlias(alias) =>
            SyncServiceError.SyncServiceUnknownDomain.Error(alias)
          }
        // do not connect to a domain that is not active
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          domainConnectionConfig.status.isActive || skipStatusCheck,
          SyncServiceError.SyncServiceDomainIsNotActive
            .Error(domainAlias, domainConnectionConfig.status): SyncServiceError,
        )
        _ = logger.debug(
          s"Performing handshake with domain with config: ${domainConnectionConfig.config}"
        )
        domainHandle <- EitherT(domainRegistry.connect(domainConnectionConfig.config))
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(domainAlias, err)
          )

        _ = domainHandle.close()
      } yield ()
    }

  /** Connect the sync service to the given domain. */
  private def performDomainConnection(
      domainAlias: DomainAlias,
      startSyncDomain: Boolean,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    def connect(
        config: DomainConnectionConfig
    ): EitherT[FutureUnlessShutdown, SyncServiceError, DomainHandle] =
      EitherT(domainRegistry.connect(config)).leftMap(err =>
        SyncServiceError.SyncServiceFailedDomainConnection(domainAlias, err)
      )

    def handleCloseDegradation(syncDomain: SyncDomain, fatal: Boolean)(err: CantonError) =
      if (fatal && parameters.exitOnFatalFailures) {
        FatalError.exitOnFatalError(err, logger)
      } else {
        // If the error is not fatal or the crash on fatal failures flag is off, then we report the unhealthy state and disconnect from the domain
        syncDomain.failureOccurred(err)
        disconnectDomain(domainAlias)
      }

    if (aliasManager.domainIdForAlias(domainAlias).exists(connectedDomainsMap.contains)) {
      logger.debug(s"Already connected to domain: ${domainAlias.unwrap}")
      resolveReconnectAttempts(domainAlias)
      EitherT.rightT(())
    } else {

      logger.debug(s"About to connect to domain: ${domainAlias.unwrap}")
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
        _ = logger.debug(s"Connecting to domain with config: ${domainConnectionConfig.config}")
        domainHandle <- connect(domainConnectionConfig.config)

        domainId = domainHandle.domainId
        domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)
        persistent = domainHandle.domainPersistentState

        domainCrypto = syncCrypto.tryForDomain(domainId, domainHandle.staticParameters)

        ephemeral <- EitherT.right[SyncServiceError](
          FutureUnlessShutdown.outcomeF(
            syncDomainStateFactory
              .createFromPersistent(
                persistent,
                ledgerApiIndexer.asEval,
                participantNodeEphemeralState,
                () => {
                  val tracker = DomainTimeTracker(
                    domainConnectionConfig.config.timeTracker,
                    clock,
                    domainHandle.sequencerClient,
                    domainHandle.staticParameters.protocolVersion,
                    timeouts,
                    domainLoggerFactory,
                  )
                  domainHandle.topologyClient.setDomainTimeTracker(tracker)
                  tracker
                },
                domainMetrics,
                parameters.cachingConfigs.sessionKeyCacheConfig,
                participantId,
              )
          )
        )

        missingKeysAlerter = new MissingKeysAlerter(
          participantId,
          domainId,
          domainHandle.topologyClient,
          domainCrypto.crypto.cryptoPrivateStore,
          domainLoggerFactory,
        )

        syncDomain = syncDomainFactory.create(
          domainId,
          domainHandle,
          participantId,
          engine,
          parameters,
          participantNodePersistentState,
          persistent,
          ephemeral,
          packageService,
          domainCrypto,
          identityPusher,
          domainHandle.topologyFactory
            .createTopologyProcessorFactory(
              domainHandle.staticParameters,
              partyNotifier,
              missingKeysAlerter,
              domainHandle.topologyClient,
              ephemeral.recordOrderPublisher,
            ),
          missingKeysAlerter,
          reassignmentCoordination,
          participantNodeEphemeralState.inFlightSubmissionTracker,
          commandProgressTracker,
          clock,
          domainMetrics,
          futureSupervisor,
          domainLoggerFactory,
          testingConfig,
        )

        _ = syncDomainHealth.set(syncDomain)
        _ = ephemeralHealth.set(syncDomain.ephemeral)
        _ = sequencerClientHealth.set(syncDomain.sequencerClient.healthComponent)
        acp <- EitherT.right[SyncServiceError](syncDomain.acsCommitmentProcessor)
        _ = acsCommitmentProcessorHealth.set(acp.healthComponent)
        _ = syncDomain.resolveUnhealthy()

        _ = connectedDomainsMap += (domainId -> syncDomain)

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

      def disconnectOn(): Unit =
        // only invoke domain disconnect if we actually got so far that the domain-id has been read from the remote node
        if (aliasManager.domainIdForAlias(domainAlias).nonEmpty)
          performDomainDisconnect(
            domainAlias
          ).discard // Ignore Lefts because we don't know to what extent the connection succeeded.

      def handleOutcome(
          outcome: UnlessShutdown[Either[SyncServiceError, Unit]]
      ): UnlessShutdown[Either[SyncServiceError, Unit]] =
        outcome match {
          case x @ UnlessShutdown.Outcome(Right(())) =>
            aliasManager.domainIdForAlias(domainAlias).foreach { domainId =>
              connectionListeners.get().foreach(_(Traced(domainId)))
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

  def logout(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      domainId <- EitherT.fromOption[FutureUnlessShutdown](
        aliasManager.domainIdForAlias(domainAlias),
        Status.INVALID_ARGUMENT.withDescription(
          s"The domain with alias ${domainAlias.unwrap} is unknown."
        ),
      )
      _ <- connectedDomainsMap
        .get(domainId)
        .fold(EitherT.pure[FutureUnlessShutdown, Status] {
          logger.info(show"Nothing to do, as we are not connected to $domainAlias")
          ()
        })(syncDomain => syncDomain.logout())
    } yield ()

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

  /** prepares a domain connection for migration: connect and wait until the topology state has been pushed
    * so we don't deploy against an empty domain
    */
  private def prepareDomainConnectionForMigration(
      aliasT: Traced[DomainAlias]
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = aliasT.withTraceContext {
    implicit tx => alias =>
      logger.debug(s"Preparing connection to $alias for migration")
      (for {
        _ <- performDomainConnectionOrHandshake(
          alias,
          ConnectDomain.Connect,
          skipStatusCheck = true,
        )
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
      _ <- FutureUnlessShutdown.outcomeF(domainConnectionConfigStore.refreshCache())
      _ <- resourceManagementService.refreshCache()
    } yield ()

  override def onClosed(): Unit = {
    val instances = Seq(
      connectQueue,
      migrationService,
      repairService,
      pruningProcessor,
    ) ++ syncCrypto.ips.allDomains.toSeq ++ connectedDomainsMap.values.toSeq ++ Seq(
      domainRouter,
      domainRegistry,
      participantNodeEphemeralState.inFlightSubmissionTracker,
      domainConnectionConfigStore,
      syncDomainPersistentStateManager,
      // As currently we stop the persistent state in here as a next step,
      // and as we need the indexer to terminate before the persistent state and after the sources which are pushing to the indexing queue(sync domains, inFlightSubmissionTracker etc),
      // we need to terminate the indexer right here
      (() => ledgerApiIndexer.closeCurrent()): AutoCloseable,
      participantNodePersistentState.value,
      syncDomainHealth,
      ephemeralHealth,
      sequencerClientHealth,
      acsCommitmentProcessorHealth,
    )

    Lifecycle.close(instances*)(logger)
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

      /* @param domain For unassignment this should be the source domain, for assignment this is the target domain
       * @param remoteDomain For unassignment this should be the target domain, for assignment this is the source domain
       */
      def doReassignment[E <: ReassignmentProcessorError, T](
          domain: DomainId,
          remoteDomain: DomainId,
      )(
          reassign: SyncDomain => EitherT[Future, E, FutureUnlessShutdown[T]]
      )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
        for {
          syncDomain <- EitherT.fromOption[Future](
            readySyncDomainById(domain),
            ifNone = RequestValidationErrors.InvalidArgument
              .Reject(s"Domain ID not found: $domain"): DamlError,
          )
          _ <- reassign(syncDomain)
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

      def getProtocolVersion(domainId: DomainId): Future[ProtocolVersion] =
        protocolVersionGetter(Traced(domainId)) match {
          case Some(protocolVersion) => Future.successful(protocolVersion)
          case None =>
            Future.failed(
              RequestValidationErrors.InvalidArgument
                .Reject(s"Domain ID's protocol version not found: $domainId")
                .asGrpcError
            )
        }

      reassignmentCommand match {
        case unassign: ReassignmentCommand.Unassign =>
          for {
            targetProtocolVersion <- getProtocolVersion(unassign.targetDomain.unwrap).map(Target(_))
            submissionResult <- doReassignment(
              domain = unassign.sourceDomain.unwrap,
              remoteDomain = unassign.targetDomain.unwrap,
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
                targetDomain = unassign.targetDomain,
                targetProtocolVersion = targetProtocolVersion,
              )
            )
          } yield submissionResult

        case assign: ReassignmentCommand.Assign =>
          doReassignment(
            domain = assign.targetDomain.unwrap,
            remoteDomain = assign.sourceDomain.unwrap,
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
              reassignmentId = ReassignmentId(assign.sourceDomain, assign.unassignId),
            )
          )
      }
    }.asJava
  }

  override def getConnectedDomains(
      request: WriteService.ConnectedDomainRequest
  )(implicit traceContext: TraceContext): Future[WriteService.ConnectedDomainResponse] = {
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
      .collect {
        case (domainAlias, (domainId, submissionReady)) if submissionReady.unwrap =>
          for {
            topology <- getSnapshot(domainAlias, domainId)
            partyWithAttributes <- topology.hostedOn(
              Set(request.party),
              participantId = request.participantId.getOrElse(participantId),
            )
          } yield partyWithAttributes
            .get(request.party)
            .map(attributes =>
              ConnectedDomainResponse.ConnectedDomain(
                domainAlias,
                domainId,
                attributes.permission,
              )
            )
      }.toSeq

    Future.sequence(result).map(_.flatten).map(ConnectedDomainResponse.apply)
  }

  def incompleteReassignmentData(
      validAt: GlobalOffset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[List[IncompleteReassignmentData]] =
    syncDomainPersistentStateManager.getAll.values.toList
      .parTraverse {
        _.reassignmentStore.findIncomplete(
          sourceDomain = None,
          validAt = validAt,
          stakeholders = NonEmpty.from(stakeholders),
          limit = NonNegativeInt.maxValue,
        )
      }
      .map(_.flatten)

  override def incompleteReassignmentOffsets(
      validAt: Offset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Vector[Offset]] =
    UpstreamOffsetConvert
      .toGlobalOffset(validAt)
      .fold(
        error => Future.failed(new IllegalArgumentException(error)),
        incompleteReassignmentData(_, stakeholders).map(
          _.map(
            _.reassignmentEventGlobalOffset.globalOffset
              .pipe(UpstreamOffsetConvert.fromGlobalOffset)
          ).toVector
        ),
      )
}

object CantonSyncService {
  sealed trait ConnectDomain extends Product with Serializable {
    def startSyncDomain: Boolean

    // Whether the domain is added in the `connectedDomainsMap` map
    def markDomainAsConnected: Boolean
  }
  object ConnectDomain {
    // Normal use case: do everything
    case object Connect extends ConnectDomain {
      override def startSyncDomain: Boolean = true

      override def markDomainAsConnected: Boolean = true
    }

    /*
    This is used with reconnectDomains.
    Because of the comment
      we need to start all domains concurrently in order to avoid the reassignment processing
    then we need to be able to delay starting the sync domain.
     */
    case object ReconnectDomains extends ConnectDomain {
      override def startSyncDomain: Boolean = false

      override def markDomainAsConnected: Boolean = true
    }

    /*
      Register the domain
      We also attempt to connect by default.
     */
    case object Register extends ConnectDomain {
      override def startSyncDomain: Boolean = true

      override def markDomainAsConnected: Boolean = true
    }

    /*
      Used when we only want to do the handshake (get the domain parameters) and do not connect to the domain.
      Use case: major upgrade for early mainnet (we want to be sure we don't process any transaction before
      the ACS is imported).
     */
    case object HandshakeOnly extends ConnectDomain {
      override def startSyncDomain: Boolean = false

      override def markDomainAsConnected: Boolean = false
    }
  }

  trait Factory[+T <: CantonSyncService] {
    def create(
        participantId: ParticipantId,
        domainRegistry: DomainRegistry,
        domainConnectionConfigStore: DomainConnectionConfigStore,
        domainAliasManager: DomainAliasManager,
        participantNodePersistentState: Eval[ParticipantNodePersistentState],
        participantNodeEphemeralState: ParticipantNodeEphemeralState,
        syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
        packageService: Eval[PackageService],
        partyOps: PartyOps,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        engine: Engine,
        commandProgressTracker: CommandProgressTracker,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        pruningProcessor: PruningProcessor,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        exitOnFatalFailures: Boolean,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
        ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    )(implicit ec: ExecutionContextExecutor, mat: Materializer, tracer: Tracer): T
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
        packageService: Eval[PackageService],
        partyOps: PartyOps,
        identityPusher: ParticipantTopologyDispatcher,
        partyNotifier: LedgerServerPartyNotifier,
        syncCrypto: SyncCryptoApiProvider,
        engine: Engine,
        commandProgressTracker: CommandProgressTracker,
        syncDomainStateFactory: SyncDomainEphemeralStateFactory,
        storage: Storage,
        clock: Clock,
        resourceManagementService: ResourceManagementService,
        cantonParameterConfig: ParticipantNodeParameters,
        indexedStringStore: IndexedStringStore,
        pruningProcessor: PruningProcessor,
        schedulers: Schedulers,
        metrics: ParticipantMetrics,
        exitOnFatalFailures: Boolean,
        sequencerInfoLoader: SequencerInfoLoader,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
        testingConfig: TestingConfigInternal,
        ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    )(implicit
        ec: ExecutionContextExecutor,
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
        partyOps,
        identityPusher,
        partyNotifier,
        syncCrypto,
        pruningProcessor,
        engine,
        commandProgressTracker,
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
        testingConfig,
        ledgerApiIndexer,
      )
  }
}
