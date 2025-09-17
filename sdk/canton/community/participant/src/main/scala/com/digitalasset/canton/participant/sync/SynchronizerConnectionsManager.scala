// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.*
import com.digitalasset.canton.health.MutableHealthComponent
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.ledger.participant.state.SyncService.ConnectedSynchronizerResponse
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentCoordination
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.UnknownAlias
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceBecamePassive,
  SyncServiceFailedSynchronizerConnection,
  SyncServiceSynchronizerDisabledUs,
  SyncServiceSynchronizerDisconnect,
  SyncServiceUnknownSynchronizer,
}
import com.digitalasset.canton.participant.sync.SynchronizerConnectionsManager.{
  AttemptReconnect,
  ConnectSynchronizer,
  ConnectedSynchronizers,
  ConnectionListener,
}
import com.digitalasset.canton.participant.synchronizer.*
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason
import com.digitalasset.canton.store.SequencedEventStore.SearchCriterion
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.daml.lf.engine.Engine
import com.google.common.collect.{BiMap, HashBiMap}
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}
import scala.jdk.CollectionConverters.*
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
private[sync] class SynchronizerConnectionsManager(
    participantId: ParticipantId,
    synchronizerRegistry: SynchronizerRegistry,
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    aliasManager: SynchronizerAliasManager,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    syncPersistentStateManager: SyncPersistentStateManager,
    packageService: PackageService,
    identityPusher: ParticipantTopologyDispatcher,
    partyNotifier: LedgerServerPartyNotifier,
    syncCrypto: SyncCryptoApiParticipantProvider,
    engine: Engine,
    commandProgressTracker: CommandProgressTracker,
    syncEphemeralStateFactory: SyncEphemeralStateFactory,
    clock: Clock,
    resourceManagementService: ResourceManagementService,
    parameters: ParticipantNodeParameters,
    connectedSynchronizerFactory: ConnectedSynchronizer.Factory[ConnectedSynchronizer],
    metrics: ParticipantMetrics,
    isActive: () => Boolean,
    declarativeChangeTrigger: () => Unit,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    testingConfig: TestingConfigInternal,
    ledgerApiIndexer: LifeCycleContainer[LedgerApiIndexer],
    connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
)(implicit ec: ExecutionContextExecutor, mat: Materializer, val tracer: Tracer)
    extends FlagCloseable
    with Spanning
    with NamedLogging
    with HasCloseContext {

  import ShowUtil.*

  val connectedSynchronizerHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, ConnectedSynchronizer.healthName, timeouts)
  val ephemeralHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SyncEphemeralState.healthName, timeouts)
  val sequencerClientHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)
  val acsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(loggerFactory, AcsCommitmentProcessor.healthName, timeouts)

  // Listeners to synchronizer connections
  private val connectionListeners = new AtomicReference[List[ConnectionListener]](List.empty)
  def subscribeToConnections(subscriber: ConnectionListener): Unit =
    connectionListeners.updateAndGet(subscriber :: _).discard

  protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  val connectedSynchronizers: ConnectedSynchronizers = new ConnectedSynchronizers()

  connectedSynchronizersLookupContainer.registerDelegate(connectedSynchronizers)

  private val reassignmentCoordination: ReassignmentCoordination =
    ReassignmentCoordination(
      reassignmentTimeProofFreshnessProportion =
        parameters.reassignmentTimeProofFreshnessProportion,
      syncPersistentStateManager = syncPersistentStateManager,
      submissionHandles = connectedSynchronizers.get,
      synchronizerId =>
        connectedSynchronizers
          .get(synchronizerId.unwrap)
          .map(_.ephemeral.reassignmentSynchronizer),
      syncCryptoApi = syncCrypto,
      loggerFactory,
    )(ec)

  private[sync] val connectQueue = {
    val queueName = "sync-service-connect-and-repair-queue"

    new SimpleExecutionQueue(
      queueName,
      futureSupervisor,
      timeouts,
      loggerFactory,
      crashOnFailure = parameters.exitOnFatalFailures,
    )
  }

  private val logicalSynchronizerUpgrade = new LogicalSynchronizerUpgrade(
    synchronizerConnectionConfigStore,
    ledgerApiIndexer,
    syncPersistentStateManager,
    connectQueue,
    connectedSynchronizers,
    connectSynchronizer = (alias: Traced[SynchronizerAlias]) =>
      connectSynchronizer(
        alias.value,
        keepRetrying = true,
        connectSynchronizer = ConnectSynchronizer.Connect,
      )(alias.traceContext),
    disconnectSynchronizer = (alias: Traced[SynchronizerAlias]) =>
      disconnectSynchronizer(alias.value)(alias.traceContext),
    timeouts,
    loggerFactory,
  )

  // Track synchronizers we would like to "keep on reconnecting until available"
  private val attemptReconnect: TrieMap[SynchronizerAlias, AttemptReconnect] = TrieMap.empty

  private def resolveReconnectAttempts(alias: SynchronizerAlias): Unit =
    attemptReconnect.remove(alias).discard

  // A connected synchronizer is ready if recovery has succeeded
  private[canton] def readyConnectedSynchronizerById(
      synchronizerId: SynchronizerId
  ): Option[ConnectedSynchronizer] =
    connectedSynchronizers.get(synchronizerId).filter(_.ready)

  private[canton] def connectedSynchronizerForAlias(
      alias: SynchronizerAlias
  ): Option[ConnectedSynchronizer] =
    aliasManager
      .synchronizerIdForAlias(alias)
      .flatMap(connectedSynchronizers.get)

  /** Returns the ready synchronizers this sync service is connected to. */
  def readySynchronizers: Map[SynchronizerAlias, (PhysicalSynchronizerId, SubmissionReady)] =
    connectedSynchronizers.snapshot
      .to(LazyList)
      .mapFilter {
        case (id, sync) if sync.ready =>
          aliasManager
            .aliasForSynchronizerId(id.logical)
            .map(_ -> ((sync.psid, sync.readyForSubmission)))
        case _ => None
      }
      .toMap

  /** Lookup a time tracker for the given `synchronizerId`. A time tracker will only be returned if
    * the synchronizer is registered and connected.
    */
  def lookupSynchronizerTimeTracker(
      requestedSynchronizer: Synchronizer
  ): Either[String, SynchronizerTimeTracker] =
    connectedSynchronizers.get(requestedSynchronizer.logical) match {
      case None =>
        s"Not connected to any synchronizer with id ${requestedSynchronizer.logical}".asLeft
      case Some(connectedSynchronizer) =>
        Either.cond(
          requestedSynchronizer.isCompatibleWith(connectedSynchronizer.psid),
          connectedSynchronizer.timeTracker,
          s"Not connected to $requestedSynchronizer but to ${connectedSynchronizer.psid}",
        )
    }

  def lookupTopologyClient(
      psid: PhysicalSynchronizerId
  ): Option[SynchronizerTopologyClientWithInit] =
    connectedSynchronizers.get(psid).map(_.topologyClient)

  /** Reconnect configured synchronizers
    *
    * @param ignoreFailures
    *   If true, a failure will not interrupt reconnects
    * @param isTriggeredManually
    *   True if the call of this method is triggered by an explicit call to the connectivity
    *   service, false if the call of this method is triggered by a node restart or transition to
    *   active.
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
            // This call is synchronized at the call site
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
                // This call is synchronized at the call site

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
              // This call is synchronized at the call site
              performSynchronizerDisconnect(_).discard[Either[SyncServiceError, Unit]]
            )
            Left(SyncServiceError.SyncServiceStartupError.CombinedStartError(lst))
        }
      })
    }

    val connectedAliases: Set[SynchronizerAlias] =
      connectedSynchronizers.snapshot.keys
        .to(LazyList)
        .map(_.logical)
        .mapFilter(aliasManager.aliasForSynchronizerId)
        .toSet

    def shouldConnectTo(config: StoredSynchronizerConnectionConfig): Boolean = {
      val alreadyConnected = connectedAliases.contains(
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
        s"Reconnecting to synchronizers ${configs.map(_.unwrap)}. Already connected: ${connectedSynchronizers.psids}"
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
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Option[PhysicalSynchronizerId]] = {
    logger.debug(s"Trying to connect $participantId to $synchronizerAlias")

    EitherT
      .fromEither[FutureUnlessShutdown](
        getSynchronizerConnectionConfigForAlias(synchronizerAlias, onlyActive = true)
      )
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
  }

  /** Attempt to connect to the synchronizer
    * @return
    *   Left if connection failed in a non-retriable way Right(None)) if connection failed and can
    *   be retried Right(Some(psid)) if connection succeeded
    */
  private def attemptSynchronizerConnection(
      synchronizerAlias: SynchronizerAlias,
      keepRetrying: Boolean,
      initial: Boolean,
      connectSynchronizer: ConnectSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Option[PhysicalSynchronizerId]] =
    connectQueue.executeEUS(
      if (keepRetrying && !attemptReconnect.isDefinedAt(synchronizerAlias)) {
        EitherT.rightT[FutureUnlessShutdown, SyncServiceError](None)
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
            Right(None)
          case Right(psid) =>
            resolveReconnectAttempts(synchronizerAlias)
            Right(Some(psid))
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

  private def updateSynchronizerConnectionConfig(
      psid: PhysicalSynchronizerId,
      config: SynchronizerConnectionConfig,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    synchronizerConnectionConfigStore
      .replace(KnownPhysicalSynchronizerId(psid), config)
      .leftMap[SyncServiceError](err =>
        SyncServiceError.SyncServicePhysicalIdRegistration
          .Error(config.synchronizerAlias, psid, err.message)
      )

  /** MUST be synchronized using the [[connectQueue]]
    */
  def performSynchronizerConnectionOrHandshake(
      synchronizerAlias: SynchronizerAlias,
      connectSynchronizer: ConnectSynchronizer,
      skipStatusCheck: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] =
    connectSynchronizer match {
      case ConnectSynchronizer.HandshakeOnly =>
        performSynchronizerHandshake(
          synchronizerAlias,
          skipStatusCheck = skipStatusCheck,
        )
      case _ =>
        performSynchronizerConnection(
          synchronizerAlias,
          startConnectedSynchronizerProcessing = connectSynchronizer.startConnectedSynchronizer,
          skipStatusCheck = skipStatusCheck,
        )
    }

  /** Checks the node is connected to the synchronizer with the given alias
    * @return
    *   None if the node is not connected and the physical synchronizer id otherwise
    */
  def isConnected(synchronizerAlias: SynchronizerAlias): Option[PhysicalSynchronizerId] =
    for {
      lsid <- aliasManager.synchronizerIdForAlias(synchronizerAlias)
      psid <- connectedSynchronizers.get(lsid).map(_.psid)
    } yield psid

  /** Perform handshake with the given synchronizer.
    * @param synchronizerAlias
    *   Alias of the synchronizer
    * @param skipStatusCheck
    *   If false, check that the connection is active (default).
    */
  private def performSynchronizerHandshake(
      synchronizerAlias: SynchronizerAlias,
      skipStatusCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] =
    isConnected(synchronizerAlias) match {
      case Some(psid) =>
        logger.debug(s"Synchronizer ${synchronizerAlias.unwrap} already registered")
        EitherT.rightT(psid)

      case None =>
        logger.debug(s"About to perform handshake with synchronizer: ${synchronizerAlias.unwrap}")

        for {
          synchronizerConnectionConfig <- EitherT.fromEither[FutureUnlessShutdown](
            getSynchronizerConnectionConfigForAlias(
              synchronizerAlias,
              onlyActive = !skipStatusCheck,
            )
          )
          _ = logger.debug(
            s"Performing handshake with synchronizer with id ${synchronizerConnectionConfig.configuredPSId} and config: ${synchronizerConnectionConfig.config}"
          )
          synchronizerHandleAndUpdatedConfig <- EitherT(
            synchronizerRegistry.connect(
              synchronizerConnectionConfig.config,
              synchronizerConnectionConfig.predecessor,
            )
          )
            .leftMap[SyncServiceError](err =>
              SyncServiceError.SyncServiceFailedSynchronizerConnection(synchronizerAlias, err)
            )
          (synchronizerHandle, updatedConfig) = synchronizerHandleAndUpdatedConfig

          psid = synchronizerHandle.psid
          _ = logger.debug(
            s"Registering id $psid for synchronizer with alias $synchronizerAlias"
          )
          _ <- synchronizerConnectionConfigStore
            .setPhysicalSynchronizerId(synchronizerAlias, psid)
            .leftMap[SyncServiceError](err =>
              SyncServiceError.SyncServicePhysicalIdRegistration
                .Error(synchronizerAlias, psid, err.message)
            )

          _ <- updateSynchronizerConnectionConfig(psid, updatedConfig)

          _ = syncCrypto.remove(psid)
          _ = synchronizerHandle.close()
        } yield psid
    }

  /** Perform a handshake with the given synchronizer.
    * @param psid
    *   the physical synchronizer id of the synchronizer.
    * @return
    */
  def connectToPSIdWithHandshake(
      psid: PhysicalSynchronizerId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] =
    connectQueue.executeEUS(
      if (connectedSynchronizers.isConnected(psid)) {
        logger.debug(s"Already connected to $psid, no need to register $psid")
        EitherT.rightT(psid)
      } else {
        logger.debug(s"About to perform handshake with synchronizer: $psid")

        for {
          synchronizerConnectionConfig <- EitherT.fromEither[FutureUnlessShutdown](
            synchronizerConnectionConfigStore
              .get(psid)
              .leftMap(e =>
                SyncServiceError.SynchronizerRegistration
                  .SuccessorInitializationError(psid, e.message): SyncServiceError
              )
          )

          _ = logger.debug(
            s"Performing handshake with synchronizer with id ${synchronizerConnectionConfig.configuredPSId} and config: ${synchronizerConnectionConfig.config}"
          )
          synchronizerHandleAndUpdatedConfig <- EitherT(
            synchronizerRegistry.connect(
              synchronizerConnectionConfig.config,
              synchronizerConnectionConfig.predecessor,
            )
          )
            .leftMap[SyncServiceError](err =>
              SyncServiceError.SyncServiceFailedSynchronizerConnection(
                synchronizerConnectionConfig.config.synchronizerAlias,
                err,
              )
            )
          (synchronizerHandle, _) = synchronizerHandleAndUpdatedConfig

          _ = syncCrypto.remove(psid)
          _ = synchronizerHandle.close()
        } yield psid
      },
      s"handshake with physical synchronizer $psid",
    )

  /** Connect the sync service to the given synchronizer. */
  private def performSynchronizerConnection(
      synchronizerAlias: SynchronizerAlias,
      startConnectedSynchronizerProcessing: Boolean,
      skipStatusCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] = {
    def connect(
        config: SynchronizerConnectionConfig,
        synchronizerPredecessor: Option[SynchronizerPredecessor],
    ): EitherT[
      FutureUnlessShutdown,
      SyncServiceFailedSynchronizerConnection,
      (SynchronizerHandle, SynchronizerConnectionConfig),
    ] =
      EitherT(synchronizerRegistry.connect(config, synchronizerPredecessor)).leftMap(err =>
        SyncServiceError.SyncServiceFailedSynchronizerConnection(synchronizerAlias, err)
      )

    def handleCloseDegradation(connectedSynchronizer: ConnectedSynchronizer, fatal: Boolean)(
        err: RpcError
    ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
      if (fatal && parameters.exitOnFatalFailures) {
        FatalError.exitOnFatalError(err, logger)
      } else {
        // If the error is not fatal or the crash on fatal failures flag is off, then we report the unhealthy state and disconnect from the synchronizer
        connectedSynchronizer.failureOccurred(err)
        disconnectSynchronizer(synchronizerAlias)
      }

    isConnected(synchronizerAlias) match {
      case Some(psid) =>
        logger.debug(s"Already connected to synchronizer: $synchronizerAlias, $psid")
        resolveReconnectAttempts(synchronizerAlias)
        EitherT.rightT(psid)

      case None =>
        logger.debug(s"About to connect to synchronizer: ${synchronizerAlias.unwrap}")
        val connectedSynchronizerMetrics = metrics.connectedSynchronizerMetrics(synchronizerAlias)

        val ret: EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] = for {

          synchronizerConnectionConfig <- EitherT.fromEither[FutureUnlessShutdown](
            getSynchronizerConnectionConfigForAlias(
              synchronizerAlias,
              onlyActive = !skipStatusCheck,
            )
          )
          _ = logger.debug(
            s"Connecting to synchronizer with id ${synchronizerConnectionConfig.configuredPSId} config: ${synchronizerConnectionConfig.config}"
          )
          synchronizerHandleAndUpdatedConfig <- connect(
            synchronizerConnectionConfig.config,
            synchronizerConnectionConfig.predecessor,
          )
          (synchronizerHandle, updatedConfig) = synchronizerHandleAndUpdatedConfig
          psid = synchronizerHandle.psid

          _ = logger.debug(
            s"Registering id $psid for synchronizer with alias $synchronizerAlias"
          )
          _ <- synchronizerConnectionConfigStore
            .setPhysicalSynchronizerId(synchronizerAlias, psid)
            .leftMap[SyncServiceError](err =>
              SyncServiceError.SyncServicePhysicalIdRegistration
                .Error(synchronizerAlias, psid, err.message)
            )
          _ <- updateSynchronizerConnectionConfig(psid, updatedConfig)

          synchronizerLoggerFactory = loggerFactory.append(
            "synchronizerId",
            psid.toString,
          )
          persistent = synchronizerHandle.syncPersistentState

          synchronizerCrypto = syncCrypto.tryForSynchronizer(
            psid,
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
                synchronizerCrypto,
                ledgerApiIndexer.asEval,
                participantNodePersistentState.map(_.contractStore),
                participantNodeEphemeralState,
                synchronizerConnectionConfig.predecessor,
                () => {
                  val tracker = SynchronizerTimeTracker(
                    synchronizerConnectionConfig.config.timeTracker,
                    clock,
                    synchronizerHandle.sequencerClient,
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
            psid.logical,
            synchronizerHandle.topologyClient,
            synchronizerCrypto.crypto.cryptoPrivateStore,
            synchronizerLoggerFactory,
          )

          sequencerConnectionSuccessorListener = new SequencerConnectionSuccessorListener(
            synchronizerAlias,
            synchronizerHandle.topologyClient,
            synchronizerConnectionConfigStore,
            new HandshakeWithPSId {
              override def performHandshake(
                  psid: PhysicalSynchronizerId
              )(implicit
                  traceContext: TraceContext
              ): EitherT[FutureUnlessShutdown, SyncServiceError, PhysicalSynchronizerId] =
                connectToPSIdWithHandshake(psid)
            },
            automaticallyConnectToUpgradedSynchronizer =
              parameters.automaticallyPerformLogicalSynchronizerUpgrade,
            loggerFactory,
          )

          lsuCallback =
            if (parameters.automaticallyPerformLogicalSynchronizerUpgrade)
              new LogicalSynchronizerUpgradeCallbackImpl(
                psid,
                ephemeral.timeTracker,
                this,
                loggerFactory,
              )
            else LogicalSynchronizerUpgradeCallback.NoOp

          connectedSynchronizer <- EitherT.right(
            connectedSynchronizerFactory.create(
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
                  partyNotifier,
                  missingKeysAlerter,
                  sequencerConnectionSuccessorListener,
                  synchronizerHandle.topologyClient,
                  ephemeral.recordOrderPublisher,
                  lsuCallback,
                  synchronizerHandle.syncPersistentState.sequencedEventStore,
                  synchronizerConnectionConfig.predecessor,
                  ledgerApiIndexer.asEval.value.ledgerApiStore.value,
                ),
              missingKeysAlerter,
              sequencerConnectionSuccessorListener,
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

          _ = connectedSynchronizers.tryAdd(connectedSynchronizer)

          // Start sequencer client subscription only after synchronizer has been added to connectedSynchronizers, e.g. to
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
              EitherTUtil.unitUS[SyncServiceError]
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
                  SyncServiceSynchronizerDisconnect.UnrecoverableError(
                    synchronizerAlias,
                    error.cause,
                  )
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
                SyncServiceSynchronizerDisconnect.UnrecoverableException(
                  synchronizerAlias,
                  exception,
                )
              ).discard
          }
        } yield {
          // remove this one from the reconnect attempt list, as we are successfully connected now
          this.resolveReconnectAttempts(synchronizerAlias)
          declarativeChangeTrigger()
          psid
        }

        def disconnectOn(): Unit =
          // only invoke synchronizer disconnect if we actually got so far that the synchronizer id has been read from the remote node
          if (aliasManager.synchronizerIdForAlias(synchronizerAlias).nonEmpty)
            performSynchronizerDisconnect(
              synchronizerAlias
            ).discard // Ignore Lefts because we don't know to what extent the connection succeeded.

        def handleOutcome(
            outcome: UnlessShutdown[Either[SyncServiceError, PhysicalSynchronizerId]]
        ): UnlessShutdown[Either[SyncServiceError, PhysicalSynchronizerId]] =
          outcome match {
            case x @ UnlessShutdown.Outcome(Right(_: PhysicalSynchronizerId)) =>
              aliasManager.synchronizerIdForAlias(synchronizerAlias).foreach { synchronizerId =>
                connectionListeners.get().foreach(_(Traced(synchronizerId)))
              }
              x
            case UnlessShutdown.AbortedDueToShutdown =>
              disconnectOn()
              UnlessShutdown.AbortedDueToShutdown
            case x @ UnlessShutdown.Outcome(
                  Left(_: SyncServiceError.SynchronizerRegistration.Error)
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
      _ <- connectedSynchronizers
        .get(synchronizerId)
        .fold(EitherT.pure[FutureUnlessShutdown, Status] {
          logger.info(show"Nothing to do, as we are not connected to $synchronizerAlias")
          ()
        })(connectedSynchronizer => connectedSynchronizer.logout())
    } yield ()

  /** MUST be synchronized using the [[connectQueue]]
    */
  def performSynchronizerDisconnect(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Either[SyncServiceError, Unit] = {
    logger.info(show"Disconnecting from $synchronizerAlias")
    (for {
      synchronizerId <- aliasManager.synchronizerIdForAlias(synchronizerAlias)
    } yield {
      val removedO = connectedSynchronizers.psidFor(synchronizerId).flatMap { psid =>
        syncCrypto.remove(psid)
        connectedSynchronizers.remove(psid)
      }
      removedO match {
        case Some(connectedSynchronizer) =>
          logger.info(s"Disconnecting connected synchronizer ${connectedSynchronizer.psid}")
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
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    connectedSynchronizers.lsids.toList
      .mapFilter(aliasManager.aliasForSynchronizerId)
      .distinct
      .parTraverse_(disconnectSynchronizer)

  /** Start the upgrade of the participant to the successor.
    *
    * Prerequisite:
    *   - Time on the current synchronizer has reached the upgrade time.
    *   - Successor is registered.
    *
    * Note: The upgrade involve operations that are retried, so the method can take some time to
    * complete.
    */
  def upgradeSynchronizerTo(
      currentPSId: PhysicalSynchronizerId,
      synchronizerSuccessor: SynchronizerSuccessor,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Starting upgrade from $currentPSId to ${synchronizerSuccessor.psid}")

    for {
      persistentState <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .get(currentPSId)
          .toRight(s"Unable to get persistent state for $currentPSId")
      )

      alias <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .aliasForSynchronizerId(currentPSId.logical)
          .toRight(s"Unable to find alias for synchronizer $currentPSId")
      )

      event <- persistentState.sequencedEventStore
        .find(SearchCriterion.Latest)
        .leftMap(_ => "The sequencer event store is empty. Was the upgrade performed already?")

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        event.timestamp >= synchronizerSuccessor.upgradeTime,
        s"Upgrade time ${synchronizerSuccessor.upgradeTime} not reached: last event in the sequenced event store has timestamp ${event.timestamp}",
      )

      _ <- logicalSynchronizerUpgrade.upgrade(
        alias,
        currentPSId,
        synchronizerSuccessor,
      )

    } yield ()
  }

  // Write health requires the ability to transact, i.e. connectivity to at least one synchronizer and HA-activeness.
  def currentWriteHealth(): HealthStatus = {
    val existsReadySynchronizer = connectedSynchronizers.snapshot.exists { case (_, sync) =>
      sync.ready
    }
    if (existsReadySynchronizer && isActive()) HealthStatus.healthy else HealthStatus.unhealthy
  }

  def computeTotalLoad: Int = connectedSynchronizers.snapshot.foldLeft(0) {
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
    val instances = (connectQueue +: connectedSynchronizers.snapshot.values.toSeq) ++ Seq(
      connectedSynchronizerHealth,
      ephemeralHealth,
      sequencerClientHealth,
      acsCommitmentProcessorHealth,
    )

    LifeCycle.close(instances*)(logger)
  }

  override def toString: String = s"SynchronizerConnectionsManager($participantId)"

  def getConnectedSynchronizers(
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
}

object SynchronizerConnectionsManager {
  type ConnectionListener = Traced[SynchronizerId] => Unit

  sealed trait ConnectSynchronizer extends Product with Serializable {

    /** Whether to start processing for a connected synchronizer. */
    def startConnectedSynchronizer: Boolean

    /** Whether the synchronizer is added in the `connectedSynchronizersMap` map */
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

  private final case class AttemptReconnect(
      alias: SynchronizerAlias,
      last: CantonTimestamp,
      retryDelay: Duration,
      trace: TraceContext,
  ) {
    val earliest: CantonTimestamp = last.plusMillis(retryDelay.toMillis)
  }

  /** Store the connected synchronizers.
    *
    * Updates to this class should be synchronizer via the single execution queue.
    *
    * Invariants:
    *   - Only one PSId per LSId
    *   - Throws if invariants do not hold
    */
  final class ConnectedSynchronizers() extends ConnectedSynchronizersLookup {

    /** These two maps should stay private. Read-only interface is provided by
      * [[ConnectedSynchronizersLookup]]
      */
    private val connected: TrieMap[PhysicalSynchronizerId, ConnectedSynchronizer] = TrieMap()
    private val lsidToPSId: BiMap[SynchronizerId, PhysicalSynchronizerId] = HashBiMap.create

    def get(psid: PhysicalSynchronizerId): Option[ConnectedSynchronizer] = connected.get(psid)
    def get(lsid: SynchronizerId): Option[ConnectedSynchronizer] =
      Option(lsidToPSId.get(lsid)).flatMap(connected.get)

    override def getAcsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
      get(synchronizerId).map(_.persistent.acsInspection)

    override def isConnected(synchronizerId: SynchronizerId): Boolean = get(synchronizerId).nonEmpty

    override def isConnectedToAny: Boolean = connected.nonEmpty

    def lsids: Set[SynchronizerId] = lsidToPSId.keySet().asScala.toSet
    def psids: Set[PhysicalSynchronizerId] = lsidToPSId.values().asScala.toSet
    def snapshot: Map[PhysicalSynchronizerId, ConnectedSynchronizer] = connected.toMap

    def tryAdd(connectedSynchronizer: ConnectedSynchronizer): Unit = {
      val lsid = connectedSynchronizer.psid.logical
      val psid = connectedSynchronizer.psid

      blocking {
        this.synchronized {
          if (connected.isDefinedAt(psid))
            throw new IllegalArgumentException(
              s"Cannot add $psid because the node is already connected to it"
            )

          if (lsidToPSId.containsKey(lsid))
            throw new IllegalArgumentException(
              s"Cannot add $psid because the node is already connected to $lsid"
            )

          connected.addOne(psid -> connectedSynchronizer)
          lsidToPSId.put(lsid, psid).discard
        }
      }
    }

    def remove(psid: PhysicalSynchronizerId): Option[ConnectedSynchronizer] =
      blocking {
        this.synchronized {
          lsidToPSId.remove(psid.logical)
          connected.remove(psid)
        }
      }
  }
}
