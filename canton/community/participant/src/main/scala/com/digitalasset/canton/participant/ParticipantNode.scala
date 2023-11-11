// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.InitConfigBase
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.{CommunityCryptoFactory, CryptoPureApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.health.{ComponentStatus, HealthService}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.admin.{
  PackageDependencyResolver,
  PackageOps,
  PackageOpsImpl,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.{
  DomainAliasResolution,
  DomainConnectionConfig as CantonDomainConnectionConfig,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.scheduler.{
  ParticipantSchedulersParameters,
  SchedulersWithParticipantPruning,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.topology.ParticipantTopologyManager.PostInitCallbacks
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  ParticipantTopologyDispatcherCommon,
  ParticipantTopologyManager,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.ServerServiceDefinition

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class ParticipantNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ],
    engine: Engine,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    setStartableStoppableLedgerApiAndCantonServices: (
        StartableStoppableLedgerApiServer,
        StartableStoppableLedgerApiDependentServices,
    ) => Unit,
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithParticipantPruning] =
      _ => Future.successful(SchedulersWithParticipantPruning.noop),
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    private[canton] val persistentStateFactory: ParticipantNodePersistentStateFactory,
    skipRecipientsCheck: Boolean,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapBase[
      ParticipantNode,
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments)
    with ParticipantNodeBootstrapCommon {

  /** per session created admin token for in-process connections to ledger-api */
  val adminToken: CantonAdminToken = config.ledgerApi.adminToken.fold(
    CantonAdminToken.create(crypto.value.pureCrypto)
  )(token => CantonAdminToken(secret = token))

  override def config: LocalParticipantConfig = arguments.config

  override protected def connectionPoolForParticipant: Boolean = true

  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService(
      "participant",
      logger,
      timeouts,
      criticalDependencies = Seq(storage),
      // The sync service won't be reporting Ok until the node is initialized, but that shouldn't prevent traffic from
      // reaching the node
      softDependencies =
        Seq(syncDomainHealth, syncDomainEphemeralHealth, syncDomainSequencerClientHealth),
    )

  private val packageDependencyResolver =
    new PackageDependencyResolver(
      DamlPackageStore(storage, futureSupervisor, parameterConfig, loggerFactory),
      arguments.parameterConfig.processingTimeouts,
      loggerFactory,
    )

  private val topologyManager =
    new ParticipantTopologyManager(
      clock,
      authorizedTopologyStore,
      crypto.value,
      packageDependencyResolver,
      parameterConfig.processingTimeouts,
      config.parameters.initialProtocolVersion.unwrap,
      loggerFactory,
      futureSupervisor,
    )
  private val partyMetadataStore =
    PartyMetadataStore(storage, parameterConfig.processingTimeouts, loggerFactory)
  // add participant node topology manager
  startTopologyManagementWriteService(topologyManager)

  override protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    withNewTraceContext { implicit traceContext =>
      val protocolVersion = config.parameters.initialProtocolVersion.unwrap

      for {
        // create keys
        namespaceKey <- CantonNodeBootstrapCommon
          .getOrCreateSigningKey(crypto.value)(
            s"$name-namespace"
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        signingKey <- CantonNodeBootstrapCommon
          .getOrCreateSigningKey(crypto.value)(
            s"$name-signing"
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        encryptionKey <- CantonNodeBootstrapCommon
          .getOrCreateEncryptionKey(crypto.value)(
            s"$name-encryption"
          )
          .mapK(FutureUnlessShutdown.outcomeK)

        // create id
        identifierName = initConfigBase.identity
          .flatMap(_.nodeIdentifier.identifierName)
          .getOrElse(name.unwrap)
        identifier <- EitherT
          .fromEither[FutureUnlessShutdown](Identifier.create(identifierName))
          .leftMap(err => s"Failed to convert participant name to identifier: $err")
        uid = UniqueIdentifier(
          identifier,
          Namespace(namespaceKey.fingerprint),
        )
        nodeId = NodeId(uid)

        // init topology manager
        participantId = ParticipantId(uid)
        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          NamespaceDelegation(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          ),
          protocolVersion,
        )
        // avoid a race condition with admin-workflows and only kick off the start once the namespace certificate is registered
        _ <- initialize(nodeId)

        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          OwnerToKeyMapping(participantId, signingKey),
          protocolVersion,
        )
        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          OwnerToKeyMapping(participantId, encryptionKey),
          protocolVersion,
        )

        // finally, we store the node id, which means that the node will not be auto-initialised next time when we start
        _ <- storeId(nodeId).mapK(FutureUnlessShutdown.outcomeK)
      } yield ()
    }

  override protected def setPostInitCallbacks(
      sync: CantonSyncService
  ): Unit = {
    // provide the idm a handle to synchronize package vettings
    topologyManager.setPostInitCallbacks(new PostInitCallbacks {

      override def clients(): Seq[DomainTopologyClient] =
        sync.readyDomains.values.toList.mapFilter { case (domainId, _) =>
          ips.forDomain(domainId)
        }

      override def partyHasActiveContracts(partyId: PartyId)(implicit
          traceContext: TraceContext
      ): Future[Boolean] = {
        sync.partyHasActiveContracts(partyId)
      }
    })
  }

  override def initialize(id: NodeId): EitherT[FutureUnlessShutdown, String, Unit] =
    startInstanceUnlessClosing {

      val participantId: ParticipantId = ParticipantId(id.identity)
      topologyManager.setParticipantId(participantId)

      val componentFactory = new ParticipantComponentBootstrapFactory {
        override def createSyncDomainAndTopologyDispatcher(
            aliasResolution: DomainAliasResolution,
            indexedStringStore: IndexedStringStore,
        ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcherCommon) = {
          val manager = new SyncDomainPersistentStateManagerOld(
            participantId,
            aliasResolution,
            storage,
            indexedStringStore,
            parameters,
            crypto.value.pureCrypto,
            clock,
            futureSupervisor,
            loggerFactory,
          )
          val topologyDispatcher =
            new ParticipantTopologyDispatcher(
              topologyManager,
              participantId,
              manager,
              crypto.value,
              clock,
              parameterConfig.processingTimeouts,
              loggerFactory,
            )
          (manager, topologyDispatcher)
        }

        override def createPackageOps(
            manager: SyncDomainPersistentStateManager,
            crypto: SyncCryptoApiProvider,
        ): PackageOps = {
          val client = new StoreBasedTopologySnapshot(
            CantonTimestamp.MaxValue,
            authorizedTopologyStore,
            Map(),
            useStateTxs = true,
            StoreBasedDomainTopologyClient.NoPackageDependencies,
            loggerFactory,
          )
          new PackageOpsImpl(
            participantId,
            client,
            manager,
            topologyManager,
            parameterConfig.initialProtocolVersion,
            loggerFactory,
          )
        }
      }

      val partyNotifierFactory = (eventPublisher: ParticipantEventPublisher) => {
        val partyNotifier = new LedgerServerPartyNotifier(
          participantId,
          eventPublisher,
          partyMetadataStore,
          clock,
          arguments.futureSupervisor,
          mustTrackSubmissionIds = false,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )
        // Notify at participant level if eager notification is configured, else rely on notification via domain.
        if (parameterConfig.partyChangeNotification == PartyNotificationConfig.Eager) {
          topologyManager.addObserver(partyNotifier.attachToIdentityManagerOld())
        }
        partyNotifier
      }

      createParticipantServices(
        participantId,
        crypto.value,
        storage,
        persistentStateFactory,
        engine,
        ledgerApiServerFactory,
        indexedStringStore,
        cantonSyncServiceFactory,
        setStartableStoppableLedgerApiAndCantonServices,
        resourceManagementServiceFactory,
        replicationServiceFactory,
        createSchedulers,
        partyNotifierFactory,
        adminToken,
        topologyManager,
        packageDependencyResolver,
        componentFactory,
        skipRecipientsCheck,
      ).map {
        case (
              partyNotifier,
              sync,
              ephemeralState,
              ledgerApiServer,
              ledgerApiDependentServices,
              schedulers,
              topologyDispatcher,
            ) =>
          new ParticipantNode(
            participantId,
            arguments.metrics,
            config,
            parameterConfig,
            storage,
            clock,
            topologyManager,
            crypto.value.pureCrypto,
            topologyDispatcher,
            partyNotifier,
            ips,
            sync,
            ephemeralState.participantEventPublisher,
            ledgerApiServer,
            ledgerApiDependentServices,
            adminToken,
            recordSequencerInteractions,
            replaySequencerConfig,
            schedulers,
            packageDependencyResolver,
            loggerFactory,
            nodeHealthService.dependencies.map(_.toComponentStatus),
          )
      }
    }

  override def isActive: Boolean = storage.isActive

  override def onClosed(): Unit = {
    Lifecycle.close(
      partyMetadataStore,
      syncDomainHealth,
      syncDomainEphemeralHealth,
      syncDomainSequencerClientHealth,
    )(logger)
    super.onClosed()
  }

  /** All existing domain stores */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override protected def sequencedTopologyStores: Seq[TopologyStore[TopologyStoreId]] =
    this.getNode.toList.flatMap(_.sync.syncDomainPersistentStateManager.getAll.map {
      case (_, state) =>
        // TODO(#14048) chicken and egg issue: we create the manager after we create the topology manager read service
        //   we should make the "stores" getter a mutable atomic reference
        state.topologyStore.asInstanceOf[TopologyStore[TopologyStoreId]]
    })

}

object ParticipantNodeBootstrap {
  val LoggerFactoryKeyName: String = "participant"

  trait Factory[PC <: LocalParticipantConfig, B <: CantonNodeBootstrap[_]] {

    type Arguments =
      CantonNodeBootstrapCommonArguments[PC, ParticipantNodeParameters, ParticipantMetrics]

    protected def createEngine(arguments: Arguments): Engine

    protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService

    protected def createLedgerApiServerFactory(
        arguments: Arguments,
        engine: Engine,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        actorSystem: ActorSystem,
    ): CantonLedgerApiServerFactory

    def create(
        arguments: NodeFactoryArguments[PC, ParticipantNodeParameters, ParticipantMetrics],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, B]
  }

  abstract class CommunityParticipantFactoryCommon[B <: CantonNodeBootstrap[_]]
      extends Factory[CommunityParticipantConfig, B] {

    override protected def createEngine(arguments: Arguments): Engine =
      DAMLe.newEngine(
        uniqueContractKeys = arguments.parameterConfig.uniqueContractKeys,
        enableLfDev = arguments.parameterConfig.devVersionSupport,
        enableStackTraces = arguments.parameterConfig.enableEngineStackTrace,
        enableContractUpgrading = arguments.parameterConfig.enableContractUpgrading,
        iterationsBetweenInterruptions = arguments.parameterConfig.iterationsBetweenInterruptions,
      )

    override protected def createResourceService(
        arguments: Arguments
    )(store: Eval[ParticipantSettingsStore]): ResourceManagementService =
      new ResourceManagementService.CommunityResourceManagementService(
        arguments.config.parameters.warnIfOverloadedFor.map(_.toInternal),
        arguments.metrics,
      )

    protected def createReplicationServiceFactory(
        arguments: Arguments
    )(storage: Storage): ServerServiceDefinition =
      StaticGrpcServices
        .notSupportedByCommunity(
          EnterpriseParticipantReplicationServiceGrpc.SERVICE,
          arguments.loggerFactory,
        )

    override protected def createLedgerApiServerFactory(
        arguments: Arguments,
        engine: Engine,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        actorSystem: ActorSystem,
    ): CantonLedgerApiServerFactory =
      new CantonLedgerApiServerFactory(
        engine = engine,
        clock = arguments.clock,
        testingTimeService = testingTimeService,
        allocateIndexerLockIds = _dbConfig => Option.empty[IndexerLockIds].asRight,
        meteringReportKey = CommunityKey,
        futureSupervisor = arguments.futureSupervisor,
        loggerFactory = arguments.loggerFactory,
        multiDomainEnabled = multiDomainEnabledForLedgerApiServer,
      )

    protected def multiDomainEnabledForLedgerApiServer: Boolean

    protected def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): B

    override def create(
        arguments: NodeFactoryArguments[
          CommunityParticipantConfig,
          ParticipantNodeParameters,
          ParticipantMetrics,
        ],
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, B] =
      arguments
        .toCantonNodeBootstrapCommonArguments(
          new CommunityStorageFactory(arguments.config.storage),
          new CommunityCryptoFactory,
          new CommunityCryptoPrivateStoreFactory,
          new CommunityGrpcVaultServiceFactory,
        )
        .map { arguments =>
          val engine = createEngine(arguments)
          createNode(
            arguments,
            engine,
            createLedgerApiServerFactory(
              arguments,
              engine,
              testingTimeService,
            ),
          )
        }

  }

  object CommunityParticipantFactory
      extends CommunityParticipantFactoryCommon[ParticipantNodeBootstrap] {
    override protected def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrap =
      new ParticipantNodeBootstrap(
        arguments,
        engine,
        CantonSyncService.DefaultFactory,
        (_ledgerApi, _ledgerApiDependentServices) => (),
        createResourceService(arguments),
        createReplicationServiceFactory(arguments),
        persistentStateFactory = ParticipantNodePersistentStateFactory,
        skipRecipientsCheck = false,
        ledgerApiServerFactory = ledgerApiServerFactory,
      )

    override protected def multiDomainEnabledForLedgerApiServer: Boolean = false
  }
}

/** A participant node in the system.
  *
  * The participant node can connect to a number of domains and offers:
  * - the ledger API to its application.
  * - the participant node admin API to its operator.
  *
  * @param id                               participant id
  * @param config                           Participant node configuration [[com.digitalasset.canton.participant.config.LocalParticipantConfig]] parsed
  *                                         * from config file.
  * @param storage                          participant node persistence
  * @param topologyManager                  topology manager
  * @param identityPusher                   identity pusher
  * @param ips                              identity client
  * @param sync                             synchronization service
  * @param eventPublisher                   participant level sync event log for non-domain events
  * @param ledgerApiServer                  ledger api server state
  * @param ledgerApiDependentCantonServices admin workflow services (ping, archive distribution)
  * @param adminToken the admin token required when JWT is enabled on the ledger api
  * @param recordSequencerInteractions If set to `Some(path)`, every sequencer client will record all sends requested and events received to the directory `path`.
  *                              A new recording starts whenever the participant is connected to a domain.
  * @param replaySequencerConfig If set to `Some(replayConfig)`, a sequencer client transport will be used enabling performance tests to replay previously recorded
  *                              requests and received events. See [[sequencing.client.ReplayConfig]] for more details.
  */
class ParticipantNode(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    val nodeParameters: ParticipantNodeParameters,
    storage: Storage,
    override protected val clock: Clock,
    val topologyManager: ParticipantTopologyManager,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcherCommon,
    partyNotifier: LedgerServerPartyNotifier,
    private[canton] val ips: IdentityProvidingServiceClient,
    override private[canton] val sync: CantonSyncService,
    eventPublisher: ParticipantEventPublisher,
    ledgerApiServer: CantonLedgerApiServerWrapper.LedgerApiServerState,
    val ledgerApiDependentCantonServices: StartableStoppableLedgerApiDependentServices,
    val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val schedulers: SchedulersWithParticipantPruning,
    packageDependencyResolver: PackageDependencyResolver,
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends ParticipantNodeCommon(sync)
    with NoTracing {

  override def isActive = sync.isActive()

  /** helper utility used to auto-connect to local domains
    *
    * during startup, we first reconnect to existing domains.
    * subsequently, if requested via a cli argument, we also auto-connect to local domains.
    */
  def autoConnectLocalDomain(config: CantonDomainConnectionConfig)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] = {
    if (sync.isActive()) {
      // check if we already know this domain
      sync.domainConnectionConfigStore
        .get(config.domain)
        .fold(
          _ =>
            for {
              _ <- sync.addDomain(config).mapK(FutureUnlessShutdown.outcomeK)
              _ <- sync.connectDomain(config.domain, keepRetrying = true)
            } yield (),
          _ => EitherTUtil.unitUS,
        )
    } else {
      logger.info("Not auto-connecting to local domains as instance is passive")
      EitherTUtil.unitUS
    }

  }

  def readyDomains: Map[DomainId, Boolean] =
    sync.readyDomains.values.toMap

  override def status: Future[ParticipantStatus] = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port)
    val domains = readyDomains
    val topologyQueues = identityPusher.queueStatus
    Future.successful(
      ParticipantStatus(
        id.uid,
        uptime(),
        ports,
        domains,
        sync.isActive(),
        topologyQueues,
        healthData,
      )
    )
  }

  override def close(): Unit = {
    logger.info("Stopping participant node")
    Lifecycle.close(
      schedulers,
      ledgerApiDependentCantonServices,
      ledgerApiServer,
      identityPusher,
      partyNotifier,
      eventPublisher,
      topologyManager,
      sync,
      packageDependencyResolver,
      storage,
    )(logger)
  }

}
