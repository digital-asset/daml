// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.instances.future.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.domain.admin.v0.MediatorInitializationServiceGrpc
import com.digitalasset.canton.domain.mediator.admin.gprc.InitializeMediatorRequest
import com.digitalasset.canton.domain.mediator.service.GrpcMediatorInitializationService
import com.digitalasset.canton.domain.mediator.store.{
  MediatorDomainConfiguration,
  MediatorDomainConfigurationStore,
}
import com.digitalasset.canton.domain.mediator.topology.MediatorTopologyManager
import com.digitalasset.canton.domain.metrics.MediatorNodeMetrics
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.health.admin.data.MediatorNodeStatus
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.{Clock, HasUptime}
import com.digitalasset.canton.topology
import com.digitalasset.canton.topology.admin.grpc.GrpcTopologyManagerWriteService
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  TopologyTransactionProcessor,
  TopologyTransactionProcessorCommon,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  DomainTopologyStore,
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{
  MediatorDomainState,
  RequestSide,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{DomainId, MediatorId, NodeId}
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

/** Community mediator Node configuration
  *
  * @param init all nodes must provided a init config however the mediator cannot auto initialize itself so defaults `autoInit` to `false`
  * @param timeTracker configuration for how time is tracked on the connected domain using the sequencer
  */
final case class CommunityMediatorNodeConfig(
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val init: InitConfig = InitConfig(identity = None),
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val parameters: MediatorNodeParameterConfig = MediatorNodeParameterConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topologyX: TopologyXConfig = TopologyXConfig.NotUsed,
) extends MediatorNodeConfigCommon(
      adminApi,
      storage,
      crypto,
      init,
      timeTracker,
      sequencerClient,
      caching,
      parameters,
      monitoring,
    )
    with ConfigDefaults[DefaultPorts, CommunityMediatorNodeConfig] {

  override val nodeTypeName: String = "mediator"

  override def replicationEnabled: Boolean = false

  override def withDefaults(ports: DefaultPorts): CommunityMediatorNodeConfig = {
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorAdminApiPort.setDefaultPort)
  }
}

abstract class MediatorNodeConfigCommon(
    val adminApi: AdminServerConfig,
    val storage: StorageConfig,
    val crypto: CryptoConfig,
    val init: InitConfig,
    val timeTracker: DomainTimeTrackerConfig,
    val sequencerClient: SequencerClientConfig,
    val caching: CachingConfigs,
    val parameters: MediatorNodeParameterConfig,
    val monitoring: NodeMonitoringConfig,
) extends LocalNodeConfig {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteMediatorConfig = RemoteMediatorConfig(adminApi.clientConfig)

  def replicationEnabled: Boolean
}

/** Various parameters for non-standard mediator settings
  *
  * @param dontWarnOnDeprecatedPV if true, then this mediator will not emit a warning when connecting to a sequencer using a deprecated protocol version.
  */
final case class MediatorNodeParameterConfig(
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val devVersionSupport: Boolean = true,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val initialProtocolVersion: ProtocolVersion = ProtocolVersion.latest,
    batching: BatchingConfig = BatchingConfig(),
    caching: CachingConfigs = CachingConfigs(),
) extends ProtocolConfig
    with LocalNodeParametersConfig

final case class MediatorNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters

final case class RemoteMediatorConfig(
    adminApi: ClientConfig
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}
class MediatorNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorNodeMetrics,
    ],
    override protected val replicaManager: MediatorReplicaManager,
    override protected val mediatorRuntimeFactory: MediatorRuntimeFactory,
)(
    implicit executionContext: ExecutionContextIdlenessExecutorService,
    override protected implicit val executionSequencerFactory: ExecutionSequencerFactory,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapBase[
      MediatorNode,
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorNodeMetrics,
    ](
      arguments
    )
    with MediatorNodeBootstrapCommon[MediatorNode, MediatorNodeConfigCommon] {

  protected val topologyManager =
    new MediatorTopologyManager(
      clock,
      authorizedTopologyStore,
      crypto.value,
      timeouts,
      parameters.initialProtocolVersion,
      loggerFactory,
      futureSupervisor,
    )

  startTopologyManagementWriteService(topologyManager)
  protected val sequencedTopologyStore =
    new DomainTopologyStore(storage, timeouts, loggerFactory, futureSupervisor)
  override protected def sequencedTopologyStores
      : Seq[com.digitalasset.canton.topology.store.TopologyStore[TopologyStoreId]] =
    sequencedTopologyStore.get().toList

  protected val domainConfigurationStore =
    MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)

  protected override val supportsReplicaInitialization: Boolean = config.replicationEnabled

  /** Simple async action queue to ensure no initialization functionality runs concurrently avoiding any chance
    * of races.
    */
  private val initializationActionQueue = new SimpleExecutionQueue(
    "mediator-init-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  adminServerRegistry
    .addServiceU(
      topology.admin.v0.TopologyManagerWriteServiceGrpc
        .bindService(
          new GrpcTopologyManagerWriteService(
            topologyManager,
            crypto.value.cryptoPublicStore,
            parameters.initialProtocolVersion,
            loggerFactory,
          ),
          executionContext,
        )
    )
  adminServerRegistry
    .addServiceU(
      MediatorInitializationServiceGrpc
        .bindService(
          new GrpcMediatorInitializationService(handleInitializationRequest, loggerFactory),
          executionContext,
        ),
      true,
    )

  // TODO(#11052) duplicate init methods
  override def initializeWithProvidedId(id: NodeId): EitherT[Future, String, Unit] = EitherT.leftT(
    "This method is disabled for mediator nodes. Please use normal mediator initialization methods"
  )

  /** Generate an identity for the node. */
  override protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // Much like sequencer nodes, mediator nodes cannot initialize their own identity as it is
    // inherently tied to the identity of the domain of which it is operating on behalf of.
    // The domain node will call the mediator with its identity to initialize it as well as
    // providing sufficient identity information and configuration to connect to a sequencer.
    logger.warn(
      "Mediator cannot auto initialize their identity. init.auto-init should be set to false"
    )
    EitherT.pure(())
  }

  /** Handle the initialization request (presumably from the domain node). */
  private def handleInitializationRequest(
      request: InitializeMediatorRequest
  ): EitherT[FutureUnlessShutdown, String, SigningPublicKey] = {
    logger.debug("Handling initialization request")
    // ensure initialization actions aren't run concurrently
    initializationActionQueue.executeE(
      {
        def domainConfiguration(initialKeyFingerprint: Fingerprint): MediatorDomainConfiguration =
          MediatorDomainConfiguration(
            initialKeyFingerprint,
            request.domainId,
            request.domainParameters,
            request.sequencerConnections,
          )

        def storeTopologySnapshot(): EitherT[Future, String, Unit] =
          request.topologyState match {
            case Some(snapshot) =>
              val store = sequencedTopologyStore.initOrGet(DomainStore(request.domainId))
              for {
                cleanSnapshot <- EitherT.fromEither(
                  MediatorNodeBootstrap.checkIfTopologySnapshotIsValid(
                    request.mediatorId,
                    snapshot,
                    request.domainId.unwrap == request.mediatorId.uid,
                  )
                )
                _ = if (cleanSnapshot.result.length != snapshot.result.length)
                  logger.debug(
                    s"Storing initial topology state of length=${cleanSnapshot.result.length} (passed one had length=${snapshot.result.length} into domain store)"
                  )
                _ <- EitherT.right(store.bootstrap(cleanSnapshot))
              } yield ()
            case None => EitherT.pure(())
          }

        def initializeMediator: EitherT[Future, String, SigningPublicKey] =
          for {
            _ <- EitherT.fromEither[Future](
              ProtocolVersionCompatibility.isSupportedByDomainNode(
                parameters,
                request.domainParameters.protocolVersion,
              )
            )
            _ <- storeTopologySnapshot()
            key <- request.signingKeyFingerprint
              .map(CantonNodeBootstrapCommon.getOrCreateSigningKeyByFingerprint(crypto.value))
              .getOrElse(
                CantonNodeBootstrapCommon.getOrCreateSigningKey(crypto.value)(s"$name-signing")
              )
            _ <- domainConfigurationStore
              .saveConfiguration(domainConfiguration(key.fingerprint))
              .leftMap(err => s"Failed to save domain configuration: $err")
            // store the new mediator identity. It's only once this completes successfully that a mediator is considered
            // initialized. If we crash/fail after storing the domain config but before storing the id the Mediator will
            // need to be called by the domain initialization again.
            nodeId = NodeId(request.mediatorId.uid)
            _ = logger.debug(
              s"Initializing mediator node with member id [${nodeId.identity}] "
            )
            _ <- storeId(nodeId)
            _ = EitherTUtil.doNotAwait(
              initialize(nodeId).onShutdown(
                Left("Initialized successfully but aborting startup due to shutdown")
              ),
              "Failed to initialize mediator",
            )
          } yield key

        def replyWithExistingConfiguration(
            existingConfig: MediatorDomainConfiguration
        ): EitherT[Future, String, SigningPublicKey] =
          for {
            // first check they're at least trying to initialize us for the same domain
            _ <- EitherT.cond(
              existingConfig.domainId == request.domainId,
              (),
              s"Attempt to initialize mediator for domain [${request.domainId}]. Mediator is already initialized for domain [${existingConfig.domainId}]",
            )

            // Returns the initial signing key. If the mediator node has rolled over its keys, the initial key may not be valid anymore.
            // However a mediator node should not be initialized again after it has been running for a while.
            existingKey <- crypto.value.cryptoPublicStore
              .signingKey(existingConfig.initialKeyFingerprint)
              .leftMap(_.toString)
              .subflatMap(_.toRight(s"Failed to lookup initial signing key"))
          } yield existingKey

        for {
          // if initialize is called on a passive instance any write operation will throw a passive instance exception,
          // however do a check upfront to provide a more helpful error message if we're already passive at this point
          _ <- EitherTUtil.condUnitET(storage.isActive, "Mediator replica is not active")
          //  check to see if we're already initialized
          existingId <- EitherT.right(initializationStore.id)
          // check to see if we've already initialized the mediator domain configuration
          existingConfiguration <- domainConfigurationStore.fetchConfiguration
            .leftMap(err => s"Failed to fetch domain configuration from database: $err")
          // if we have an id, we should have had persisted the domain configuration. check this is true.
          _ <- (existingId, existingConfiguration) match {
            case (Some(_id), None) =>
              logger.warn(
                "Mediator is in a bad state. It has a stored identity but is missing the required domain configuration to successfully start."
              )
              EitherT.leftT(
                "Mediator has an identity but is missing the required domain configuration"
              )
            case _ => EitherT.pure(())
          }
          // if we have already been initialized take our original mediator signing key, otherwise start the process of
          // initializing the mediator. if there is an existing configuration but no stored id it means we haven't successfully
          // initialized before and there for will attempt to initialize with the new config.
          mediatorKey <- existingId
            .flatMap(_ =>
              existingConfiguration
            ) // we've validated above that if existingId is set then existingConfiguration is also present
            .fold(initializeMediator)(replyWithExistingConfiguration)
        } yield mediatorKey
      },
      s"handle initialization request for ${request.domainId}",
    )
  }

  /** Attempt to start the node with this identity. */
  override protected def initialize(nodeId: NodeId): EitherT[FutureUnlessShutdown, String, Unit] = {
    val mediatorId = MediatorId(nodeId.identity)
    def topologyComponentFactory(domainId: DomainId, protocolVersion: ProtocolVersion): EitherT[
      Future,
      String,
      (TopologyTransactionProcessorCommon, DomainTopologyClientWithInit),
    ] = {
      val topologyStore = sequencedTopologyStore.initOrGet(DomainStore(domainId))
      EitherT.right(
        TopologyTransactionProcessor.createProcessorAndClientForDomain(
          topologyStore,
          mediatorId,
          domainId,
          protocolVersion,
          crypto.value,
          Map(),
          parameters,
          clock,
          futureSupervisor,
          loggerFactory,
        )
      )
    }
    startInstanceUnlessClosing(performUnlessClosingEitherU(functionFullName) {
      initializeNodePrerequisites(
        storage,
        crypto.value,
        mediatorId,
        fetchConfig = () => domainConfigurationStore.fetchConfiguration.leftMap(_.toString),
        saveConfig = domainConfigurationStore.saveConfiguration(_).leftMap(_.toString),
        indexedStringStore,
        topologyComponentFactory,
        None,
        None,
        None,
      ).map(domainId =>
        new MediatorNode(
          config,
          mediatorId,
          domainId,
          replicaManager,
          storage,
          clock,
          loggerFactory,
          nodeHealthService.dependencies.map(_.toComponentStatus),
        )
      )
    })
  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(
      initializationActionQueue,
      sequencedTopologyStore,
      domainConfigurationStore,
      deferredSequencerClientHealth,
    )(
      logger
    )
    super.onClosed()
  }

}

object MediatorNodeBootstrap {
  val LoggerFactoryKeyName: String = "mediator"

  /** validates if topology snapshot is suitable to initialise mediator */
  def checkIfTopologySnapshotIsValid(
      mediatorId: MediatorId,
      snapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
      emptyIsValid: Boolean,
  ): Either[String, StoredTopologyTransactions[TopologyChangeOp.Positive]] = {

    def accumulateSide(x: TopologyMapping, sides: (Boolean, Boolean)): (Boolean, Boolean) =
      x match {
        case x: MediatorDomainState if x.mediator == mediatorId =>
          RequestSide.accumulateSide(sides, x.side)
        case _ => sides
      }

    // empty snapshot is valid
    // TODO(i8054) rip out default mediator initialisation
    if (emptyIsValid && snapshot.result.isEmpty) Right(snapshot)
    else {
      val (accumulated, (fromSeen, toSeen)) = snapshot.result.foldLeft(
        (Seq.empty[StoredTopologyTransaction[TopologyChangeOp.Positive]], (false, false))
      ) {
        // don't add any more transactions after initialisation
        case (acc @ (_, (true, true)), _) => acc
        case ((acc, sides), elem) =>
          val (from, to) = accumulateSide(elem.transaction.transaction.element.mapping, sides)
          // append element unless the mediator is initialised in this mapping (as we will receive this mapping
          // from the dispatcher
          if (from && to)
            (acc, (from, to))
          else (acc :+ elem, (from, to))
      }
      Either.cond(
        fromSeen && toSeen,
        StoredTopologyTransactions(accumulated),
        s"Mediator ${mediatorId} is not activated in the given snapshot",
      )
    }
  }

}

class MediatorNodeCommon(
    config: MediatorNodeConfigCommon,
    mediatorId: MediatorId,
    domainId: DomainId,
    replicaManager: MediatorReplicaManager,
    storage: Storage,
    override protected val clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  def isActive: Boolean = replicaManager.isActive

  def status: Future[MediatorNodeStatus] = {
    val ports = Map("admin" -> config.adminApi.port)
    Future.successful(
      MediatorNodeStatus(
        mediatorId.uid,
        domainId,
        uptime(),
        ports,
        replicaManager.isActive,
        replicaManager.getTopologyQueueStatus,
        healthData,
      )
    )
  }

  override def close(): Unit =
    Lifecycle.close(
      replicaManager,
      storage,
    )(logger)
}

class MediatorNode(
    config: MediatorNodeConfigCommon,
    mediatorId: MediatorId,
    domainId: DomainId,
    replicaManager: MediatorReplicaManager,
    storage: Storage,
    clock: Clock,
    loggerFactory: NamedLoggerFactory,
    components: => Seq[ComponentStatus],
) extends MediatorNodeCommon(
      config,
      mediatorId,
      domainId,
      replicaManager,
      storage,
      clock,
      loggerFactory,
      components,
    )
