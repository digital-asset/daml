// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.domain.admin.v2.MediatorInitializationServiceGrpc
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequestX,
  InitializeMediatorResponseX,
}
import com.digitalasset.canton.domain.mediator.service.GrpcMediatorInitializationServiceX
import com.digitalasset.canton.domain.mediator.store.{
  MediatorDomainConfiguration,
  MediatorDomainConfigurationStore,
}
import com.digitalasset.canton.domain.metrics.MediatorNodeMetrics
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.{ComponentStatus, GrpcHealthReporter, HealthService}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  TopologyTransactionProcessorCommon,
  TopologyTransactionProcessorX,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

/** Community Mediator Node X configuration that defaults to auto-init
  */
final case class CommunityMediatorNodeXConfig(
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val init: InitConfig = InitConfig(identity = Some(InitConfigBase.Identity())),
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val parameters: MediatorNodeParameterConfig = MediatorNodeParameterConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topologyX: TopologyXConfig = TopologyXConfig(),
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
    with ConfigDefaults[DefaultPorts, CommunityMediatorNodeXConfig] {

  override val nodeTypeName: String = "mediatorx"

  override def replicationEnabled: Boolean = false

  override def withDefaults(ports: DefaultPorts): CommunityMediatorNodeXConfig = {
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorXAdminApiPort.setDefaultPort)
  }
}

class MediatorNodeBootstrapX(
    arguments: CantonNodeBootstrapCommonArguments[
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorNodeMetrics,
    ],
    protected val replicaManager: MediatorReplicaManager,
    override protected val mediatorRuntimeFactory: MediatorRuntimeFactory,
)(
    implicit executionContext: ExecutionContextIdlenessExecutorService,
    override protected implicit val executionSequencerFactory: ExecutionSequencerFactory,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapX[
      MediatorNodeX,
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorNodeMetrics,
    ](arguments)
    with MediatorNodeBootstrapCommon[MediatorNodeX, MediatorNodeConfigCommon] {

  override protected def member(uid: UniqueIdentifier): Member = MediatorId(uid)

  private val domainTopologyManager = new SingleUseCell[DomainTopologyManagerX]()

  override protected def sequencedTopologyStores: Seq[TopologyStoreX[DomainStore]] =
    domainTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManagerX] =
    domainTopologyManager.get.toList

  private class WaitForMediatorToDomainInit(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      healthService: HealthService,
  ) extends BootstrapStageWithStorage[MediatorNodeX, StartupNode, DomainId](
        "wait-for-mediator-to-domain-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcMediatorInitializationServiceX.Callback {

    adminServerRegistry
      .addServiceU(
        MediatorInitializationServiceGrpc
          .bindService(
            new GrpcMediatorInitializationServiceX(this, loggerFactory),
            executionContext,
          ),
        true,
      )

    protected val domainConfigurationStore =
      MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
    addCloseable(domainConfigurationStore)
    addCloseable(deferredSequencerClientHealth)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[DomainId]] = domainConfigurationStore.fetchConfiguration.toOption.mapFilter {
      case Some(res) => Some(res.domainId)
      case None => None
    }.value

    override protected def buildNextStage(domainId: DomainId): StartupNode = {
      val domainTopologyStore =
        TopologyStoreX(DomainStore(domainId), storage, timeouts, loggerFactory)
      addCloseable(domainTopologyStore)

      val outboxQueue = new DomainOutboxQueue(loggerFactory)
      val topologyManager = new DomainTopologyManagerX(
        clock = clock,
        crypto = crypto,
        store = domainTopologyStore,
        outboxQueue = outboxQueue,
        enableTopologyTransactionValidation = config.topologyX.enableTopologyTransactionValidation,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
        loggerFactory = loggerFactory,
      )

      if (domainTopologyManager.putIfAbsent(topologyManager).nonEmpty) {
        // TODO(#14048) how to handle this error properly?
        throw new IllegalStateException("domainTopologyManager shouldn't have been set before")
      }

      new StartupNode(
        storage,
        crypto,
        mediatorId,
        authorizedTopologyManager,
        topologyManager,
        domainId,
        domainConfigurationStore,
        domainTopologyStore,
        healthService,
      )
    }

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[DomainId]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    override def initialize(request: InitializeMediatorRequestX)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeMediatorResponseX] = {
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized mediator. Skipping initialization!"
        )
        EitherT.pure(InitializeMediatorResponseX())
      } else {
        completeWithExternal {
          logger.info(
            s"Assigning mediator to ${request.domainId} via sequencers ${request.sequencerConnections}"
          )
          domainConfigurationStore
            .saveConfiguration(
              MediatorDomainConfiguration(
                Fingerprint.tryCreate("unused"), // x-nodes do not need to return the initial key
                request.domainId,
                request.domainParameters,
                request.sequencerConnections,
              )
            )
            .leftMap(_.toString)
            .map(_ => request.domainId)
        }.map(_ => InitializeMediatorResponseX())
      }
    }

  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      domainTopologyManager: DomainTopologyManagerX,
      domainId: DomainId,
      domainConfigurationStore: MediatorDomainConfigurationStore,
      domainTopologyStore: TopologyStoreX[DomainStore],
      healthService: HealthService,
  ) extends BootstrapStage[MediatorNodeX, RunningNode[MediatorNodeX]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNodeX]]] = {

      def topologyComponentFactory(domainId: DomainId, protocolVersion: ProtocolVersion): EitherT[
        Future,
        String,
        (TopologyTransactionProcessorCommon, DomainTopologyClientWithInit),
      ] =
        EitherT.right(
          TopologyTransactionProcessorX.createProcessorAndClientForDomain(
            domainTopologyStore,
            domainId,
            protocolVersion,
            crypto.pureCrypto,
            arguments.parameterConfig,
            config.topologyX.enableTopologyTransactionValidation,
            arguments.clock,
            arguments.futureSupervisor,
            domainLoggerFactory,
          )
        )

      val domainOutboxFactory = new DomainOutboxXFactory(
        domainId = domainId,
        memberId = mediatorId,
        authorizedTopologyManager = authorizedTopologyManager,
        domainTopologyManager = domainTopologyManager,
        crypto = crypto,
        topologyXConfig = config.topologyX,
        timeouts = timeouts,
        loggerFactory = domainLoggerFactory,
        futureSupervisor = arguments.futureSupervisor,
      )
      performUnlessClosingEitherU("starting up mediator node") {
        val indexedStringStore = IndexedStringStore.create(
          storage,
          parameterConfig.cachingConfigs.indexedStrings,
          timeouts,
          domainLoggerFactory,
        )
        addCloseable(indexedStringStore)
        for {
          domainId <- initializeNodePrerequisites(
            storage,
            crypto,
            mediatorId,
            () => domainConfigurationStore.fetchConfiguration.leftMap(_.toString),
            domainConfigurationStore.saveConfiguration(_).leftMap(_.toString),
            indexedStringStore,
            topologyComponentFactory,
            Some(TopologyManagerStatus.combined(authorizedTopologyManager, domainTopologyManager)),
            maybeDomainTopologyStateInit = Some(
              new StoreBasedDomainTopologyInitializationCallback(mediatorId, domainTopologyStore)
            ),
            maybeDomainOutboxFactory = Some(domainOutboxFactory),
          )
        } yield {
          val node = new MediatorNodeX(
            arguments.config,
            mediatorId,
            domainId,
            replicaManager,
            storage,
            clock,
            domainLoggerFactory,
            components = healthService.dependencies.map(_.toComponentStatus),
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
        }
      }
    }
  }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      healthServer: GrpcHealthReporter,
      healthService: HealthService,
  ): BootstrapStageOrLeaf[MediatorNodeX] = {
    new WaitForMediatorToDomainInit(
      storage,
      crypto,
      MediatorId(nodeId),
      authorizedTopologyManager,
      healthService,
    )
  }

  override protected def onClosed(): Unit = {
    super.onClosed()
  }

}

object MediatorNodeBootstrapX {
  val LoggerFactoryKeyName: String = "mediatorx"
}

class MediatorNodeX(
    config: MediatorNodeConfigCommon,
    mediatorId: MediatorId,
    domainId: DomainId,
    protected[canton] val replicaManager: MediatorReplicaManager,
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
    ) {

  override def close(): Unit = {
    super.close()
  }

}
