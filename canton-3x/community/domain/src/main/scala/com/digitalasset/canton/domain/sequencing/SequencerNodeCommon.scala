// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import better.files.*
import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.config.{
  SequencerNodeConfigCommon,
  SequencerNodeParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.sequencer.{Sequencer, SequencerFactory}
import com.digitalasset.canton.domain.server.DynamicDomainGrpcServer
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.{SequencerHealthStatus, SequencerNodeStatus}
import com.digitalasset.canton.health.{
  ComponentStatus,
  GrpcHealthReporter,
  HealthService,
  MutableHealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.{DomainParametersLookup, StaticDomainParameters}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorCommon
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContextExecutorService, Future}

trait SequencerNodeBootstrapCommon[
    T <: CantonNode,
    NC <: LocalNodeConfig & SequencerNodeConfigCommon,
] {

  this: CantonNodeBootstrapCommon[T, NC, SequencerNodeParameters, SequencerMetrics] =>

  // Deferred health component for the sequencer health, created during initialization
  protected lazy val sequencerHealth = new MutableHealthQuasiComponent[Sequencer](
    loggerFactory,
    Sequencer.healthName,
    SequencerHealthStatus(isActive = false),
    timeouts,
    SequencerHealthStatus.shutdownStatus,
  )

  // The service exposed by the gRPC health endpoint of sequencer public API
  // This will be used by sequencer clients who perform client-side load balancing to determine sequencer health
  protected lazy val sequencerPublicApiHealthService = HealthService(
    CantonGrpcUtil.sequencerHealthCheckServiceName,
    logger,
    timeouts,
    criticalDependencies = Seq(sequencerHealth),
  )

  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService(
      "sequencer",
      logger,
      timeouts,
      Seq(storage),
    )

  // Creates a dynamic domain server that initially only exposes a health endpoint, and can later be
  // setup with the sequencer runtime to provide the full sequencer domain API
  protected def makeDynamicDomainServer(
      maxRequestSize: MaxRequestSize,
      grpcHealthReporter: GrpcHealthReporter,
  ) = {
    new DynamicDomainGrpcServer(
      loggerFactory,
      maxRequestSize,
      arguments.parameterConfig,
      config.publicApi,
      arguments.metrics.metricsFactory,
      arguments.metrics.grpcMetrics,
      grpcHealthReporter,
      sequencerPublicApiHealthService,
    )
  }

  protected def onClosedCommon(): Unit = {}

  protected def mediatorsProcessParticipantTopologyRequests: Boolean = false

  protected val createEnterpriseAdminService: (
      Sequencer,
      NamedLoggerFactory,
  ) => Option[ServerServiceDefinition]

  protected def createSequencerRuntime(
      sequencerFactory: SequencerFactory,
      domainId: DomainId,
      sequencerId: SequencerId,
      staticMembersToRegister: Seq[Member],
      topologyClient: DomainTopologyClientWithInit,
      topologyProcessor: TopologyTransactionProcessorCommon,
      topologyManagerStatus: Option[TopologyManagerStatus],
      staticDomainParameters: StaticDomainParameters,
      storage: Storage,
      crypto: Crypto,
      indexedStringStore: IndexedStringStore,
      initializationObserver: Future[Unit],
      initializedAtHead: => Future[Boolean],
      arguments: CantonNodeBootstrapCommonArguments[_, SequencerNodeParameters, SequencerMetrics],
      topologyStateForInitializationService: Option[TopologyStateForInitializationService],
      maybeDomainOutboxFactory: Option[DomainOutboxXFactorySingleCreate],
      memberAuthServiceFactory: MemberAuthenticationServiceFactory,
      rateLimitManager: Option[SequencerRateLimitManager],
      implicitMemberRegistration: Boolean,
      domainLoggerFactory: NamedLoggerFactory,
  ): EitherT[Future, String, SequencerRuntime] = {
    for {
      agreementManager <- EitherT.fromEither[Future](config.serviceAgreement.traverse {
        agreementFile =>
          ServiceAgreementManager.create(
            agreementFile.toScala,
            storage,
            crypto.pureCrypto,
            staticDomainParameters.protocolVersion,
            parameters.processingTimeouts,
            domainLoggerFactory,
          )
      })
      clientDiscriminator <- EitherT.right(
        SequencerClientDiscriminator.fromDomainMember(sequencerId, indexedStringStore)
      )

      syncCrypto = new DomainSyncCryptoClient(
        sequencerId,
        domainId,
        topologyClient,
        crypto,
        parameters.cachingConfigs,
        parameters.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

      sequencer <- EitherT.liftF[Future, String, Sequencer](
        sequencerFactory.create(
          domainId,
          sequencerId,
          clock,
          clock,
          syncCrypto,
          futureSupervisor,
          rateLimitManager,
          implicitMemberRegistration,
        )
      )

      runtime = new SequencerRuntimeForSeparateNode(
        sequencerId,
        sequencer,
        staticDomainParameters,
        parameters,
        config.publicApi,
        config.timeTracker,
        arguments.testingConfig,
        arguments.metrics,
        domainId,
        syncCrypto,
        topologyClient,
        topologyProcessor,
        topologyManagerStatus,
        mediatorsProcessParticipantTopologyRequests,
        initializationObserver,
        initializedAtHead,
        storage,
        clock,
        SequencerAuthenticationConfig(
          agreementManager,
          config.publicApi.nonceExpirationTime,
          config.publicApi.tokenExpirationTime,
        ),
        createEnterpriseAdminService(_, domainLoggerFactory),
        staticMembersToRegister,
        futureSupervisor,
        agreementManager,
        memberAuthServiceFactory,
        topologyStateForInitializationService,
        maybeDomainOutboxFactory,
        clientDiscriminator,
        domainLoggerFactory,
      )
      _ <- runtime.initializeAll()
    } yield runtime
  }

  protected def createSequencerServer(
      runtime: SequencerRuntime,
      staticDomainParameters: StaticDomainParameters,
      topologyClient: DomainTopologyClientWithInit,
      server: Option[DynamicDomainGrpcServer],
      healthReporter: GrpcHealthReporter,
      loggerFactory: NamedLoggerFactory,
  ): EitherT[Future, String, DynamicDomainGrpcServer] = {
    runtime.registerAdminGrpcServices(service => adminServerRegistry.addServiceU(service))
    val domainParamsLookup = DomainParametersLookup.forSequencerDomainParameters(
      staticDomainParameters,
      config.publicApi.overrideMaxRequestSize,
      topologyClient,
      futureSupervisor,
      loggerFactory,
    )
    for {
      maxRequestSize <- EitherTUtil
        .fromFuture(
          domainParamsLookup.getApproximate(),
          error => s"Unable to retrieve the domain parameters: ${error.getMessage}",
        )
        .map(paramsO =>
          paramsO.map(_.maxRequestSize).getOrElse(MaxRequestSize(NonNegativeInt.maxValue))
        )
      sequencerNodeServer = server
        .getOrElse(
          makeDynamicDomainServer(maxRequestSize, healthReporter)
        )
        .initialize(runtime)
      // wait for the server to be initialized before reporting a serving health state
      _ = sequencerHealth.set(runtime.sequencer)
    } yield sequencerNodeServer
  }

}

class SequencerNodeCommon(
    config: SequencerNodeConfigCommon,
    metrics: SequencerMetrics,
    parameters: SequencerNodeParameters,
    override protected val clock: Clock,
    val sequencer: SequencerRuntime,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerNodeServer: DynamicDomainGrpcServer,
    healthData: => Seq[ComponentStatus],
)(implicit executionContext: ExecutionContextExecutorService)
    extends CantonNode
    with NamedLogging
    with HasUptime {

  logger.info(s"Creating sequencer server with public api ${config.publicApi}")(TraceContext.empty)

  override def isActive = true

  override def status: Future[SequencerNodeStatus] = {
    for {
      healthStatus <- sequencer.health
      activeMembers <- sequencer.fetchActiveMembers()
      ports = Map("public" -> config.publicApi.port, "admin" -> config.adminApi.port)
      participants = activeMembers.collect { case participant: ParticipantId =>
        participant
      }
    } yield SequencerNodeStatus(
      sequencer.domainId.unwrap,
      sequencer.domainId,
      uptime(),
      ports,
      participants,
      healthStatus,
      topologyQueue = sequencer.topologyQueue,
      healthData,
    )
  }

  override def close(): Unit =
    Lifecycle.close(
      sequencer,
      sequencerNodeServer.publicServer,
    )(logger)

}
