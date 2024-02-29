// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.Monad
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.domain.Domain
import com.digitalasset.canton.domain.mediator.store.MediatorDomainConfiguration
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.domain.service.GrpcSequencerConnectionService
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.{HealthService, MutableHealthComponent}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SequencerClient,
  SequencerClientFactory,
}
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorCommon
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}
import monocle.Lens

import scala.concurrent.Future

trait MediatorNodeBootstrapCommon[
    T <: CantonNode,
    NC <: LocalNodeConfig & MediatorNodeConfigCommon,
] {

  this: CantonNodeBootstrapCommon[T, NC, MediatorNodeParameters, MediatorMetrics] =>

  type TopologyComponentFactory = (DomainId, ProtocolVersion) => EitherT[
    Future,
    String,
    (TopologyTransactionProcessorCommon, DomainTopologyClientWithInit),
  ]

  protected val replicaManager: MediatorReplicaManager

  protected def mediatorRuntimeFactory: MediatorRuntimeFactory

  protected lazy val deferredSequencerClientHealth =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)

  protected implicit def executionSequencerFactory: ExecutionSequencerFactory

  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService(
      "mediator",
      logger,
      timeouts,
      Seq(storage),
      softDependencies = Seq(deferredSequencerClientHealth),
    )

  /** Attempt to start the node with this identity. */
  protected def initializeNodePrerequisites(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit],
      indexedStringStore: IndexedStringStore,
      topologyComponentFactory: TopologyComponentFactory,
      topologyManagerStatusO: Option[TopologyManagerStatus],
      maybeDomainTopologyStateInit: Option[DomainTopologyInitializationCallback],
      maybeDomainOutboxFactory: Option[DomainOutboxXFactory],
  ): EitherT[Future, String, DomainId] =
    for {
      domainConfig <- fetchConfig()
        .leftMap(err => s"Failed to fetch domain configuration: $err")
        .flatMap { x =>
          EitherT.fromEither(
            x.toRight(
              s"Mediator domain config has not been set. Must first be initialized by the domain in order to start."
            )
          )
        }

      sequencerInfoLoader = new SequencerInfoLoader(
        timeouts = timeouts,
        traceContextPropagation = parameters.tracing.propagation,
        clientProtocolVersions = NonEmpty.mk(Seq, domainConfig.domainParameters.protocolVersion),
        minimumProtocolVersion = Some(domainConfig.domainParameters.protocolVersion),
        dontWarnOnDeprecatedPV = parameterConfig.dontWarnOnDeprecatedPV,
        loggerFactory = loggerFactory.append("domainId", domainConfig.domainId.toString),
      )

      _ <- EitherT.right[String](
        replicaManager.setup(
          adminServerRegistry,
          () =>
            mkMediatorRuntime(
              mediatorId,
              domainConfig,
              indexedStringStore,
              fetchConfig,
              saveConfig,
              storage,
              crypto,
              topologyComponentFactory,
              topologyManagerStatusO,
              maybeDomainTopologyStateInit,
              maybeDomainOutboxFactory,
              sequencerInfoLoader,
            ),
          storage.isActive,
        )
      )
    } yield domainConfig.domainId

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      domainConfig: MediatorDomainConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit],
      storage: Storage,
      crypto: Crypto,
      topologyComponentFactory: TopologyComponentFactory,
      topologyManagerStatusO: Option[TopologyManagerStatus],
      maybeDomainTopologyStateInit: Option[DomainTopologyInitializationCallback],
      maybeDomainOutboxFactory: Option[DomainOutboxXFactory],
      sequencerInfoLoader: SequencerInfoLoader,
  ): EitherT[Future, String, MediatorRuntime] = {
    val domainId = domainConfig.domainId
    val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)
    val domainAlias = DomainAlias(domainConfig.domainId.uid.toLengthLimitedString)

    def getSequencerConnectionFromStore = fetchConfig()
      .map(_.map(_.sequencerConnections))

    for {
      _ <- CryptoHandshakeValidator
        .validate(domainConfig.domainParameters, config.crypto)
        .toEitherT
      sequencedEventStore = SequencedEventStore(
        storage,
        SequencerClientDiscriminator.UniqueDiscriminator,
        domainConfig.domainParameters.protocolVersion,
        timeouts,
        domainLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore(storage)
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        SequencerClientDiscriminator.UniqueDiscriminator,
        timeouts,
        domainLoggerFactory,
      )
      topologyProcessorAndClient <- topologyComponentFactory(
        domainId,
        domainConfig.domainParameters.protocolVersion,
      )
      (topologyProcessor, topologyClient) = topologyProcessorAndClient
      _ = ips.add(topologyClient)
      syncCryptoApi = new DomainSyncCryptoClient(
        mediatorId,
        domainId,
        topologyClient,
        crypto,
        parameters.cachingConfigs,
        timeouts,
        futureSupervisor,
        domainLoggerFactory,
      )
      sequencerClientFactory = SequencerClientFactory(
        domainId,
        syncCryptoApi,
        crypto,
        parameters.sequencerClient,
        parameters.tracing.propagation,
        arguments.testingConfig,
        domainConfig.domainParameters,
        timeouts,
        clock,
        topologyClient,
        futureSupervisor,
        member =>
          Domain.recordSequencerInteractions
            .get()
            .lift(member)
            .map(Domain.setMemberRecordingPath(member)),
        member =>
          Domain.replaySequencerConfig.get().lift(member).map(Domain.defaultReplayPath(member)),
        arguments.metrics.sequencerClient,
        parameters.loggingConfig,
        domainLoggerFactory,
        ProtocolVersionCompatibility.trySupportedProtocolsDomain(parameters),
        None,
      )
      sequencerClientRef =
        GrpcSequencerConnectionService.setup[MediatorDomainConfiguration](mediatorId)(
          adminServerRegistry,
          fetchConfig,
          saveConfig,
          Lens[MediatorDomainConfiguration, SequencerConnections](_.sequencerConnections)(
            connection => conf => conf.copy(sequencerConnections = connection)
          ),
          RequestSigner(syncCryptoApi, domainConfig.domainParameters.protocolVersion),
          sequencerClientFactory,
          sequencerInfoLoader,
          domainAlias,
        )
      // we wait here until the sequencer becomes active. this allows to reconfigure the
      // sequencer client address
      connections <- GrpcSequencerConnectionService.waitUntilSequencerConnectionIsValid(
        sequencerClientFactory,
        this,
        futureSupervisor,
        getSequencerConnectionFromStore,
      )
      info <- sequencerInfoLoader
        .loadSequencerEndpoints(
          domainAlias,
          connections,
        )
        .leftMap(_.cause)
      requestSigner = RequestSigner(syncCryptoApi, domainConfig.domainParameters.protocolVersion)
      _ <- maybeDomainTopologyStateInit match {
        case Some(domainTopologyStateInit) =>
          val headSnapshot = topologyClient.headSnapshot
          for {
            // TODO(i12076): Request topology information from all sequencers and reconcile
            isMediatorActive <- EitherT.right[String](headSnapshot.isMediatorActive(mediatorId))
            _ <- Monad[EitherT[Future, String, *]].whenA(!isMediatorActive)(
              sequencerClientFactory
                .makeTransport(
                  info.sequencerConnections.default,
                  mediatorId,
                  requestSigner,
                  allowReplay = false,
                )
                .flatMap(
                  ResourceUtil.withResourceEitherT(_)(
                    domainTopologyStateInit
                      .callback(topologyClient, _, domainConfig.domainParameters.protocolVersion)
                  )
                )
            )
          } yield {}

        case None => EitherT.pure[Future, String](())
      }

      sequencerClient <- sequencerClientFactory.create(
        mediatorId,
        sequencedEventStore,
        sendTrackerStore,
        requestSigner,
        info.sequencerConnections,
        info.expectedSequencers,
      )

      _ = sequencerClientRef.set(sequencerClient)
      _ = deferredSequencerClientHealth.set(sequencerClient.healthComponent)

      // can just new up the enterprise mediator factory here as the mediator node is only available in enterprise setups
      mediatorRuntime <- mediatorRuntimeFactory.create(
        mediatorId,
        domainId,
        storage,
        sequencerCounterTrackerStore,
        sequencedEventStore,
        sequencerClient,
        syncCryptoApi,
        topologyClient,
        topologyProcessor,
        topologyManagerStatusO,
        maybeDomainOutboxFactory,
        config.timeTracker,
        parameters,
        domainConfig.domainParameters.protocolVersion,
        clock,
        arguments.metrics,
        futureSupervisor,
        domainLoggerFactory,
      )
      _ <- mediatorRuntime.start()
    } yield mediatorRuntime
  }

}
