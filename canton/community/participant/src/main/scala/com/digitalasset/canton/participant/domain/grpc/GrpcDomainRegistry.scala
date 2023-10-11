// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import akka.stream.Materializer
import cats.Eval
import cats.data.EitherT
import cats.instances.future.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.store.{
  ParticipantSettingsLookup,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyDispatcherCommon,
  TopologyComponentFactory,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future}

/** Domain registry used to connect to domains over GRPC
  *
  * @param participantId The participant id from which we connect to domains.
  * @param participantNodeParameters General set of parameters that control Canton
  * @param ec ExecutionContext used by the sequencer client
  * @param trustDomain a call back handle to the participant topology manager to issue a domain trust certificate
  */
class GrpcDomainRegistry(
    val participantId: ParticipantId,
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantSettings: Eval[ParticipantSettingsLookup],
    agreementService: AgreementService,
    topologyDispatcher: ParticipantTopologyDispatcherCommon,
    cryptoApiProvider: SyncCryptoApiProvider,
    cryptoConfig: CryptoConfig,
    clock: Clock,
    val participantNodeParameters: ParticipantNodeParameters,
    aliasManager: DomainAliasManager,
    testingConfig: TestingConfigInternal,
    recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    metrics: DomainAlias => SyncDomainMetrics,
    sequencerInfoLoader: SequencerInfoLoader,
    override protected val futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(
    implicit val ec: ExecutionContextExecutor,
    override implicit val executionSequencerFactory: ExecutionSequencerFactory,
    val materializer: Materializer,
    val tracer: Tracer,
) extends DomainRegistry
    with DomainRegistryHelpers
    with FlagCloseable
    with HasFutureSupervision
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  private class GrpcDomainHandle(
      override val domainId: DomainId,
      override val domainAlias: DomainAlias,
      override val staticParameters: StaticDomainParameters,
      sequencer: SequencerClient,
      override val topologyClient: DomainTopologyClientWithInit,
      override val topologyFactory: TopologyComponentFactory,
      override val domainPersistentState: SyncDomainPersistentState,
      override protected val timeouts: ProcessingTimeout,
  ) extends DomainHandle
      with FlagCloseableAsync
      with NamedLogging {

    override val sequencerClient: SequencerClient = sequencer
    override def loggerFactory: NamedLoggerFactory = GrpcDomainRegistry.this.loggerFactory

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
      import TraceContext.Implicits.Empty.*
      List[AsyncOrSyncCloseable](
        SyncCloseable(
          "topologyOutbox",
          topologyDispatcher.domainDisconnected(domainAlias),
        ),
        SyncCloseable("agreementService", agreementService.close()),
        SyncCloseable("sequencerClient", sequencerClient.close()),
      )
    }
  }

  override def connect(
      config: DomainConnectionConfig
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, DomainHandle]] = {

    val sequencerConnections: SequencerConnections =
      config.sequencerConnections

    val agreementClient = new AgreementClient(
      agreementService,
      sequencerConnections,
      loggerFactory,
    )

    val runE = for {
      info <- sequencerInfoLoader
        .loadSequencerEndpoints(config.domain, sequencerConnections)(
          traceContext,
          CloseContext(this),
        )
        .leftMap(DomainRegistryError.fromSequencerInfoLoaderError)
        .mapK(
          FutureUnlessShutdown.outcomeK
        )

      _ <- aliasManager
        .processHandshake(config.domain, info.domainId)
        .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
        .mapK(
          FutureUnlessShutdown.outcomeK
        )

      domainHandle <- getDomainHandle(
        config,
        syncDomainPersistentStateManager,
        info,
      )(
        cryptoApiProvider,
        cryptoConfig,
        clock,
        testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        topologyDispatcher,
        packageDependencies,
        metrics,
        agreementClient,
        participantSettings,
      )
    } yield new GrpcDomainHandle(
      domainHandle.domainId,
      domainHandle.alias,
      domainHandle.staticParameters,
      domainHandle.sequencer,
      domainHandle.topologyClient,
      domainHandle.topologyFactory,
      domainHandle.domainPersistentState,
      domainHandle.timeouts,
    )

    runE.value
  }
}
