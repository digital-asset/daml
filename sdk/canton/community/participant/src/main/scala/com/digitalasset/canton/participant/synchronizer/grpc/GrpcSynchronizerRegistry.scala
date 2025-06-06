// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{
  CryptoHandshakeValidator,
  SyncCryptoApiParticipantProvider,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.*
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.SynchronizerRegistryInternalError
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  TopologyComponentFactory,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.client.{
  RecordingConfig,
  ReplayConfig,
  RichSequencerClient,
}
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContextExecutor

/** synchronizer registry used to connect to synchronizers over GRPC
  *
  * @param participantId
  *   The participant id from which we connect to synchronizers.
  * @param participantNodeParameters
  *   General set of parameters that control Canton
  * @param ec
  *   ExecutionContext used by the sequencer client
  */
class GrpcSynchronizerRegistry(
    val participantId: ParticipantId,
    syncPersistentStateManager: SyncPersistentStateManager,
    topologyDispatcher: ParticipantTopologyDispatcher,
    cryptoApiProvider: SyncCryptoApiParticipantProvider,
    cryptoConfig: CryptoConfig,
    clock: Clock,
    val participantNodeParameters: ParticipantNodeParameters,
    aliasManager: SynchronizerAliasManager,
    testingConfig: TestingConfigInternal,
    recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    packageDependencyResolver: PackageDependencyResolverUS,
    metrics: SynchronizerAlias => ConnectedSynchronizerMetrics,
    sequencerInfoLoader: SequencerInfoLoader,
    partyNotifier: LedgerServerPartyNotifier,
    override protected val futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(
    implicit val ec: ExecutionContextExecutor,
    override implicit val executionSequencerFactory: ExecutionSequencerFactory,
    val materializer: Materializer,
    val tracer: Tracer,
) extends SynchronizerRegistry
    with SynchronizerRegistryHelpers
    with FlagCloseable
    with HasFutureSupervision
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  private class GrpcSynchronizerHandle(
      override val synchronizerId: PhysicalSynchronizerId,
      override val synchronizerAlias: SynchronizerAlias,
      override val staticParameters: StaticSynchronizerParameters,
      sequencer: RichSequencerClient,
      override val sequencerChannelClientO: Option[SequencerChannelClient],
      override val topologyClient: SynchronizerTopologyClientWithInit,
      override val topologyFactory: TopologyComponentFactory,
      override val syncPersistentState: SyncPersistentState,
      override val syncCrypto: SynchronizerCryptoClient,
      override protected val timeouts: ProcessingTimeout,
  ) extends SynchronizerHandle
      with FlagCloseableAsync
      with NamedLogging {

    override val sequencerClient: RichSequencerClient = sequencer
    override def loggerFactory: NamedLoggerFactory = GrpcSynchronizerRegistry.this.loggerFactory

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
      import TraceContext.Implicits.Empty.*
      List[AsyncOrSyncCloseable](
        // Close the synchronizer crypto client first to stop waiting for snapshots that may block the sequencer subscription
        SyncCloseable("SyncCryptoClient", syncCrypto.close()),
        SyncCloseable(
          "topologyOutbox",
          topologyDispatcher.synchronizerDisconnected(synchronizerAlias),
        ),
        // Close the sequencer client so that the processors won't receive or handle events when
        // their shutdown is initiated.
        SyncCloseable("sequencerClient", sequencerClient.close()),
        SyncCloseable("sequencerChannelClient", sequencerChannelClientO.foreach(_.close())),
      )
    }
  }

  override def connect(
      config: SynchronizerConnectionConfig
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Either[SynchronizerRegistryError, (SynchronizerHandle, SynchronizerConnectionConfig)]
  ] = {

    val sequencerConnections: SequencerConnections =
      config.sequencerConnections

    val runE = for {
      info <- sequencerInfoLoader
        .loadAndAggregateSequencerEndpoints(
          config.synchronizerAlias,
          config.synchronizerId,
          sequencerConnections,
          SequencerConnectionValidation.Active, // only validate active sequencers (not all endpoints)
        )(traceContext, CloseContext(this))
        .leftMap(SynchronizerRegistryError.fromSequencerInfoLoaderError)

      _ <- CryptoHandshakeValidator
        .validate(info.staticSynchronizerParameters, cryptoConfig)
        .leftMap(
          SynchronizerRegistryError.HandshakeErrors.SynchronizerCryptoHandshakeFailed.Error(_)
        )
        .toEitherT[FutureUnlessShutdown]

      _ <- aliasManager
        .processHandshake(config.synchronizerAlias, info.synchronizerId)
        .leftMap(SynchronizerRegistryHelpers.fromSynchronizerAliasManagerError)

      updatedConfigE = {
        val connectionsWithSequencerId = info.sequencerConnections.aliasToConnection
        val updatedConnections = config.sequencerConnections.aliasToConnection.map {
          case (_, connection) =>
            val potentiallyUpdatedConnection =
              connectionsWithSequencerId.getOrElse(connection.sequencerAlias, connection)
            val updatedConnection = potentiallyUpdatedConnection.sequencerId
              .map(connection.withSequencerId)
              .getOrElse(connection)
            updatedConnection
        }.toSeq
        SequencerConnections
          .many(
            updatedConnections,
            config.sequencerConnections.sequencerTrustThreshold,
            config.sequencerConnections.submissionRequestAmplification,
          )
          .map(connections => config.copy(sequencerConnections = connections))

      }
      updatedConfig <- EitherT
        .fromEither[FutureUnlessShutdown](updatedConfigE)
        .leftMap(SynchronizerRegistryInternalError.InvalidState(_))
      synchronizerHandle <- getSynchronizerHandle(
        config,
        syncPersistentStateManager,
        info,
      )(
        cryptoApiProvider,
        clock,
        testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        topologyDispatcher,
        packageDependencyResolver,
        partyNotifier,
        metrics,
      )
    } yield {
      val grpcHandle = new GrpcSynchronizerHandle(
        synchronizerHandle.synchronizerId,
        synchronizerHandle.alias,
        synchronizerHandle.staticParameters,
        synchronizerHandle.sequencer,
        synchronizerHandle.channelSequencerClientO,
        synchronizerHandle.topologyClient,
        synchronizerHandle.topologyFactory,
        synchronizerHandle.persistentState,
        synchronizerHandle.syncCryptoApi,
        synchronizerHandle.timeouts,
      )
      (grpcHandle, updatedConfig)
    }

    runE.value
  }
}
