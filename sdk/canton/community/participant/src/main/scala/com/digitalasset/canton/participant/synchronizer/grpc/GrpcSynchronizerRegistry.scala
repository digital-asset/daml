// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{
  CryptoHandshakeValidator,
  SyncCryptoApiParticipantProvider,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.SynchronizerPredecessor
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
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolError
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.client.{
  RecordingConfig,
  ReplayConfig,
  RichSequencerClient,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnectionXPoolFactory,
  SequencerConnectionValidation,
  SequencerConnections,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContextExecutor
import scala.util.Success

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
      override val psid: PhysicalSynchronizerId,
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
          topologyDispatcher.synchronizerDisconnected(psid),
        ),
        // Close the sequencer client so that the processors won't receive or handle events when
        // their shutdown is initiated.
        SyncCloseable("sequencerClient", sequencerClient.close()),
        SyncCloseable("sequencerChannelClient", sequencerChannelClientO.foreach(_.close())),
      )
    }
  }

  override def connect(
      config: SynchronizerConnectionConfig,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Either[SynchronizerRegistryError, (SynchronizerHandle, SynchronizerConnectionConfig)]
  ] = {

    val sequencerConnections: SequencerConnections =
      config.sequencerConnections

    val connectionPoolFactory = new GrpcSequencerConnectionXPoolFactory(
      clientProtocolVersions =
        ProtocolVersionCompatibility.supportedProtocols(participantNodeParameters),
      minimumProtocolVersion = participantNodeParameters.protocolConfig.minimumProtocolVersion,
      authConfig = participantNodeParameters.sequencerClient.authToken,
      member = participantId,
      clock = clock,
      crypto = cryptoApiProvider.crypto,
      seedForRandomnessO = testingConfig.sequencerTransportSeed,
      futureSupervisor = futureSupervisor,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

    val connectionPoolE = connectionPoolFactory
      .createFromOldConfig(
        config.sequencerConnections,
        config.synchronizerId,
        participantNodeParameters.tracing,
      )
      .leftMap[SynchronizerRegistryError](error =>
        SynchronizerRegistryError.SynchronizerRegistryInternalError.InvalidState(error.toString)
      )

    val useNewConnectionPool = participantNodeParameters.sequencerClient.useNewConnectionPool

    val runE = for {
      connectionPool <- connectionPoolE.toEitherT[FutureUnlessShutdown]
      _ <-
        if (useNewConnectionPool) {
          connectionPool.start().leftMap {
            case error: SequencerConnectionXPoolError.TimeoutError =>
              SynchronizerRegistryError.ConnectionErrors.SynchronizerIsNotAvailable
                .Error(config.synchronizerAlias, error.toString)

            case error @ (_: SequencerConnectionXPoolError.ThresholdUnreachableError |
                _: SequencerConnectionXPoolError.InvalidConfigurationError) =>
              SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers
                .Error(error.toString)
          }
        } else EitherTUtil.unitUS[SynchronizerRegistryError]

      info <-
        if (useNewConnectionPool) {
          // TODO:(i27260): Cleanup old code
          // This builds a `SequencerAggregatedInfo` structure to satisfy further code that expects it to be present:
          //
          // - The updating of the configuration performed below needs `info.sequencerConnections.aliasToConnection`
          //   to update the sequencer IDs.
          // - The `SynchronizerRegistryHelpers` use `info.sequencerConnections` to initialize a
          //   `SequencerConnectClient` and talk to the `SequencerConnectService` (see TODO(i27618)).
          // - The `SequencerChannelClient` (used by Online Party Replication?) needs `info.sequencerConnections` and
          //   `info.expectedSequencersO` to validate its configuration and initialize transports (it still uses transports).
          //
          // The connections used for building this information with the transport mechanism however depends on the
          // validation mode (all, active only, etc.), whereas with the connection pool we only have the threshold-many
          // connections that were needed to initialize the pool.
          //
          // It is unclear at this point whether this may lead to incorrect behaviors of those components.
          val psid = connectionPool.physicalSynchronizerIdO.getOrElse(
            ErrorUtil.invalidState(
              "a successfully started connection pool must have the synchronizer ID defined"
            )
          )
          val staticParameters = connectionPool.staticSynchronizerParametersO.getOrElse(
            ErrorUtil.invalidState(
              "a successfully started connection pool must have the static parameters defined"
            )
          )

          NonEmpty.from(connectionPool.getAllConnections()) match {
            case Some(allConnectionsNE) =>
              val expectedSequencers = allConnectionsNE.map { connection =>
                val name = connection.config.name
                val alias = name.substring(0, name.lastIndexOf('-'))
                val sequencerId = connection.attributes.sequencerId
                SequencerAlias.tryCreate(alias) -> sequencerId
              }.toMap
              val aliasToSequencerConnection = expectedSequencers.map { case (alias, sequencerId) =>
                val sequencerConnection = config.sequencerConnections.aliasToConnection
                  .getOrElse(alias, ErrorUtil.invalidState(s"Unknown alias: $alias"))
                alias -> sequencerConnection.withSequencerId(sequencerId)
              }.toMap

              SequencerConnections
                .many(
                  NonEmptyUtil.fromUnsafe(aliasToSequencerConnection.values.toSeq),
                  config.sequencerConnections.sequencerTrustThreshold,
                  config.sequencerConnections.sequencerLivenessMargin,
                  config.sequencerConnections.submissionRequestAmplification,
                )
                .leftMap(error =>
                  SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers
                    .Error(error)
                )
                .map(newSequencerConnections =>
                  SequencerAggregatedInfo(
                    psid = psid,
                    staticSynchronizerParameters = staticParameters,
                    expectedSequencersO = Some(expectedSequencers),
                    sequencerConnections = newSequencerConnections,
                  )
                )
                .toEitherT[FutureUnlessShutdown]

            case None => // This should not happen because the pool was successfully started
              val error = SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers
                .Error("No validated connection found")
              EitherT.leftT[FutureUnlessShutdown, SequencerAggregatedInfo](error)
          }
        } else
          sequencerInfoLoader
            .loadAndAggregateSequencerEndpoints(
              config.synchronizerAlias,
              config.synchronizerId,
              sequencerConnections,
              SequencerConnectionValidation.ThresholdActive,
            )(traceContext, CloseContext(this))
            .leftMap(SynchronizerRegistryError.fromSequencerInfoLoaderError)

      _ <- CryptoHandshakeValidator
        .validate(info.staticSynchronizerParameters, cryptoConfig)
        .leftMap(
          SynchronizerRegistryError.HandshakeErrors.SynchronizerCryptoHandshakeFailed.Error(_)
        )
        .toEitherT[FutureUnlessShutdown]

      _ <- aliasManager
        .processHandshake(config.synchronizerAlias, info.psid)
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
            config.sequencerConnections.sequencerLivenessMargin,
            config.sequencerConnections.submissionRequestAmplification,
          )
          .map(connections => config.copy(sequencerConnections = connections))

      }
      updatedConfig <- EitherT
        .fromEither[FutureUnlessShutdown](updatedConfigE)
        .leftMap(SynchronizerRegistryInternalError.InvalidState(_))
      synchronizerHandle <- getSynchronizerHandle(
        config,
        synchronizerPredecessor,
        syncPersistentStateManager,
        info,
        connectionPool,
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

    runE.thereafter {
      case Success(UnlessShutdown.Outcome(Right(_))) =>
      // In case of error or exception, ensure the pool is closed
      case _ => connectionPoolE.foreach(_.close())
    }.value
  }
}
