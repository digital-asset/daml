// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.SequencerConnectClient
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.HandshakeErrors.SynchronizerIdMismatch
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryHelpers.SynchronizerHandle
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  TopologyComponentFactory,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.channel.{
  SequencerChannelClient,
  SequencerChannelClientFactory,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

trait SynchronizerRegistryHelpers extends FlagCloseable with NamedLogging {
  this: HasFutureSupervision =>
  def participantId: ParticipantId
  protected def participantNodeParameters: ParticipantNodeParameters

  implicit def ec: ExecutionContextExecutor
  override protected def executionContext: ExecutionContext = ec
  implicit def executionSequencerFactory: ExecutionSequencerFactory
  implicit def materializer: Materializer
  implicit def tracer: Tracer

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  protected def getSynchronizerHandle(
      config: SynchronizerConnectionConfig,
      syncPersistentStateManager: SyncPersistentStateManager,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(
      cryptoApiProvider: SyncCryptoApiParticipantProvider,
      clock: Clock,
      testingConfig: TestingConfigInternal,
      recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
      replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
      topologyDispatcher: ParticipantTopologyDispatcher,
      packageDependencyResolver: PackageDependencyResolverUS,
      partyNotifier: LedgerServerPartyNotifier,
      metrics: SynchronizerAlias => ConnectedSynchronizerMetrics,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SynchronizerHandle] = {
    import sequencerAggregatedInfo.synchronizerId

    for {
      indexedSynchronizerId <- EitherT
        .right(syncPersistentStateManager.indexedSynchronizerId(synchronizerId))

      _ <- EitherT
        .fromEither[Future](verifySynchronizerId(config, synchronizerId))
        .mapK(FutureUnlessShutdown.outcomeK)

      // fetch or create persistent state for the synchronizer
      persistentState <- syncPersistentStateManager
        .lookupOrCreatePersistentState(
          config.synchronizerAlias,
          indexedSynchronizerId,
          sequencerAggregatedInfo.staticSynchronizerParameters,
        )

      // check and issue the synchronizer trust certificate
      _ <- EitherTUtil.ifThenET(!config.initializeFromTrustedSynchronizer)(
        topologyDispatcher.trustSynchronizer(synchronizerId)
      )

      synchronizerLoggerFactory = loggerFactory.append(
        "synchronizerId",
        indexedSynchronizerId.synchronizerId.toString,
      )

      topologyFactory <- syncPersistentStateManager
        .topologyFactoryFor(
          synchronizerId,
          sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
        )
        .toRight(
          SynchronizerRegistryError.SynchronizerRegistryInternalError
            .InvalidState(
              "topology factory for synchronizer is unavailable"
            ): SynchronizerRegistryError
        )
        .toEitherT[FutureUnlessShutdown]

      newTopologyClient <- EitherT.right(
        performUnlessClosingUSF("create caching client")(
          topologyFactory.createCachingTopologyClient(
            packageDependencyResolver
          )
        )
      )

      topologyClient = cryptoApiProvider.ips.add(newTopologyClient) match {
        case client: SynchronizerTopologyClientWithInit => client
        case t => throw new IllegalStateException(s"Unknown type for topology client $t")
      }

      synchronizerCryptoApi <- EitherT.fromEither[FutureUnlessShutdown](
        cryptoApiProvider
          .forSynchronizer(synchronizerId, sequencerAggregatedInfo.staticSynchronizerParameters)
          .toRight(
            SynchronizerRegistryError.SynchronizerRegistryInternalError
              .InvalidState("crypto api for synchronizer is unavailable"): SynchronizerRegistryError
          )
      )

      (sequencerClientFactory, sequencerChannelClientFactoryO) = {
        // apply optional synchronizer specific overrides to the nodes general sequencer client config
        val sequencerClientConfig = participantNodeParameters.sequencerClient.copy(
          initialConnectionRetryDelay = config.initialRetryDelay
            .map(_.toConfig)
            .getOrElse(participantNodeParameters.sequencerClient.initialConnectionRetryDelay),
          maxConnectionRetryDelay = config.maxRetryDelay
            .map(_.toConfig)
            .getOrElse(
              participantNodeParameters.sequencerClient.maxConnectionRetryDelay
            ),
        )

        // Yields a unique path inside the given directory for record/replay purposes.
        def updateMemberRecordingPath(recordingConfig: RecordingConfig): RecordingConfig = {
          val namePrefix =
            s"${participantId.show.stripSuffix("...")}-${synchronizerId.show.stripSuffix("...")}"
          recordingConfig.setFilename(namePrefix)
        }

        def ifParticipant[C](configO: Option[C]): Member => Option[C] = {
          case _: ParticipantId => configO
          case _ => None
        }
        (
          SequencerClientFactory(
            synchronizerId,
            synchronizerCryptoApi,
            cryptoApiProvider.crypto,
            sequencerClientConfig,
            participantNodeParameters.tracing.propagation,
            testingConfig,
            sequencerAggregatedInfo.staticSynchronizerParameters,
            participantNodeParameters.processingTimeouts,
            clock,
            topologyClient,
            futureSupervisor,
            ifParticipant(recordSequencerInteractions.get().map(updateMemberRecordingPath)),
            ifParticipant(
              replaySequencerConfig
                .get()
                .map(config =>
                  config.copy(recordingConfig = updateMemberRecordingPath(config.recordingConfig))
                )
            ),
            metrics(config.synchronizerAlias).sequencerClient,
            participantNodeParameters.loggingConfig,
            participantNodeParameters.exitOnFatalFailures,
            synchronizerLoggerFactory,
            ProtocolVersionCompatibility.supportedProtocols(participantNodeParameters),
          ),
          participantNodeParameters.unsafeOnlinePartyReplication
            .map(_ =>
              new SequencerChannelClientFactory(
                synchronizerId,
                synchronizerCryptoApi,
                cryptoApiProvider.crypto,
                sequencerClientConfig,
                participantNodeParameters.tracing.propagation,
                sequencerAggregatedInfo.staticSynchronizerParameters,
                participantNodeParameters.processingTimeouts,
                clock,
                synchronizerLoggerFactory,
                ProtocolVersionCompatibility.supportedProtocols(participantNodeParameters),
              )
            ),
        )
      }

      // TODO(i12906): We should check for the activeness only from the honest sequencers.
      active <- isActive(config.synchronizerAlias, sequencerAggregatedInfo)

      // if the participant is being restarted and has completed topology initialization previously
      // then we can skip it
      _ <-
        if (active) EitherT.pure[FutureUnlessShutdown, SynchronizerRegistryError](())
        else {
          logger.debug(
            s"Participant is not yet active on synchronizer $synchronizerId. Initializing topology"
          )
          val client = sequencerConnectClient(config.synchronizerAlias, sequencerAggregatedInfo)
          for {
            success <- topologyDispatcher.onboardToSynchronizer(
              synchronizerId,
              config.synchronizerAlias,
              client,
              sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
            )
            _ <- EitherT.cond[FutureUnlessShutdown](
              success,
              (),
              SynchronizerRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
                s"Synchronizer ${config.synchronizerAlias} has rejected our on-boarding attempt"
              ),
            )
            // make sure the participant is immediately active after pushing our topology,
            // or whether we have to stop here to wait for a asynchronous approval at the synchronizer
            _ <- {
              logger.debug("Now waiting to become active")
              waitForActive(config.synchronizerAlias, sequencerAggregatedInfo)
            }
          } yield ()
        }

      sequencerClient <- sequencerClientFactory
        .create(
          participantId,
          persistentState.sequencedEventStore,
          persistentState.sendTrackerStore,
          RequestSigner(
            synchronizerCryptoApi,
            sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
            loggerFactory,
          ),
          sequencerAggregatedInfo.sequencerConnections,
          sequencerAggregatedInfo.expectedSequencers,
        )
        .leftMap[SynchronizerRegistryError](
          SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )

      _ <- downloadSynchronizerTopologyStateForInitializationIfNeeded(
        syncPersistentStateManager,
        synchronizerId,
        topologyFactory.createInitialTopologySnapshotValidator(
          sequencerAggregatedInfo.staticSynchronizerParameters
        ),
        topologyClient,
        sequencerClient,
        partyNotifier,
        sequencerAggregatedInfo,
      )

      sequencerChannelClientO <- EitherT.fromEither[FutureUnlessShutdown](
        sequencerChannelClientFactoryO.traverse { sequencerChannelClientFactory =>
          sequencerChannelClientFactory
            .create(
              participantId,
              sequencerAggregatedInfo.sequencerConnections,
              sequencerAggregatedInfo.expectedSequencers,
            )
            .leftMap(
              SynchronizerRegistryError.SynchronizerRegistryInternalError
                .InvalidState(_): SynchronizerRegistryError
            )
        }
      )
    } yield SynchronizerHandle(
      synchronizerId,
      config.synchronizerAlias,
      sequencerAggregatedInfo.staticSynchronizerParameters,
      sequencerClient,
      sequencerChannelClientO,
      topologyClient,
      topologyFactory,
      persistentState,
      timeouts,
    )
  }

  private def downloadSynchronizerTopologyStateForInitializationIfNeeded(
      syncPersistentStateManager: SyncPersistentStateManager,
      synchronizerId: SynchronizerId,
      topologySnapshotValidator: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      partyNotifier: LedgerServerPartyNotifier,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    performUnlessClosingEitherUSF("check-for-synchronizer-topology-initialization")(
      syncPersistentStateManager.synchronizerTopologyStateInitFor(
        synchronizerId,
        participantId,
      )
    ).flatMap {
      case None =>
        EitherT.right[SynchronizerRegistryError](FutureUnlessShutdown.unit)
      case Some(topologyInitializationCallback) =>
        topologyInitializationCallback
          .callback(
            topologySnapshotValidator,
            topologyClient,
            sequencerClient,
            sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
          )
          // notify the ledger api server about regular and admin parties contained
          // in the topology snapshot for this synchronizer
          .semiflatMap { storedTopologyTransactions =>
            import cats.syntax.parallel.*
            storedTopologyTransactions.result
              .groupBy(stt => (stt.sequenced, stt.validFrom))
              .toSeq
              .sortBy(_._1)
              .parTraverse_ { case ((sequenced, effective), topologyTransactions) =>
                partyNotifier.observeTopologyTransactions(
                  sequenced,
                  effective,
                  topologyTransactions.map(_.transaction),
                )
              }
          }
          .leftMap[SynchronizerRegistryError](
            SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
          )
    }

  // if participant has provided synchronizer id previously, compare and make sure the synchronizer being
  // connected to is the one expected
  private def verifySynchronizerId(
      config: SynchronizerConnectionConfig,
      synchronizerId: SynchronizerId,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): Either[SynchronizerIdMismatch.Error, Unit] =
    config.synchronizerId match {
      case None => Either.unit
      case Some(configuredSynchronizerId) =>
        Either.cond(
          configuredSynchronizerId == synchronizerId,
          (),
          SynchronizerRegistryError.HandshakeErrors.SynchronizerIdMismatch
            .Error(expected = configuredSynchronizerId, observed = synchronizerId),
        )
    }

  private def sequencerConnectClient(
      synchronizerAlias: SynchronizerAlias,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  ): SequencerConnectClient = {
    // TODO(i12076): Currently it takes the first available connection
    //  here which is already checked for healthiness, but it should maybe check all of them - if they're healthy
    val sequencerConnection = sequencerAggregatedInfo.sequencerConnections.default
    sequencerConnectClientBuilder(synchronizerAlias, sequencerConnection)
  }

  private def isActive(
      synchronizerAlias: SynchronizerAlias,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] = {
    val client = sequencerConnectClient(synchronizerAlias, sequencerAggregatedInfo)
    isActive(synchronizerAlias, client, waitForActive = false).thereafter { _ =>
      client.close()
    }
  }

  private def waitForActive(
      synchronizerAlias: SynchronizerAlias,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] = {
    val client = sequencerConnectClient(synchronizerAlias, sequencerAggregatedInfo)
    isActive(synchronizerAlias, client, waitForActive = true)
      .subflatMap(isActive =>
        Either.cond(
          isActive,
          (),
          SynchronizerRegistryError.ConnectionErrors.ParticipantIsNotActive
            .Error(s"Participant $participantId is not active"): SynchronizerRegistryError,
        )
      )
      .thereafter { _ =>
        client.close()
      }
  }

  private def isActive(
      synchronizerAlias: SynchronizerAlias,
      client: SequencerConnectClient,
      waitForActive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
    client
      .isActive(participantId, synchronizerAlias, waitForActive = waitForActive)
      .leftMap(SynchronizerRegistryHelpers.toSynchronizerRegistryError(synchronizerAlias))

  private def sequencerConnectClientBuilder: SequencerConnectClient.Builder = {
    (synchronizerAlias: SynchronizerAlias, config: SequencerConnection) =>
      SequencerConnectClient(
        synchronizerAlias,
        config,
        participantNodeParameters.processingTimeouts,
        participantNodeParameters.tracing.propagation,
        loggerFactory,
      )
  }
}

object SynchronizerRegistryHelpers {

  private[synchronizer] final case class SynchronizerHandle(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
      staticParameters: StaticSynchronizerParameters,
      sequencer: RichSequencerClient,
      channelSequencerClientO: Option[SequencerChannelClient],
      topologyClient: SynchronizerTopologyClientWithInit,
      topologyFactory: TopologyComponentFactory,
      persistentState: SyncPersistentState,
      timeouts: ProcessingTimeout,
  )

  private def toSynchronizerRegistryError(alias: SynchronizerAlias)(
      error: SequencerConnectClient.Error
  )(implicit loggingContext: ErrorLoggingContext): SynchronizerRegistryError =
    error match {
      case SequencerConnectClient.Error.DeserializationFailure(e) =>
        SynchronizerRegistryError.SynchronizerRegistryInternalError.DeserializationFailure(e)
      case SequencerConnectClient.Error.InvalidResponse(cause) =>
        SynchronizerRegistryError.SynchronizerRegistryInternalError.InvalidResponse(cause, None)
      case SequencerConnectClient.Error.InvalidState(cause) =>
        SynchronizerRegistryError.SynchronizerRegistryInternalError.InvalidState(cause)
      case SequencerConnectClient.Error.Transport(message) =>
        SynchronizerRegistryError.ConnectionErrors.SynchronizerIsNotAvailable.Error(alias, message)
    }

  def fromSynchronizerAliasManagerError(
      error: SynchronizerAliasManager.Error
  )(implicit loggingContext: ErrorLoggingContext): SynchronizerRegistryError =
    error match {
      case SynchronizerAliasManager.GenericError(reason) =>
        SynchronizerRegistryError.HandshakeErrors.HandshakeFailed.Error(reason)
      case SynchronizerAliasManager.SynchronizerAliasDuplication(
            synchronizerId,
            alias,
            previousSynchronizerId,
          ) =>
        SynchronizerRegistryError.HandshakeErrors.SynchronizerAliasDuplication.Error(
          synchronizerId,
          alias,
          previousSynchronizerId,
        )
    }

}
