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
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal, TopologyConfig}
import com.digitalasset.canton.crypto.{
  SyncCryptoApiParticipantProvider,
  SynchronizerCrypto,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
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
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.channel.{
  SequencerChannelClient,
  SequencerChannelClientFactory,
}
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnectionXPool}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator
import com.digitalasset.canton.topology.store.PackageDependencyResolver
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Success

trait SynchronizerRegistryHelpers extends FlagCloseable with NamedLogging with HasCloseContext {
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
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      syncPersistentStateManager: SyncPersistentStateManager,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
      connectionPool: SequencerConnectionXPool,
  )(
      cryptoApiProvider: SyncCryptoApiParticipantProvider,
      clock: Clock,
      testingConfig: TestingConfigInternal,
      topologyConfig: TopologyConfig,
      recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
      replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
      topologyDispatcher: ParticipantTopologyDispatcher,
      packageDependencyResolver: PackageDependencyResolver,
      partyNotifier: LedgerServerPartyNotifier,
      metrics: SynchronizerAlias => ConnectedSynchronizerMetrics,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SynchronizerHandle] = {
    import sequencerAggregatedInfo.psid

    val synchronizerHandleET = for {
      physicalSynchronizerIdx <- EitherT
        .right(syncPersistentStateManager.getPhysicalSynchronizerIdx(psid))

      synchronizerIdx <- EitherT
        .right(syncPersistentStateManager.getSynchronizerIdx(psid.logical))

      synchronizerTopologyStoreId <- EitherT.right(
        syncPersistentStateManager.getSynchronizerTopologyStoreId(psid)
      )

      _ <- EitherT
        .fromEither[Future](verifySynchronizerId(config, psid))
        .mapK(FutureUnlessShutdown.outcomeK)

      // fetch or create persistent state for the synchronizer
      persistentState <- syncPersistentStateManager
        .lookupOrCreatePersistentState(
          config.synchronizerAlias,
          physicalSynchronizerIdx,
          synchronizerTopologyStoreId,
          synchronizerIdx,
          sequencerAggregatedInfo.staticSynchronizerParameters,
        )

      // check and issue the synchronizer trust certificate
      _ <- EitherTUtil.ifThenET(!config.initializeFromTrustedSynchronizer)(
        topologyDispatcher.trustSynchronizer(psid)
      )

      synchronizerLoggerFactory = loggerFactory.append(
        "psid",
        physicalSynchronizerIdx.toString,
      )

      topologyFactory <- syncPersistentStateManager
        .topologyFactoryFor(psid)
        .toRight(
          SynchronizerRegistryError.SynchronizerRegistryInternalError
            .InvalidState(
              "topology factory for synchronizer is unavailable"
            ): SynchronizerRegistryError
        )
        .toEitherT[FutureUnlessShutdown]

      topologyClient <- EitherT.right(
        synchronizeWithClosing("create caching client")(
          topologyFactory.createCachingTopologyClient(
            packageDependencyResolver,
            synchronizerPredecessor,
          )
        )
      )

      // If the connection to a synchronizer fails, the topology client and crypto cache are not removed from the cache.
      // This is why we want to clear the topology client and crypto caches before creating the new clients.
      _ = cryptoApiProvider.removeAndClose(psid)
      _ = cryptoApiProvider.ips.add(topologyClient)

      synchronizerCryptoApi <- EitherT.fromEither[FutureUnlessShutdown](
        cryptoApiProvider
          .forSynchronizer(
            psid,
            sequencerAggregatedInfo.staticSynchronizerParameters,
          )
          .toRight(
            SynchronizerRegistryError.SynchronizerRegistryInternalError
              .InvalidState("crypto api for synchronizer is unavailable"): SynchronizerRegistryError
          )
      )

      synchronizerCrypto = SynchronizerCrypto(
        cryptoApiProvider.crypto,
        sequencerAggregatedInfo.staticSynchronizerParameters,
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
            s"${participantId.show.stripSuffix("...")}-${psid.toProtoPrimitive}"
          recordingConfig.setFilename(namePrefix)
        }

        def ifParticipant[C](configO: Option[C]): Member => Option[C] = {
          case _: ParticipantId => configO
          case _ => None
        }
        (
          SequencerClientFactory(
            psid,
            synchronizerCryptoApi,
            synchronizerCrypto,
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
                psid,
                synchronizerCryptoApi,
                synchronizerCrypto,
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
            s"Participant is not yet active on synchronizer $psid. Initializing topology"
          )
          val client = sequencerConnectClient(config.synchronizerAlias, sequencerAggregatedInfo)
          for {
            success <- topologyDispatcher.onboardToSynchronizer(
              psid,
              config.synchronizerAlias,
              client,
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
          synchronizerPredecessor,
          sequencerAggregatedInfo.expectedSequencersO,
          connectionPool,
        )
        .leftMap[SynchronizerRegistryError](
          SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )

      _ <- downloadSynchronizerTopologyStateForInitializationIfNeeded(
        syncPersistentStateManager,
        psid,
        topologyFactory.createInitialTopologySnapshotValidator(topologyConfig),
        topologyClient,
        sequencerClient,
        partyNotifier,
        sequencerAggregatedInfo,
      ).thereafter {
        case Success(AbortedDueToShutdown) =>
          /*
           Without that, the synchronizer handler is not returned, and never closed.
           In particular, the sequencer client is never closed.
           */
          sequencerClient.close()

        case _ => ()
      }

      sequencerChannelClientO <- EitherT.fromEither[FutureUnlessShutdown](
        sequencerChannelClientFactoryO.traverse { sequencerChannelClientFactory =>
          sequencerChannelClientFactory
            .create(
              participantId,
              sequencerAggregatedInfo.sequencerConnections,
              sequencerAggregatedInfo.expectedSequencersO
                .getOrElse(ErrorUtil.invalidState("`expectedSequencersO` should be defined")),
            )
            .leftMap(
              SynchronizerRegistryError.SynchronizerRegistryInternalError
                .InvalidState(_): SynchronizerRegistryError
            )
        }
      )
    } yield SynchronizerHandle(
      psid,
      config.synchronizerAlias,
      sequencerAggregatedInfo.staticSynchronizerParameters,
      sequencerClient,
      sequencerChannelClientO,
      topologyClient,
      topologyFactory,
      persistentState,
      synchronizerCryptoApi,
      timeouts,
    )

    synchronizerHandleET
  }

  private def downloadSynchronizerTopologyStateForInitializationIfNeeded(
      syncPersistentStateManager: SyncPersistentStateManager,
      synchronizerId: PhysicalSynchronizerId,
      topologySnapshotValidator: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      partyNotifier: LedgerServerPartyNotifier,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    synchronizeWithClosing("check-for-synchronizer-topology-initialization")(
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
      synchronizerId: PhysicalSynchronizerId,
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
    // TODO(i27618): modify to use the connection pool
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
      synchronizerId: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
      staticParameters: StaticSynchronizerParameters,
      sequencer: RichSequencerClient,
      channelSequencerClientO: Option[SequencerChannelClient],
      topologyClient: SynchronizerTopologyClientWithInit,
      topologyFactory: TopologyComponentFactory,
      persistentState: SyncPersistentState,
      syncCryptoApi: SynchronizerCryptoClient,
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
    }
}
