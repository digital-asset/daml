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
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.DomainRegistryHelpers.DomainHandle
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.HandshakeErrors.SynchronizerIdMismatch
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

trait DomainRegistryHelpers extends FlagCloseable with NamedLogging { this: HasFutureSupervision =>
  def participantId: ParticipantId
  protected def participantNodeParameters: ParticipantNodeParameters

  implicit def ec: ExecutionContextExecutor
  override protected def executionContext: ExecutionContext = ec
  implicit def executionSequencerFactory: ExecutionSequencerFactory
  implicit def materializer: Materializer
  implicit def tracer: Tracer

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  protected def getDomainHandle(
      config: DomainConnectionConfig,
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(
      cryptoApiProvider: SyncCryptoApiProvider,
      clock: Clock,
      testingConfig: TestingConfigInternal,
      recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
      replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
      topologyDispatcher: ParticipantTopologyDispatcher,
      packageDependencyResolver: PackageDependencyResolverUS,
      partyNotifier: LedgerServerPartyNotifier,
      metrics: SynchronizerAlias => SyncDomainMetrics,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, DomainHandle] = {
    import sequencerAggregatedInfo.synchronizerId

    for {
      indexedSynchronizerId <- EitherT
        .right(syncDomainPersistentStateManager.indexedSynchronizerId(synchronizerId))

      _ <- EitherT
        .fromEither[Future](verifySynchronizerId(config, synchronizerId))
        .mapK(FutureUnlessShutdown.outcomeK)

      // fetch or create persistent state for the domain
      persistentState <- syncDomainPersistentStateManager
        .lookupOrCreatePersistentState(
          config.synchronizerAlias,
          indexedSynchronizerId,
          sequencerAggregatedInfo.staticSynchronizerParameters,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      // check and issue the domain trust certificate
      _ <-
        if (config.initializeFromTrustedSynchronizer) {
          EitherTUtil.unit.mapK(FutureUnlessShutdown.outcomeK)
        } else {
          topologyDispatcher
            .trustDomain(synchronizerId)
            .leftMap(
              SynchronizerRegistryError.ConfigurationErrors.CanNotIssueSynchronizerTrustCertificate
                .Error(_)
            )
        }

      domainLoggerFactory = loggerFactory.append(
        "synchronizerId",
        indexedSynchronizerId.synchronizerId.toString,
      )

      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(
          synchronizerId,
          sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
        )
        .toRight(
          SynchronizerRegistryError.DomainRegistryInternalError
            .InvalidState("topology factory for domain is unavailable"): SynchronizerRegistryError
        )
        .toEitherT[FutureUnlessShutdown]

      topologyClient <- EitherT.right(
        performUnlessClosingUSF("create caching client")(
          topologyFactory.createCachingTopologyClient(
            packageDependencyResolver
          )
        )
      )
      _ = cryptoApiProvider.ips.add(topologyClient)

      synchronizerCryptoApi <- EitherT.fromEither[FutureUnlessShutdown](
        cryptoApiProvider
          .forSynchronizer(synchronizerId, sequencerAggregatedInfo.staticSynchronizerParameters)
          .toRight(
            SynchronizerRegistryError.DomainRegistryInternalError
              .InvalidState("crypto api for domain is unavailable"): SynchronizerRegistryError
          )
      )

      (sequencerClientFactory, sequencerChannelClientFactoryO) = {
        // apply optional domain specific overrides to the nodes general sequencer client config
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
            domainLoggerFactory,
            ProtocolVersionCompatibility.supportedProtocols(participantNodeParameters),
          ),
          Option.when(
            participantNodeParameters.unsafeEnableOnlinePartyReplication
          )(
            new SequencerChannelClientFactory(
              synchronizerId,
              synchronizerCryptoApi,
              cryptoApiProvider.crypto,
              sequencerClientConfig,
              participantNodeParameters.tracing.propagation,
              sequencerAggregatedInfo.staticSynchronizerParameters,
              participantNodeParameters.processingTimeouts,
              clock,
              domainLoggerFactory,
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
            success <- topologyDispatcher.onboardToDomain(
              synchronizerId,
              config.synchronizerAlias,
              client,
              sequencerAggregatedInfo.staticSynchronizerParameters.protocolVersion,
            )
            _ <- EitherT.cond[FutureUnlessShutdown](
              success,
              (),
              SynchronizerRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
                s"Domain ${config.synchronizerAlias} has rejected our on-boarding attempt"
              ),
            )
            // make sure the participant is immediately active after pushing our topology,
            // or whether we have to stop here to wait for a asynchronous approval at the domain
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
        syncDomainPersistentStateManager,
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
              SynchronizerRegistryError.DomainRegistryInternalError
                .InvalidState(_): SynchronizerRegistryError
            )
        }
      )
    } yield DomainHandle(
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
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
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
      syncDomainPersistentStateManager.synchronizerTopologyStateInitFor(
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
  private def verifySynchronizerId(config: DomainConnectionConfig, synchronizerId: SynchronizerId)(
      implicit loggingContext: ErrorLoggingContext
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
    isActive(synchronizerAlias, client, false).thereafter { _ =>
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
    isActive(synchronizerAlias, client, true)
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
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(synchronizerAlias))

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

object DomainRegistryHelpers {

  private[synchronizer] final case class DomainHandle(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
      staticParameters: StaticSynchronizerParameters,
      sequencer: RichSequencerClient,
      channelSequencerClientO: Option[SequencerChannelClient],
      topologyClient: SynchronizerTopologyClientWithInit,
      topologyFactory: TopologyComponentFactory,
      domainPersistentState: SyncDomainPersistentState,
      timeouts: ProcessingTimeout,
  )

  def toDomainRegistryError(alias: SynchronizerAlias)(
      error: SequencerConnectClient.Error
  )(implicit loggingContext: ErrorLoggingContext): SynchronizerRegistryError =
    error match {
      case SequencerConnectClient.Error.DeserializationFailure(e) =>
        SynchronizerRegistryError.DomainRegistryInternalError.DeserializationFailure(e)
      case SequencerConnectClient.Error.InvalidResponse(cause) =>
        SynchronizerRegistryError.DomainRegistryInternalError.InvalidResponse(cause, None)
      case SequencerConnectClient.Error.InvalidState(cause) =>
        SynchronizerRegistryError.DomainRegistryInternalError.InvalidState(cause)
      case SequencerConnectClient.Error.Transport(message) =>
        SynchronizerRegistryError.ConnectionErrors.DomainIsNotAvailable.Error(alias, message)
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
