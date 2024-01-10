// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.Eval
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoHandshakeValidator, SyncCryptoApiProvider}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.DomainRegistryError.HandshakeErrors.DomainIdMismatch
import com.digitalasset.canton.participant.domain.DomainRegistryHelpers.DomainHandle
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
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ResourceUtil}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}
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
      cryptoConfig: CryptoConfig,
      clock: Clock,
      testingConfig: TestingConfigInternal,
      recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
      replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
      topologyDispatcher: ParticipantTopologyDispatcherCommon,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      metrics: DomainAlias => SyncDomainMetrics,
      agreementClient: AgreementClient,
      participantSettings: Eval[ParticipantSettingsLookup],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, DomainHandle] = {
    import sequencerAggregatedInfo.domainId

    for {
      indexedDomainId <- EitherT
        .right(syncDomainPersistentStateManager.indexedDomainId(domainId))
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- CryptoHandshakeValidator
        .validate(sequencerAggregatedInfo.staticDomainParameters, cryptoConfig)
        .leftMap(DomainRegistryError.HandshakeErrors.DomainCryptoHandshakeFailed.Error(_))
        .toEitherT[FutureUnlessShutdown]

      _ <- EitherT
        .fromEither[Future](verifyDomainId(config, domainId))
        .mapK(FutureUnlessShutdown.outcomeK)

      acceptedAgreement <- agreementClient
        .isRequiredAgreementAccepted(domainId)
        .leftMap(e =>
          DomainRegistryError.HandshakeErrors.ServiceAgreementAcceptanceFailed.Error(e.reason)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      // fetch or create persistent state for the domain
      persistentState <- syncDomainPersistentStateManager
        .lookupOrCreatePersistentState(
          config.domain,
          indexedDomainId,
          sequencerAggregatedInfo.staticDomainParameters,
          participantSettings,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      // check and issue the domain trust certificate
      _ <-
        if (config.initializeFromTrustedDomain) {
          EitherTUtil.unit.mapK(FutureUnlessShutdown.outcomeK)
        } else {
          topologyDispatcher
            .trustDomain(
              domainId,
              sequencerAggregatedInfo.staticDomainParameters,
            )
            .leftMap(
              DomainRegistryError.ConfigurationErrors.CanNotIssueDomainTrustCertificate.Error(_)
            )
        }

      domainLoggerFactory = loggerFactory.append("domainId", indexedDomainId.domainId.toString)

      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(domainId)
        .toRight(
          DomainRegistryError.DomainRegistryInternalError
            .InvalidState("topology factory for domain is unavailable"): DomainRegistryError
        )
        .toEitherT[FutureUnlessShutdown]

      topologyClient <- EitherT.right(
        performUnlessClosingF("create caching client")(
          topologyFactory.createCachingTopologyClient(
            sequencerAggregatedInfo.staticDomainParameters.protocolVersion,
            packageDependencies,
          )
        )
      )
      _ = cryptoApiProvider.ips.add(topologyClient)

      domainCryptoApi <- EitherT.fromEither[FutureUnlessShutdown](
        cryptoApiProvider
          .forDomain(domainId)
          .toRight(
            DomainRegistryError.DomainRegistryInternalError
              .InvalidState("crypto api for domain is unavailable"): DomainRegistryError
          )
      )

      sequencerClientFactory = {
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
            s"${participantId.show.stripSuffix("...")}-${domainId.show.stripSuffix("...")}"
          recordingConfig.setFilename(namePrefix)
        }

        def ifParticipant[C](configO: Option[C]): Member => Option[C] = {
          case _: ParticipantId => configO
          case _ => None // unauthenticated members don't need it
        }
        SequencerClientFactory(
          domainId,
          domainCryptoApi,
          cryptoApiProvider.crypto,
          acceptedAgreement.map(_.id),
          sequencerClientConfig,
          participantNodeParameters.tracing.propagation,
          testingConfig,
          sequencerAggregatedInfo.staticDomainParameters,
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
          metrics(config.domain).sequencerClient,
          participantNodeParameters.loggingConfig,
          domainLoggerFactory,
          ProtocolVersionCompatibility.supportedProtocolsParticipant(participantNodeParameters),
          participantNodeParameters.protocolConfig.minimumProtocolVersion,
        )
      }

      // TODO(i12906): We should check for the activeness only from the honest sequencers.
      active <- isActive(config.domain, sequencerAggregatedInfo)

      // if the participant is being restarted and has completed topology initialization previously
      // then we can skip it
      _ <-
        if (active) EitherT.pure[FutureUnlessShutdown, DomainRegistryError](())
        else {
          logger.debug(
            s"Participant is not yet active on domain ${domainId}. Initialising topology"
          )
          for {
            success <- topologyDispatcher.onboardToDomain(
              domainId,
              config.domain,
              config.timeTracker,
              sequencerAggregatedInfo.sequencerConnections,
              sequencerClientFactory,
              sequencerAggregatedInfo.staticDomainParameters.protocolVersion,
              sequencerAggregatedInfo.expectedSequencers,
            )
            _ <- EitherT.cond[FutureUnlessShutdown](
              success,
              (),
              DomainRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
                s"Domain ${config.domain} has rejected our on-boarding attempt"
              ),
            )
            // make sure the participant is immediately active after pushing our topology,
            // or whether we have to stop here to wait for a asynchronous approval at the domain
            _ <- {
              logger.debug("Now waiting to become active")
              waitForActive(config.domain, sequencerAggregatedInfo)
            }
          } yield ()
        }

      requestSigner = RequestSigner(
        domainCryptoApi,
        sequencerAggregatedInfo.staticDomainParameters.protocolVersion,
      )
      _ <- downloadDomainTopologyStateForInitializationIfNeeded(
        // TODO(i12076): Download topology state from one of the sequencers based on the health
        sequencerAggregatedInfo.sequencerConnections.default,
        syncDomainPersistentStateManager,
        domainId,
        topologyClient,
        sequencerClientFactory,
        requestSigner,
        sequencerAggregatedInfo.staticDomainParameters.protocolVersion,
      )
      sequencerClient <- sequencerClientFactory
        .create(
          participantId,
          persistentState.sequencedEventStore,
          persistentState.sendTrackerStore,
          requestSigner,
          sequencerAggregatedInfo.sequencerConnections,
          sequencerAggregatedInfo.expectedSequencers,
        )
        .leftMap[DomainRegistryError](
          DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield DomainHandle(
      domainId,
      config.domain,
      sequencerAggregatedInfo.staticDomainParameters,
      sequencerClient,
      topologyClient,
      topologyFactory,
      persistentState,
      timeouts,
    )
  }

  private def downloadDomainTopologyStateForInitializationIfNeeded(
      sequencerConnection: SequencerConnection,
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
      domainId: DomainId,
      topologyClient: DomainTopologyClientWithInit,
      sequencerClientFactory: SequencerClientTransportFactory,
      requestSigner: RequestSigner,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
      materializer: Materializer,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {

    performUnlessClosingEitherU("check-for-domain-topology-initialization")(
      syncDomainPersistentStateManager.domainTopologyStateInitFor(
        domainId,
        participantId,
      )
    ).flatMap {
      case None =>
        EitherT.right[DomainRegistryError](FutureUnlessShutdown.unit)
      case Some(topologyInitializationCallback) =>
        performUnlessClosingEitherU(
          name = "sequencer-transport-for-downloading-essential-state"
        )(
          sequencerClientFactory
            .makeTransport(
              sequencerConnection,
              participantId,
              requestSigner,
            )
        )
          .flatMap(transport =>
            performUnlessClosingEitherU(
              "downloading-essential-topology-state"
            )(
              ResourceUtil.withResourceM(transport)(
                topologyInitializationCallback
                  .callback(topologyClient, _, protocolVersion)
              )
            )
          )
          .leftMap[DomainRegistryError](
            DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
          )
    }
  }

  // if participant has provided domain id previously, compare and make sure the domain being
  // connected to is the one expected
  private def verifyDomainId(config: DomainConnectionConfig, domainId: DomainId)(implicit
      loggingContext: ErrorLoggingContext
  ): Either[DomainIdMismatch.Error, Unit] =
    config.domainId match {
      case None => Right(())
      case Some(configuredDomainId) =>
        Either.cond(
          configuredDomainId == domainId,
          (),
          DomainRegistryError.HandshakeErrors.DomainIdMismatch
            .Error(expected = configuredDomainId, observed = domainId),
        )
    }

  private def sequencerConnectClient(
      sequencerAggregatedInfo: SequencerAggregatedInfo
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, SequencerConnectClient] = {
    // TODO(i12076): Currently it takes the first available connection
    //  here which is already checked for healthiness, but it should maybe check all of them - if they're healthy
    val sequencerConnection = sequencerAggregatedInfo.sequencerConnections.default
    sequencerConnectClientBuilder(sequencerConnection)
      .leftMap(err =>
        DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(err.message)
      )
      .mapK(
        FutureUnlessShutdown.outcomeK
      )
      .leftWiden[DomainRegistryError]
  }

  private def isActive(domainAlias: DomainAlias, sequencerAggregatedInfo: SequencerAggregatedInfo)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    sequencerConnectClient(sequencerAggregatedInfo)
      .flatMap(client =>
        isActive(domainAlias, client, false)
          .thereafter { _ =>
            client.close()
          }
      )

  private def waitForActive(
      domainAlias: DomainAlias,
      sequencerAggregatedInfo: SequencerAggregatedInfo,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {
    sequencerConnectClient(sequencerAggregatedInfo)
      .flatMap(client =>
        isActive(domainAlias, client, true)
          .flatMap { isActive =>
            EitherT
              .cond[Future](
                isActive,
                (),
                DomainRegistryError.ConnectionErrors.ParticipantIsNotActive
                  .Error(s"Participant $participantId is not active"),
              )
              .leftWiden[DomainRegistryError]
              .mapK(FutureUnlessShutdown.outcomeK)
          }
          .thereafter { _ =>
            client.close()
          }
      )
  }

  private def isActive(
      domainAlias: DomainAlias,
      client: SequencerConnectClient,
      waitForActive: Boolean = true,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    client
      .isActive(participantId, waitForActive = waitForActive)
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  private def sequencerConnectClientBuilder: SequencerConnectClient.Builder = {
    (config: SequencerConnection) =>
      SequencerConnectClient(
        config,
        participantNodeParameters.processingTimeouts,
        participantNodeParameters.tracing.propagation,
        loggerFactory,
      )
  }
}

object DomainRegistryHelpers {

  private[domain] final case class DomainHandle(
      domainId: DomainId,
      alias: DomainAlias,
      staticParameters: StaticDomainParameters,
      sequencer: RichSequencerClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyFactory: TopologyComponentFactory,
      domainPersistentState: SyncDomainPersistentState,
      timeouts: ProcessingTimeout,
  )

  def toDomainRegistryError(alias: DomainAlias)(
      error: SequencerConnectClient.Error
  )(implicit loggingContext: ErrorLoggingContext): DomainRegistryError =
    error match {
      case SequencerConnectClient.Error.DeserializationFailure(e) =>
        DomainRegistryError.DomainRegistryInternalError.DeserializationFailure(e)
      case SequencerConnectClient.Error.InvalidResponse(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidResponse(cause, None)
      case SequencerConnectClient.Error.InvalidState(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidState(cause)
      case SequencerConnectClient.Error.Transport(message) =>
        DomainRegistryError.ConnectionErrors.DomainIsNotAvailable.Error(alias, message)
    }

  def fromDomainAliasManagerError(
      error: DomainAliasManager.Error
  )(implicit loggingContext: ErrorLoggingContext): DomainRegistryError =
    error match {
      case DomainAliasManager.GenericError(reason) =>
        DomainRegistryError.HandshakeErrors.HandshakeFailed.Error(reason)
      case DomainAliasManager.DomainAliasDuplication(domainId, alias, previousDomainId) =>
        DomainRegistryError.HandshakeErrors.DomainAliasDuplication.Error(
          domainId,
          alias,
          previousDomainId,
        )
    }

}
