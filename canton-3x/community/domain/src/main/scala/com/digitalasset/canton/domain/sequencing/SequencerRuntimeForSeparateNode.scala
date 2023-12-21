// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.admin.v0.TopologyBootstrapServiceGrpc
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.sequencer.{
  DirectSequencerClientTransport,
  Sequencer,
}
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerTopologyBootstrapService
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorCommon
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

/* If we're running separately from the domain node we create a sequencer client and connect it to a topology client
 * to power sequencer authentication.
 */
class SequencerRuntimeForSeparateNode(
    sequencerId: SequencerId,
    sequencer: Sequencer,
    staticDomainParameters: StaticDomainParameters,
    localNodeParameters: CantonNodeWithSequencerParameters,
    publicServerConfig: PublicServerConfig,
    timeTrackerConfig: DomainTimeTrackerConfig,
    testingConfig: TestingConfigInternal,
    metrics: SequencerMetrics,
    domainId: DomainId,
    syncCrypto: DomainSyncCryptoClient,
    topologyClient: DomainTopologyClientWithInit,
    topologyProcessor: TopologyTransactionProcessorCommon,
    topologyManagerStatusO: Option[TopologyManagerStatus],
    override val mediatorsProcessParticipantTopologyRequests: Boolean,
    initializationEffective: Future[Unit],
    initializedAtHead: => Future[Boolean],
    storage: Storage,
    clock: Clock,
    authenticationConfig: SequencerAuthenticationConfig,
    additionalAdminServiceFactory: Sequencer => Option[ServerServiceDefinition],
    staticMembersToRegister: Seq[Member],
    futureSupervisor: FutureSupervisor,
    agreementManager: Option[ServiceAgreementManager],
    memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
    topologyStateForInitializationService: Option[TopologyStateForInitializationService],
    maybeDomainOutboxFactory: Option[DomainOutboxXFactorySingleCreate],
    clientDiscriminator: SequencerClientDiscriminator,
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    tracer: Tracer,
    actorSystem: ActorSystem,
    traceContext: TraceContext,
) extends SequencerRuntime(
      sequencerId,
      sequencer,
      staticDomainParameters,
      localNodeParameters,
      publicServerConfig,
      metrics,
      domainId,
      syncCrypto,
      topologyClient,
      topologyManagerStatusO,
      storage,
      clock,
      authenticationConfig,
      additionalAdminServiceFactory,
      staticMembersToRegister,
      futureSupervisor,
      agreementManager,
      memberAuthenticationServiceFactory,
      topologyStateForInitializationService,
      loggerFactory,
    ) {

  private val sequencedEventStore =
    SequencedEventStore(
      storage,
      clientDiscriminator,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  private val client: SequencerClient = new SequencerClientImpl(
    domainId,
    sequencerId,
    SequencerTransports.default(
      sequencerId,
      new DirectSequencerClientTransport(
        sequencer,
        localNodeParameters.processingTimeouts,
        loggerFactory,
      ),
    ),
    localNodeParameters.sequencerClient,
    testingConfig,
    staticDomainParameters.protocolVersion,
    sequencerDomainParamsLookup,
    localNodeParameters.processingTimeouts,
    // Since the sequencer runtime trusts itself, there is no point in validating the events.
    SequencedEventValidatorFactory.noValidation(domainId, warn = false),
    clock,
    RequestSigner(syncCrypto, staticDomainParameters.protocolVersion),
    sequencedEventStore,
    new SendTracker(
      Map(),
      SendTrackerStore(storage),
      metrics.sequencerClient,
      loggerFactory,
      timeouts,
    ),
    metrics.sequencerClient,
    None,
    replayEnabled = false,
    syncCrypto.pureCrypto,
    localNodeParameters.loggingConfig,
    loggerFactory,
    futureSupervisor,
    sequencer.firstSequencerCounterServeableForSequencer,
  )
  private val timeTracker = DomainTimeTracker(
    timeTrackerConfig,
    clock,
    client,
    staticDomainParameters.protocolVersion,
    timeouts,
    loggerFactory,
  )

  private val topologyManagerSequencerCounterTrackerStore =
    SequencerCounterTrackerStore(storage, clientDiscriminator, timeouts, loggerFactory)

  override protected lazy val domainOutboxO: Option[DomainOutboxHandle] =
    maybeDomainOutboxFactory
      .map(
        _.createOnlyOnce(
          staticDomainParameters.protocolVersion,
          topologyClient,
          client,
          clock,
          loggerFactory,
        )
      )

  private val eventHandler = StripSignature(topologyProcessor.createHandler(domainId))

  def initializeAll()(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      _ <- initialize(topologyInitIsCompleted = false)
      _ = logger.debug(
        s"Subscribing topology client within sequencer runtime for ${clientDiscriminator}"
      )
      _ <- EitherT.right(
        client.subscribeTracking(
          topologyManagerSequencerCounterTrackerStore,
          DiscardIgnoredEvents(loggerFactory) {
            EnvelopeOpener(staticDomainParameters.protocolVersion, syncCrypto.pureCrypto)(
              eventHandler
            )
          },
          timeTracker,
        )
      )
      _ <- domainOutboxO
        .map(_.startup().onShutdown(Right(())))
        .getOrElse(EitherT.rightT[Future, String](()))
    } yield {
      import scala.util.chaining.*
      // if we're a separate sequencer node assume we should wait for our local topology client to observe
      // the required topology transactions to at least authorize the domain members
      initializationEffective
        .tap(_ => isTopologyInitializedPromise.success(()))
        .discard // we unlock the waiting future in any case
    }
  }

  override def registerAdminGrpcServices(
      register: ServerServiceDefinition => Unit
  )(implicit ec: ExecutionContext): Unit = {
    super.registerAdminGrpcServices(register)(ec)

    register(
      TopologyBootstrapServiceGrpc
        .bindService(
          new GrpcSequencerTopologyBootstrapService(
            domainId,
            staticDomainParameters.protocolVersion,
            syncCrypto,
            client,
            // intentionally just querying whether the topology client is initialized, and not waiting for it to complete
            () => initializedAtHead,
            loggerFactory,
            futureSupervisor,
            timeouts,
          )(ec),
          executionContext,
        )
    )
  }

  override def onClosed(): Unit = {
    Lifecycle.close(
      timeTracker,
      topologyClient,
      client,
      topologyProcessor,
      topologyManagerSequencerCounterTrackerStore,
      sequencedEventStore,
    )(logger)
    super.onClosed()
  }

}
