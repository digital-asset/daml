// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.config.SequencerNodeParameters
import com.digitalasset.canton.domain.sequencing.sequencer.{
  DirectSequencerClientTransport,
  Sequencer,
}
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerAdministrationService
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencer.admin.v30.SequencerAdministrationServiceGrpc
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.sequencing.protocol.SequencedEvent
import com.digitalasset.canton.sequencing.{
  BoxedEnvelope,
  HandlerResult,
  SubscriptionStart,
  UnsignedEnvelopeBox,
  UnsignedProtocolEventHandler,
}
import com.digitalasset.canton.store.{
  IndexedDomain,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStateForInitializationService, TopologyStore}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.traffic.TrafficControlProcessor
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

/* We create a sequencer client and connect it to a topology client
 * to power sequencer authentication.
 */
// TODO(#15161): Fold into SequencerRuntime base class
class SequencerRuntimeForSeparateNode(
    sequencerId: SequencerId,
    sequencer: Sequencer,
    staticDomainParameters: StaticDomainParameters,
    localNodeParameters: SequencerNodeParameters,
    publicServerConfig: PublicServerConfig,
    timeTrackerConfig: DomainTimeTrackerConfig,
    testingConfig: TestingConfigInternal,
    metrics: SequencerMetrics,
    indexedDomain: IndexedDomain,
    syncCrypto: DomainSyncCryptoClient,
    topologyStore: TopologyStore[DomainStore],
    topologyClient: DomainTopologyClientWithInit,
    topologyProcessor: TopologyTransactionProcessor,
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
    memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
    topologyStateForInitializationService: Option[TopologyStateForInitializationService],
    maybeDomainOutboxFactory: Option[DomainOutboxFactorySingleCreate],
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
      indexedDomain.domainId,
      syncCrypto,
      topologyClient,
      topologyManagerStatusO,
      storage,
      clock,
      authenticationConfig,
      additionalAdminServiceFactory,
      staticMembersToRegister,
      futureSupervisor,
      memberAuthenticationServiceFactory,
      topologyStateForInitializationService,
      loggerFactory,
    ) {

  private val sequencedEventStore =
    SequencedEventStore(
      storage,
      indexedDomain,
      staticDomainParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  private val client: SequencerClient =
    new SequencerClientImplPekko[DirectSequencerClientTransport.SubscriptionError](
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
    SequencerCounterTrackerStore(
      storage,
      indexedDomain,
      timeouts,
      loggerFactory,
    )

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

  private val topologyHandler = topologyProcessor.createHandler(domainId)
  private val trafficProcessor =
    new TrafficControlProcessor(
      syncCrypto,
      domainId,
      sequencer.rateLimitManager.flatMap(_.balanceKnownUntil),
      loggerFactory,
    )

  sequencer.rateLimitManager.foreach(_.balanceUpdateSubscriber.foreach(trafficProcessor.subscribe))

  // TODO(i17434): Use topologyHandler.combineWith(trafficProcessorHandler)
  private def handler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {
      override def name: String = s"sequencer-runtime-$domainId"

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        Seq(
          topologyProcessor.subscriptionStartsAt(start, domainTimeTracker),
          trafficProcessor.subscriptionStartsAt(start, domainTimeTracker),
        ).sequence_

      override def apply(
          tracedEvents: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult =
        tracedEvents.withTraceContext { implicit traceContext => events =>
          NonEmpty.from(events).fold(HandlerResult.done)(handle)
        }

      private def handle(tracedEvents: NonEmpty[Seq[Traced[SequencedEvent[DefaultOpenEnvelope]]]])(
          implicit traceContext: TraceContext
      ): HandlerResult = {
        for {
          topology <- topologyHandler(Traced(tracedEvents))
          _ <- trafficProcessor.handle(tracedEvents)
        } yield topology
      }
    }

  private val eventHandler = StripSignature(handler(domainId))

  private val sequencerAdministrationService =
    new GrpcSequencerAdministrationService(
      sequencer,
      client,
      topologyStore,
      topologyClient,
      timeTracker,
      staticDomainParameters,
      loggerFactory,
    )

  override def registerAdminGrpcServices(
      register: ServerServiceDefinition => Unit
  ): Unit = {
    super.registerAdminGrpcServices(register)
    register(
      SequencerAdministrationServiceGrpc.bindService(
        sequencerAdministrationService,
        executionContext,
      )
    )
  }

  def initializeAll()(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      _ <- initialize(topologyInitIsCompleted = false)
      _ = logger.debug("Subscribing topology client within sequencer runtime")
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

  override def onClosed(): Unit = {
    Lifecycle.close(
      Lifecycle.toCloseableOption(sequencer.rateLimitManager),
      timeTracker,
      syncCrypto,
      topologyClient,
      client,
      topologyProcessor,
      topologyManagerSequencerCounterTrackerStore,
      sequencedEventStore,
    )(logger)
    super.onClosed()
  }

}
