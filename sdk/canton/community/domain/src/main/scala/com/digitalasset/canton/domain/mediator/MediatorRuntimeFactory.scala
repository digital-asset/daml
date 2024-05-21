// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.mediator.store.{
  FinalizedResponseStore,
  MediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorAdministrationServiceGrpc
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, StaticGrpcServices}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.admin.v30.DomainTimeServiceGrpc
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, GrpcDomainTimeService}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

/** Mediator component and its supporting services */
trait MediatorRuntime extends FlagCloseable {
  def mediator: Mediator

  final def registerAdminGrpcServices(register: ServerServiceDefinition => Unit): Unit = {
    register(timeService)
    register(enterpriseAdministrationService)
    register(apiInfoService)
  }

  def timeService: ServerServiceDefinition
  def enterpriseAdministrationService: ServerServiceDefinition
  def apiInfoService: ServerServiceDefinition
  def domainOutbox: DomainOutboxHandle

  def start()(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      err: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _ <- EitherT.right(mediator.startFUS())
    // start the domainOutbox only after the mediator has been started, otherwise
    // the future returned by startup will not be complete, because any topology transactions pushed to the
    // domain aren't actually processed until after the runtime is up and ... running
    _ <- domainOutbox.startup()
  } yield ()

  override protected def onClosed(): Unit = {
    Lifecycle.close(domainOutbox, mediator)(logger)
  }
}

private[mediator] class CommunityMediatorRuntime(
    override val mediator: Mediator,
    override val domainOutbox: DomainOutboxHandle,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit protected val ec: ExecutionContext)
    extends MediatorRuntime
    with NamedLogging {
  override val timeService: ServerServiceDefinition = DomainTimeServiceGrpc.bindService(
    GrpcDomainTimeService.forDomainEntity(mediator.domain, mediator.timeTracker, loggerFactory),
    ec,
  )
  override val enterpriseAdministrationService: ServerServiceDefinition =
    StaticGrpcServices.notSupportedByCommunity(
      MediatorAdministrationServiceGrpc.SERVICE,
      logger,
    )
  override val apiInfoService: ServerServiceDefinition =
    ApiInfoServiceGrpc.bindService(new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi), ec)
}

trait MediatorRuntimeFactory {
  def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: RichSequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyTransactionProcessor: TopologyTransactionProcessor,
      topologyManagerStatus: TopologyManagerStatus,
      domainOutboxFactory: DomainOutboxFactory,
      timeTracker: DomainTimeTracker,
      nodeParameters: CantonNodeParameters,
      protocolVersion: ProtocolVersion,
      clock: Clock,
      metrics: MediatorMetrics,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime]
}

object CommunityMediatorRuntimeFactory extends MediatorRuntimeFactory {
  override def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: RichSequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyTransactionProcessor: TopologyTransactionProcessor,
      topologyManagerStatus: TopologyManagerStatus,
      domainOutboxFactory: DomainOutboxFactory,
      timeTracker: DomainTimeTracker,
      nodeParameters: CantonNodeParameters,
      protocolVersion: ProtocolVersion,
      clock: Clock,
      metrics: MediatorMetrics,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime] = {
    val finalizedResponseStore = FinalizedResponseStore(
      storage,
      syncCrypto.pureCrypto,
      protocolVersion,
      nodeParameters.processingTimeouts,
      loggerFactory,
    )
    val deduplicationStore =
      MediatorDeduplicationStore(
        mediatorId,
        storage,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    val state =
      new MediatorState(
        finalizedResponseStore,
        deduplicationStore,
        clock,
        metrics,
        protocolVersion,
        nodeParameters.cachingConfigs.finalizedMediatorConfirmationRequests,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )

    val outbox = domainOutboxFactory.create(
      protocolVersion,
      topologyClient,
      sequencerClient,
      clock,
      loggerFactory,
    )
    EitherT.pure[FutureUnlessShutdown, String](
      new CommunityMediatorRuntime(
        new Mediator(
          domainId,
          mediatorId,
          sequencerClient,
          topologyClient,
          syncCrypto,
          topologyTransactionProcessor,
          topologyManagerStatus,
          outbox,
          timeTracker,
          state,
          sequencerCounterTrackerStore,
          sequencedEventStore,
          nodeParameters,
          protocolVersion,
          clock,
          metrics,
          loggerFactory,
        ),
        outbox,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    )
  }
}
