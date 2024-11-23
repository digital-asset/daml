// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.mediator.service.GrpcMediatorAdministrationService
import com.digitalasset.canton.domain.mediator.store.{
  FinalizedResponseStore,
  MediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorAdministrationServiceGrpc
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
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
final class MediatorRuntime(
    val mediator: Mediator,
    domainOutbox: DomainOutboxHandle,
    config: MediatorConfig,
    storage: Storage,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit protected val ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {
  val pruningScheduler: MediatorPruningScheduler = new MediatorPruningScheduler(
    clock = clock,
    mediator = mediator,
    storage = storage,
    config = config.pruning,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  val timeService: ServerServiceDefinition = DomainTimeServiceGrpc.bindService(
    GrpcDomainTimeService.forDomainEntity(mediator.domain, mediator.timeTracker, loggerFactory),
    ec,
  )
  val administrationService: ServerServiceDefinition =
    MediatorAdministrationServiceGrpc.bindService(
      new GrpcMediatorAdministrationService(mediator, pruningScheduler, loggerFactory),
      ec,
    )

  ApiInfoServiceGrpc
    .bindService(new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi), ec)
    .discard

  def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT.right(mediator.startFUS())
      // start the domainOutbox only after the mediator has been started, otherwise
      // the future returned by startup will not be complete, because any topology transactions pushed to the
      // domain aren't actually processed until after the runtime is up and ... running
      _ <- domainOutbox.startup()
      _ <- EitherT.right(FutureUnlessShutdown.outcomeF(pruningScheduler.start()))
    } yield ()

  override protected def onClosed(): Unit =
    LifeCycle.close(pruningScheduler, domainOutbox, mediator)(logger)
}

object MediatorRuntimeFactory {
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
      config: MediatorConfig,
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

    val domainOutbox = domainOutboxFactory.create(
      protocolVersion,
      topologyClient,
      sequencerClient,
      clock,
      loggerFactory,
    )
    val mediator = new Mediator(
      domainId,
      mediatorId,
      sequencerClient,
      topologyClient,
      syncCrypto,
      topologyTransactionProcessor,
      topologyManagerStatus,
      domainOutbox,
      timeTracker,
      state,
      sequencerCounterTrackerStore,
      sequencedEventStore,
      nodeParameters,
      protocolVersion,
      clock,
      metrics,
      loggerFactory,
    )

    EitherT.pure[FutureUnlessShutdown, String](
      new MediatorRuntime(
        mediator,
        domainOutbox,
        config,
        storage,
        clock,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    )
  }
}
