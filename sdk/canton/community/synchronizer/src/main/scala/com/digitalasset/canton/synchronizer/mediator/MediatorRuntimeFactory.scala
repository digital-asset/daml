// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.{
  MediatorAdministrationServiceGrpc,
  MediatorInspectionServiceGrpc,
}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.synchronizer.mediator.service.{
  GrpcMediatorAdministrationService,
  GrpcMediatorInspectionService,
}
import com.digitalasset.canton.synchronizer.mediator.store.{
  FinalizedResponseStore,
  MediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.time.admin.v30.SynchronizerTimeServiceGrpc
import com.digitalasset.canton.time.{Clock, GrpcSynchronizerTimeService, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

/** Mediator component and its supporting services */
final class MediatorRuntime(
    val mediator: Mediator,
    synchronizerOutbox: SynchronizerOutboxHandle,
    config: MediatorConfig,
    storage: Storage,
    clock: Clock,
    batchingConfig: BatchingConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    protected val ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends FlagCloseable
    with NamedLogging {
  val pruningScheduler: MediatorPruningScheduler = new MediatorPruningScheduler(
    clock = clock,
    mediator = mediator,
    storage = storage,
    config = config.pruning,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  val timeService: ServerServiceDefinition = SynchronizerTimeServiceGrpc.bindService(
    GrpcSynchronizerTimeService
      .forSynchronizerEntity(mediator.psid, mediator.timeTracker, loggerFactory),
    ec,
  )
  val administrationService: ServerServiceDefinition =
    MediatorAdministrationServiceGrpc.bindService(
      new GrpcMediatorAdministrationService(mediator, pruningScheduler, loggerFactory),
      ec,
    )

  val inspectionService: ServerServiceDefinition = MediatorInspectionServiceGrpc.bindService(
    new GrpcMediatorInspectionService(
      mediator.state.finalizedResponseStore,
      mediator.state.recordOrderTimeAwaiter,
      batchingConfig.maxItemsInBatch,
      loggerFactory,
    ),
    ec,
  )

  ApiInfoServiceGrpc
    .bindService(new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi), ec)
    .discard

  def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT.right(mediator.start())
      // start the synchronizerOutbox only after the mediator has been started, otherwise
      // the future returned by startup will not be complete, because any topology transactions pushed to the
      // synchronizer aren't actually processed until after the runtime is up and ... running
      _ <- synchronizerOutbox.startup()
      _ <- EitherT.right(FutureUnlessShutdown.outcomeF(pruningScheduler.start()))
    } yield ()

  override protected def onClosed(): Unit =
    LifeCycle.close(pruningScheduler, synchronizerOutbox, mediator)(logger)
}

object MediatorRuntimeFactory {
  def create(
      mediatorId: MediatorId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: RichSequencerClient,
      syncCrypto: SynchronizerCryptoClient,
      topologyClient: SynchronizerTopologyClientWithInit,
      topologyTransactionProcessor: TopologyTransactionProcessor,
      topologyManagerStatus: TopologyManagerStatus,
      synchronizerOutboxFactory: SynchronizerOutboxFactory,
      timeTracker: SynchronizerTimeTracker,
      nodeParameters: CantonNodeParameters,
      clock: Clock,
      metrics: MediatorMetrics,
      config: MediatorConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime] = {
    val finalizedResponseStore = FinalizedResponseStore(
      storage,
      syncCrypto.pureCrypto,
      sequencerClient.protocolVersion,
      nodeParameters.cachingConfigs.finalizedMediatorConfirmationRequests,
      nodeParameters.processingTimeouts,
      loggerFactory,
    )
    val deduplicationStore =
      MediatorDeduplicationStore(
        storage,
        nodeParameters.processingTimeouts,
        loggerFactory,
        config.deduplicationStore.pruneAtMostEvery.toInternal,
        config.deduplicationStore.persistBatching,
      )
    val state =
      new MediatorState(
        finalizedResponseStore,
        deduplicationStore,
        clock,
        metrics,
        sequencerClient.protocolVersion,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )

    val synchronizerOutbox = synchronizerOutboxFactory.create(
      topologyClient,
      sequencerClient,
      timeTracker,
      clock,
      loggerFactory,
    )

    val mediator = new Mediator(
      mediatorId,
      sequencerClient,
      topologyClient,
      syncCrypto,
      topologyTransactionProcessor,
      topologyManagerStatus,
      synchronizerOutbox,
      timeTracker,
      state,
      sequencerCounterTrackerStore,
      sequencedEventStore,
      nodeParameters,
      clock,
      metrics,
      loggerFactory,
    )

    EitherT.pure[FutureUnlessShutdown, String](
      new MediatorRuntime(
        mediator,
        synchronizerOutbox,
        config,
        storage,
        clock,
        nodeParameters.batchingConfig,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    )
  }
}
