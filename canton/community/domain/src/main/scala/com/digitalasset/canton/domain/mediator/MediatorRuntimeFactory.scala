// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.admin.v0.EnterpriseMediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.api.v0.DomainTimeServiceGrpc
import com.digitalasset.canton.domain.mediator.store.{
  FinalizedResponseStore,
  MediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.{Clock, GrpcDomainTimeService}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorCommon
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** Mediator component and its supporting services */
trait MediatorRuntime extends FlagCloseable {
  def mediator: Mediator

  final def registerAdminGrpcServices(register: ServerServiceDefinition => Unit): Unit = {
    register(timeService)
    register(enterpriseAdministrationService)
  }

  def timeService: ServerServiceDefinition
  def enterpriseAdministrationService: ServerServiceDefinition
  def start()(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = for {
    _ <- EitherT.right(mediator.start())
  } yield ()

  override protected def onClosed(): Unit =
    mediator.close()
}

private[mediator] class CommunityMediatorRuntime(
    override val mediator: Mediator,
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
      EnterpriseMediatorAdministrationServiceGrpc.SERVICE,
      logger,
    )
}

trait MediatorRuntimeFactory {
  def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: SequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyTransactionProcessor: TopologyTransactionProcessorCommon,
      topologyManagerStatusO: Option[TopologyManagerStatus],
      timeTrackerConfig: DomainTimeTrackerConfig,
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
  ): EitherT[Future, String, MediatorRuntime]
}

object CommunityMediatorRuntimeFactory extends MediatorRuntimeFactory {
  override def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: SequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyTransactionProcessor: TopologyTransactionProcessorCommon,
      topologyManagerStatusO: Option[TopologyManagerStatus],
      timeTrackerConfig: DomainTimeTrackerConfig,
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
  ): EitherT[Future, String, MediatorRuntime] = {
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
        nodeParameters.cachingConfigs.finalizedMediatorRequests,
        nodeParameters.processingTimeouts,
        loggerFactory,
      )

    EitherT.pure[Future, String](
      new CommunityMediatorRuntime(
        new Mediator(
          domainId,
          mediatorId,
          sequencerClient,
          topologyClient,
          syncCrypto,
          topologyTransactionProcessor,
          topologyManagerStatusO,
          timeTrackerConfig,
          state,
          sequencerCounterTrackerStore,
          sequencedEventStore,
          nodeParameters,
          protocolVersion,
          clock,
          metrics,
          loggerFactory,
        ),
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    )
  }
}
