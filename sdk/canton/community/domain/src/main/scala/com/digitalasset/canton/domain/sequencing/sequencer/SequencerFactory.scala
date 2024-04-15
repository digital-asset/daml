// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.block.SequencerDriver
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerWriterConfig.DefaultMaxSqlInListSize
import com.digitalasset.canton.domain.sequencing.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerStore
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

trait SequencerFactory extends FlagCloseable with HasCloseContext {

  def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit ex: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Unit]

  def create(
      domainId: DomainId,
      sequencerId: SequencerId,
      clock: Clock,
      driverClock: Clock, // this clock is only used in tests, otherwise can the same clock as above can be passed
      domainSyncCryptoApi: DomainSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): Future[Sequencer]
}

abstract class DatabaseSequencerFactory(
    storage: Storage,
    override val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
) extends SequencerFactory
    with NamedLogging {

  override def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit ex: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Unit] = {

    // TODO(#18401): Parameterize DatabaseSequencer with the SequencerStore;
    //  create it in this factory, and pass the same one to DBS and use here;
    //  this will allow using in-memory stores for testing sequencer onboarding.
    //  Close context then should be changed to the sequencer's close context.
    val generalStore: SequencerStore =
      SequencerStore(
        storage,
        protocolVersion,
        DefaultMaxSqlInListSize,
        timeouts,
        loggerFactory,
        // At the moment this store instance is only used for the sequencer initialization,
        // if it is retrying a db operation and the factory is closed, the store will be closed as well;
        // if request succeeds, the store will no be retrying and doesn't need to be closed
        overrideCloseContext = Some(this.closeContext),
      )

    generalStore.initializeFromSnapshot(initialState.snapshot)
  }
}

class CommunityDatabaseSequencerFactory(
    config: DatabaseSequencerConfig,
    metrics: SequencerMetrics,
    storage: Storage,
    sequencerProtocolVersion: ProtocolVersion,
    topologyClientMember: Member,
    nodeParameters: CantonNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
) extends DatabaseSequencerFactory(
      storage,
      nodeParameters.processingTimeouts,
      sequencerProtocolVersion,
    ) {

  override def create(
      domainId: DomainId,
      sequencerId: SequencerId,
      clock: Clock,
      driverClock: Clock,
      domainSyncCryptoApi: DomainSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): Future[Sequencer] = {
    val sequencer = DatabaseSequencer.single(
      config,
      None,
      nodeParameters.processingTimeouts,
      storage,
      clock,
      domainId,
      topologyClientMember,
      sequencerProtocolVersion,
      domainSyncCryptoApi,
      metrics,
      loggerFactory,
      nodeParameters.useUnifiedSequencer,
    )

    Future.successful(config.testingInterceptor.map(_(clock)(sequencer)(ec)).getOrElse(sequencer))
  }

}

/** Artificial interface for dependency injection
  */
trait MkSequencerFactory {

  def apply(
      protocolVersion: ProtocolVersion,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      scheduler: ScheduledExecutorService,
      metrics: SequencerMetrics,
      storage: Storage,
      sequencerId: SequencerId,
      nodeParameters: CantonNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(
      sequencerConfig: SequencerConfig
  )(implicit executionContext: ExecutionContext): SequencerFactory

}

object CommunitySequencerFactory extends MkSequencerFactory {
  override def apply(
      protocolVersion: ProtocolVersion,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      scheduler: ScheduledExecutorService,
      metrics: SequencerMetrics,
      storage: Storage,
      sequencerId: SequencerId,
      nodeParameters: CantonNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(sequencerConfig: SequencerConfig)(implicit
      executionContext: ExecutionContext
  ): SequencerFactory = sequencerConfig match {
    case communityDbConfig: CommunitySequencerConfig.Database =>
      new CommunityDatabaseSequencerFactory(
        communityDbConfig,
        metrics,
        storage,
        protocolVersion,
        sequencerId,
        nodeParameters,
        loggerFactory,
      )

    case CommunitySequencerConfig.External(sequencerType, config, testingInterceptor) =>
      DriverBlockSequencerFactory(
        sequencerType,
        SequencerDriver.DriverApiVersion,
        config,
        health,
        storage,
        protocolVersion,
        sequencerId,
        nodeParameters,
        metrics,
        loggerFactory,
        testingInterceptor,
      )

    case config: SequencerConfig =>
      throw new UnsupportedOperationException(s"Invalid config type ${config.getClass}")
  }
}
