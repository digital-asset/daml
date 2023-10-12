// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

trait SequencerFactory extends AutoCloseable {

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
      rateLimitManager: Option[SequencerRateLimitManager],
      implicitMemberRegistration: Boolean,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): Future[Sequencer]
}

abstract class DatabaseSequencerFactory extends SequencerFactory {

  override def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit ex: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Unit] =
    EitherT.leftT(
      "Database sequencer does not support dynamically bootstrapping from a snapshot. " +
        "Database sequencers from the same domain should share the same database with no need for extra initialization steps once one of the sequencer has been initialized."
    )

}

class CommunityDatabaseSequencerFactory(
    config: DatabaseSequencerConfig,
    metrics: SequencerMetrics,
    storage: Storage,
    sequencerProtocolVersion: ProtocolVersion,
    topologyClientMember: Member,
    nodeParameters: CantonNodeParameters,
    val loggerFactory: NamedLoggerFactory,
) extends DatabaseSequencerFactory {

  override def create(
      domainId: DomainId,
      sequencerId: SequencerId,
      clock: Clock,
      driverClock: Clock,
      domainSyncCryptoApi: DomainSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      rateLimitManager: Option[SequencerRateLimitManager],
      implicitMemberRegistration: Boolean,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): Future[Sequencer] = {
    val sequencer = DatabaseSequencer.single(
      config,
      nodeParameters.processingTimeouts,
      storage,
      clock,
      domainId,
      topologyClientMember,
      sequencerProtocolVersion,
      domainSyncCryptoApi,
      metrics,
      loggerFactory,
    )

    Future.successful(config.testingInterceptor.map(_(clock)(sequencer)(ec)).getOrElse(sequencer))
  }

  override def close(): Unit = ()
}
