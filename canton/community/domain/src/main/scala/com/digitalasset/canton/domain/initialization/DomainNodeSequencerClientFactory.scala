// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.Domain
import com.digitalasset.canton.domain.metrics.DomainMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SequencerClient,
  SequencerClientFactory,
  SequencerClientTransportFactory,
}
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.store.{SendTrackerStore, SequencedEventStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContextExecutor, Future}

// customize the default sequencer-client-factory to allow passing in separate metrics and a customized logger factory
// to be able to distinguish between the mediator and topology manager when running in the same node
class DomainNodeSequencerClientFactory(
    id: DomainId,
    metrics: DomainMetrics,
    topologyClient: DomainTopologyClientWithInit,
    cantonNodeParameters: CantonNodeParameters,
    crypto: Crypto,
    domainParameters: StaticDomainParameters,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
) extends SequencerClientFactory
    with SequencerClientTransportFactory
    with NamedLogging {

  override def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      requestSigner: RequestSigner,
      sequencerConnections: SequencerConnections,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClient] =
    factory(member).create(
      member,
      sequencedEventStore,
      sendTrackerStore,
      requestSigner,
      sequencerConnections,
      expectedSequencers,
    )

  override def makeTransport(
      connection: SequencerConnection,
      member: Member,
      requestSigner: RequestSigner,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClientTransport] =
    factory(member).makeTransport(connection, member, requestSigner)

  private def factory(member: Member)(implicit
      executionContext: ExecutionContextExecutor
  ): SequencerClientFactory with SequencerClientTransportFactory = {
    val (clientMetrics, clientName) = member match {
      case MediatorId(_) => (metrics.mediator.sequencerClient, "mediator")
      case DomainTopologyManagerId(_) =>
        (metrics.topologyManager.sequencerClient, "topology-manager")
      case other => sys.error(s"Unexpected sequencer client in Domain node: $other")
    }

    val clientLoggerFactory = loggerFactory.append("client", clientName)

    val sequencerClientSyncCrypto =
      new DomainSyncCryptoClient(
        SequencerId(id),
        id,
        topologyClient,
        crypto,
        cantonNodeParameters.cachingConfigs,
        cantonNodeParameters.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

    SequencerClientFactory(
      id,
      sequencerClientSyncCrypto,
      crypto,
      None,
      cantonNodeParameters.sequencerClient,
      cantonNodeParameters.tracing.propagation,
      testingConfig,
      domainParameters,
      cantonNodeParameters.processingTimeouts,
      clock,
      topologyClient,
      futureSupervisor,
      member =>
        Domain.recordSequencerInteractions
          .get()
          .lift(member)
          .map(Domain.setMemberRecordingPath(member)),
      member =>
        Domain.replaySequencerConfig.get().lift(member).map(Domain.defaultReplayPath(member)),
      clientMetrics,
      cantonNodeParameters.loggingConfig,
      clientLoggerFactory,
      supportedProtocolVersions = ProtocolVersionCompatibility
        .trySupportedProtocolsDomain(cantonNodeParameters),
      minimumProtocolVersion = None,
    )
  }

  override def validateTransport(
      connection: SequencerConnection,
      logWarning: Boolean,
  )(implicit
      executionContext: ExecutionContextExecutor,
      errorLoggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    SequencerClientTransportFactory.validateTransport(
      connection,
      cantonNodeParameters.tracing.propagation,
      cantonNodeParameters.sequencerClient,
      logWarning,
      loggerFactory,
    )

}
