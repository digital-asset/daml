// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{Crypto, SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLoggingContext}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.{DomainParametersLookup, StaticDomainParameters}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.ReplayAction.{SequencerEvents, SequencerSends}
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.sequencing.client.transports.replay.{
  ReplayingEventsSequencerClientTransport,
  ReplayingSendsSequencerClientTransportImpl,
  ReplayingSendsSequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.protocol.{GetTrafficStateForMemberRequest, TrafficState}
import com.digitalasset.canton.sequencing.traffic.{EventCostCalculator, TrafficStateController}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerAlias, SequencerCounter}
import io.grpc.{CallOptions, ManagedChannel}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.*

trait SequencerClientFactory {
  def create(
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
  ): EitherT[Future, String, RichSequencerClient]

}

object SequencerClientFactory {
  def apply(
      domainId: DomainId,
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      crypto: Crypto,
      config: SequencerClientConfig,
      traceContextPropagation: TracingConfig.Propagation,
      testingConfig: TestingConfigInternal,
      domainParameters: StaticDomainParameters,
      processingTimeout: ProcessingTimeout,
      clock: Clock,
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      recordingConfigForMember: Member => Option[RecordingConfig],
      replayConfigForMember: Member => Option[ReplayConfig],
      metrics: SequencerClientMetrics,
      loggingConfig: LoggingConfig,
      exitOnTimeout: Boolean,
      loggerFactory: NamedLoggerFactory,
      supportedProtocolVersions: Seq[ProtocolVersion],
      minimumProtocolVersion: Option[ProtocolVersion],
  ): SequencerClientFactory & SequencerClientTransportFactory =
    new SequencerClientFactory with SequencerClientTransportFactory {

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
      ): EitherT[Future, String, RichSequencerClient] = {
        // initialize recorder if it's been configured for the member (should only be used for testing)
        val recorderO = recordingConfigForMember(member).map { recordingConfig =>
          new SequencerClientRecorder(
            recordingConfig.fullFilePath,
            processingTimeout,
            loggerFactory,
          )
        }
        val sequencerDomainParamsLookup = DomainParametersLookup.forSequencerDomainParameters(
          domainParameters,
          config.overrideMaxRequestSize,
          topologyClient,
          futureSupervisor,
          loggerFactory,
        )

        val sequencerTransportsMap = makeTransport(
          sequencerConnections,
          member,
          requestSigner,
        )

        for {
          sequencerTransports <- EitherT.fromEither[Future](
            SequencerTransports.from(
              sequencerTransportsMap,
              expectedSequencers,
              sequencerConnections.sequencerTrustThreshold,
              sequencerConnections.submissionRequestAmplification,
            )
          )

          // Find the timestamp of the last known sequenced event, we'll use that timestamp to initialize
          // the traffic state
          latestSequencedTimestampO <- EitherT.right(
            sequencedEventStore
              .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
              .toOption
              .value
              .map(_.map(_.timestamp))
          )
          getTrafficStateFromDomainFn = { (ts: CantonTimestamp) =>
            BftSender
              .makeRequest[SequencerAlias, String, SequencerClientTransport, Option[
                TrafficState
              ], Option[TrafficState]](
                s"Retrieving traffic state from domain for $member at $ts",
                futureSupervisor,
                loggerFactory.getTracedLogger(this.getClass),
                sequencerTransportsMap.forgetNE,
                sequencerConnections.sequencerTrustThreshold,
                _.getTrafficStateForMember(
                  // Request the traffic state at the timestamp immediately following the last sequenced event timestamp
                  // That's because we will not re-process that event, but if it was a traffic purchase, the sequencer
                  // would return a state with the previous extra traffic value, because traffic purchases only become
                  // valid _after_ they've been sequenced. This ensures the participant doesn't miss a traffic purchase
                  // if it gets disconnected just after reading one.
                  GetTrafficStateForMemberRequest(
                    member,
                    ts.immediateSuccessor,
                    domainParameters.protocolVersion,
                  )
                ).map(_.trafficState),
                identity,
              )
              .leftMap { err =>
                s"Failed to retrieve traffic state from domain for $member: $err"
              }
          }
          // Make a BFT call to all the transports to retrieve the current traffic state from the domain
          // and initialize the trafficStateController with it
          trafficStateO <- latestSequencedTimestampO
            .traverse(getTrafficStateFromDomainFn(_).onShutdown(Left("Aborted due to shutdown")))
            .map(_.flatten)

          // fetch the initial set of pending sends to initialize the client with.
          // as it owns the client that should be writing to this store it should not be racy.
          initialPendingSends <- EitherT.right(sendTrackerStore.fetchPendingSends)
          trafficStateController = new TrafficStateController(
            member,
            loggerFactory,
            syncCryptoApi,
            trafficStateO.getOrElse(TrafficState.empty(CantonTimestamp.Epoch)),
            domainParameters.protocolVersion,
            new EventCostCalculator(loggerFactory),
            futureSupervisor,
            processingTimeout,
            metrics.trafficConsumption,
            domainId,
          )
          sendTracker = new SendTracker(
            initialPendingSends,
            sendTrackerStore,
            metrics,
            loggerFactory,
            processingTimeout,
            Some(trafficStateController),
            member,
          )
          // pluggable send approach to support transitioning to the new async sends
          validatorFactory = new SequencedEventValidatorFactory {
            override def create(loggerFactory: NamedLoggerFactory)(implicit
                traceContext: TraceContext
            ): SequencedEventValidator =
              if (config.skipSequencedEventValidation) {
                SequencedEventValidator.noValidation(domainId)(
                  NamedLoggingContext(loggerFactory, traceContext)
                )
              } else {
                new SequencedEventValidatorImpl(
                  domainId,
                  domainParameters.protocolVersion,
                  syncCryptoApi,
                  loggerFactory,
                  processingTimeout,
                )
              }
          }
        } yield new RichSequencerClientImpl(
          domainId,
          member,
          sequencerTransports,
          config,
          testingConfig,
          domainParameters.protocolVersion,
          sequencerDomainParamsLookup,
          processingTimeout,
          validatorFactory,
          clock,
          requestSigner,
          sequencedEventStore,
          sendTracker,
          metrics,
          recorderO,
          replayConfigForMember(member).isDefined,
          syncCryptoApi,
          loggingConfig,
          Some(trafficStateController),
          exitOnTimeout,
          loggerFactory,
          futureSupervisor,
          SequencerCounter.Genesis,
        )
      }

      override def makeTransport(
          connection: SequencerConnection,
          member: Member,
          requestSigner: RequestSigner,
          allowReplay: Boolean = true,
      )(implicit
          executionContext: ExecutionContextExecutor,
          executionSequencerFactory: ExecutionSequencerFactory,
          materializer: Materializer,
          traceContext: TraceContext,
      ): SequencerClientTransport & SequencerClientTransportPekko = {
        val loggerFactoryWithSequencerAlias =
          SequencerClient.loggerFactoryWithSequencerAlias(
            loggerFactory,
            connection.sequencerAlias,
          )

        // TODO(#13789) Use only `SequencerClientTransportPekko` as the return type
        def mkRealTransport(): SequencerClientTransport & SequencerClientTransportPekko =
          connection match {
            case grpc: GrpcSequencerConnection => grpcTransport(grpc, member)
          }

        replayConfigForMember(member).filter(_ => allowReplay) match {
          case None => mkRealTransport()
          case Some(ReplayConfig(recording, SequencerEvents)) =>
            new ReplayingEventsSequencerClientTransport(
              domainParameters.protocolVersion,
              recording.fullFilePath,
              processingTimeout,
              loggerFactoryWithSequencerAlias,
            )
          case Some(ReplayConfig(recording, replaySendsConfig: SequencerSends)) =>
            if (replaySendsConfig.usePekko) {
              val underlyingTransport = mkRealTransport()
              new ReplayingSendsSequencerClientTransportPekko(
                domainParameters.protocolVersion,
                recording.fullFilePath,
                replaySendsConfig,
                member,
                underlyingTransport,
                requestSigner,
                metrics,
                processingTimeout,
                loggerFactoryWithSequencerAlias,
              )
            } else {
              val underlyingTransport = mkRealTransport()
              new ReplayingSendsSequencerClientTransportImpl(
                domainParameters.protocolVersion,
                recording.fullFilePath,
                replaySendsConfig,
                member,
                underlyingTransport,
                requestSigner,
                metrics,
                processingTimeout,
                loggerFactoryWithSequencerAlias,
              )
            }
        }
      }

      private def createChannel(conn: GrpcSequencerConnection)(implicit
          executionContext: ExecutionContextExecutor
      ): ManagedChannel = {
        val channelBuilder = ClientChannelBuilder(
          SequencerClient.loggerFactoryWithSequencerAlias(loggerFactory, conn.sequencerAlias)
        )
        GrpcSequencerChannelBuilder(
          channelBuilder,
          conn,
          NonNegativeInt.maxValue, // we set this limit only on the sequencer node, to avoid restarting the client if this value is changed
          traceContextPropagation,
          config.keepAliveClient,
        )
      }

      /** the wait-for-ready call option is added for when round-robin-ing through connections
        * so that if one of them gets closed, we try the next one instead of unnecessarily failing.
        * wait-for-ready semantics: https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
        * this is safe for non-idempotent RPCs.
        */
      private def callOptionsForEndpoints(endpoints: NonEmpty[Seq[Endpoint]]): CallOptions =
        if (endpoints.length > 1) CallOptions.DEFAULT.withWaitForReady()
        else CallOptions.DEFAULT

      private def grpcSequencerClientAuth(
          connection: GrpcSequencerConnection,
          member: Member,
      )(implicit
          executionContext: ExecutionContextExecutor,
          traceContext: TraceContext,
      ): GrpcSequencerClientAuth = {
        val channelPerEndpoint = connection.endpoints.map { endpoint =>
          val subConnection = connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
          endpoint -> createChannel(subConnection)
        }.toMap
        new GrpcSequencerClientAuth(
          domainId,
          member,
          crypto,
          channelPerEndpoint,
          supportedProtocolVersions,
          config.authToken,
          clock,
          processingTimeout,
          SequencerClient.loggerFactoryWithSequencerAlias(
            loggerFactory,
            connection.sequencerAlias,
          ),
        )
      }

      private def grpcTransport(connection: GrpcSequencerConnection, member: Member)(implicit
          executionContext: ExecutionContextExecutor,
          executionSequencerFactory: ExecutionSequencerFactory,
          materializer: Materializer,
          traceContext: TraceContext,
      ): SequencerClientTransport & SequencerClientTransportPekko = {
        val channel = createChannel(connection)
        val auth = grpcSequencerClientAuth(connection, member)
        val callOptions = callOptionsForEndpoints(connection.endpoints)
        new GrpcSequencerClientTransportPekko(
          channel,
          callOptions,
          auth,
          metrics,
          processingTimeout,
          SequencerClient
            .loggerFactoryWithSequencerAlias(loggerFactory, connection.sequencerAlias),
          domainParameters.protocolVersion,
        )
      }

    }
}
