// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient, SynchronizerCrypto}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, SynchronizerParametersLookup}
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
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.{CallOptions, ManagedChannel}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.*

trait SequencerClientFactory {
  def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      requestSigner: RequestSigner,
      sequencerConnections: SequencerConnections,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      expectedSequencersO: Option[NonEmpty[Map[SequencerAlias, SequencerId]]],
      connectionPool: SequencerConnectionXPool,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, RichSequencerClient]

}

object SequencerClientFactory {
  def apply(
      psid: PhysicalSynchronizerId,
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      crypto: SynchronizerCrypto,
      config: SequencerClientConfig,
      traceContextPropagation: TracingConfig.Propagation,
      testingConfig: TestingConfigInternal,
      synchronizerParameters: StaticSynchronizerParameters,
      processingTimeout: ProcessingTimeout,
      clock: Clock,
      topologyClient: SynchronizerTopologyClient,
      futureSupervisor: FutureSupervisor,
      recordingConfigForMember: Member => Option[RecordingConfig],
      replayConfigForMember: Member => Option[ReplayConfig],
      metrics: SequencerClientMetrics,
      loggingConfig: LoggingConfig,
      exitOnFatalErrors: Boolean,
      namedLoggerFactory: NamedLoggerFactory,
      supportedProtocolVersions: Seq[ProtocolVersion],
  ): SequencerClientFactory & SequencerClientTransportFactory =
    new SequencerClientFactory with SequencerClientTransportFactory with NamedLogging {
      override protected def loggerFactory: NamedLoggerFactory = namedLoggerFactory

      override def create(
          member: Member,
          sequencedEventStore: SequencedEventStore,
          sendTrackerStore: SendTrackerStore,
          requestSigner: RequestSigner,
          sequencerConnections: SequencerConnections,
          synchronizerPredecessor: Option[SynchronizerPredecessor],
          expectedSequencersO: Option[NonEmpty[Map[SequencerAlias, SequencerId]]],
          connectionPool: SequencerConnectionXPool,
      )(implicit
          executionContext: ExecutionContextExecutor,
          executionSequencerFactory: ExecutionSequencerFactory,
          materializer: Materializer,
          tracer: Tracer,
          traceContext: TraceContext,
          closeContext: CloseContext,
      ): EitherT[FutureUnlessShutdown, String, RichSequencerClient] = {
        // initialize recorder if it's been configured for the member (should only be used for testing)
        val recorderO = recordingConfigForMember(member).map { recordingConfig =>
          new SequencerClientRecorder(
            recordingConfig.fullFilePath,
            processingTimeout,
            loggerFactory,
          )
        }
        val sequencerSynchronizerParamsLookup =
          SynchronizerParametersLookup.forSequencerSynchronizerParameters(
            config.overrideMaxRequestSize,
            topologyClient,
            loggerFactory,
          )

        val sequencerTransportsMapO = Option.when(!config.useNewConnectionPool)(
          makeTransport(
            sequencerConnections,
            member,
            requestSigner,
          )
        )

        def getTrafficStateWithTransports(
            ts: CantonTimestamp
        ): EitherT[FutureUnlessShutdown, String, Option[TrafficState]] =
          BftSender
            .makeRequest(
              s"Retrieving traffic state from synchronizer for $member at $ts",
              futureSupervisor,
              logger,
              sequencerTransportsMapO.getOrElse(
                throw new IllegalStateException(
                  "sequencerTransportsMap undefined while using transports"
                )
              ),
              sequencerConnections.sequencerTrustThreshold,
            )(
              _.getTrafficStateForMember(
                // Request the traffic state at the timestamp immediately following the last sequenced event timestamp
                // That's because we will not re-process that event, but if it was a traffic purchase, the sequencer
                // would return a state with the previous extra traffic value, because traffic purchases only become
                // valid _after_ they've been sequenced. This ensures the participant doesn't miss a traffic purchase
                // if it gets disconnected just after reading one.
                GetTrafficStateForMemberRequest(
                  member,
                  ts.immediateSuccessor,
                  synchronizerParameters.protocolVersion,
                )
              ).map(_.trafficState)
            )(identity)
            .leftMap { err =>
              s"Failed to retrieve traffic state from synchronizer for $member: $err"
            }

        def getTrafficStateWithConnectionPool(
            ts: CantonTimestamp
        ): EitherT[FutureUnlessShutdown, String, Option[TrafficState]] =
          for {
            connections <- EitherT.fromEither[FutureUnlessShutdown](
              NonEmpty
                .from(connectionPool.getOneConnectionPerSequencer("get-traffic-state"))
                .toRight(
                  s"No connection available to retrieve traffic state from synchronizer for $member"
                )
            )
            result <- BftSender
              .makeRequest(
                s"Retrieving traffic state from synchronizer for $member at $ts",
                futureSupervisor,
                logger,
                operators = connections,
                threshold = sequencerConnections.sequencerTrustThreshold,
              )(
                performRequest = _.getTrafficStateForMember(
                  // Request the traffic state at the timestamp immediately following the last sequenced event timestamp
                  // That's because we will not re-process that event, but if it was a traffic purchase, the sequencer
                  // would return a state with the previous extra traffic value, because traffic purchases only become
                  // valid _after_ they've been sequenced. This ensures the participant doesn't miss a traffic purchase
                  // if it gets disconnected just after reading one.
                  GetTrafficStateForMemberRequest(
                    member,
                    ts.immediateSuccessor,
                    synchronizerParameters.protocolVersion,
                  ),
                  timeout = processingTimeout.network.duration,
                ).map(_.trafficState)
              )(identity)
              .leftMap { err =>
                s"Failed to retrieve traffic state from synchronizer for $member: $err"
              }

          } yield result

        for {
          sequencerTransports <- EitherT.fromEither[FutureUnlessShutdown](
            SequencerTransports.from(
              sequencerTransportsMapO,
              expectedSequencersO,
              sequencerConnections.sequencerTrustThreshold,
              sequencerConnections.sequencerLivenessMargin,
              sequencerConnections.submissionRequestAmplification,
              sequencerConnections.sequencerConnectionPoolDelays,
            )
          )
          // Reinitialize the sequencer counter allocator to ensure that passive->active replica transitions
          // correctly track the counters produced by other replicas
          _ <- EitherT.right(
            sequencedEventStore.reinitializeFromDbOrSetLowerBound()
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

          getTrafficStateFromSynchronizerFn =
            if (config.useNewConnectionPool) getTrafficStateWithConnectionPool _
            else getTrafficStateWithTransports _

          getTrafficStateFromSynchronizerWithRetryFn = { (ts: CantonTimestamp) =>
            EitherT(
              retry
                .Backoff(
                  logger,
                  closeContext.context,
                  retry.Forever,
                  config.startupConnectionRetryDelay.asFiniteApproximation,
                  config.maxConnectionRetryDelay.asFiniteApproximation,
                  "Traffic State Initialization",
                  s"Initialize traffic state from a BFT read with threshold ${sequencerConnections.sequencerTrustThreshold} from ${sequencerConnections.connections.length} total connections",
                  retryLogLevel = Some(Level.INFO),
                )
                .unlessShutdown(
                  getTrafficStateFromSynchronizerFn(ts).value,
                  AllExceptionRetryPolicy,
                )
            )
          }

          // Make a BFT call to all the transports to retrieve the current traffic state from the synchronizer
          // and initialize the trafficStateController with it
          trafficStateO <- latestSequencedTimestampO
            .traverse(getTrafficStateFromSynchronizerWithRetryFn)
            .map(_.flatten)

          // fetch the initial set of pending sends to initialize the client with.
          // as it owns the client that should be writing to this store it should not be racy.
          initialPendingSends = sendTrackerStore.fetchPendingSends
          trafficStateController = new TrafficStateController(
            member,
            loggerFactory,
            syncCryptoApi,
            trafficStateO.getOrElse(TrafficState.empty(CantonTimestamp.Epoch)),
            synchronizerParameters.protocolVersion,
            new EventCostCalculator(loggerFactory),
            metrics.trafficConsumption,
            psid,
          )
          sendTracker = new SendTracker(
            initialPendingSends,
            sendTrackerStore,
            metrics,
            loggerFactory,
            processingTimeout,
            Some(trafficStateController),
          )
          // pluggable send approach to support transitioning to the new async sends
          validatorFactory = new SequencedEventValidatorFactory {
            override def create(loggerFactory: NamedLoggerFactory)(implicit
                traceContext: TraceContext
            ): SequencedEventValidator =
              if (config.skipSequencedEventValidation) {
                SequencedEventValidator.noValidation(psid)(
                  NamedLoggingContext(loggerFactory, traceContext)
                )
              } else {
                new SequencedEventValidatorImpl(
                  psid,
                  syncCryptoApi,
                  loggerFactory,
                  processingTimeout,
                )
              }
          }
        } yield new RichSequencerClientImpl(
          psid,
          synchronizerPredecessor,
          member,
          sequencerTransports,
          connectionPool,
          config,
          testingConfig,
          sequencerSynchronizerParamsLookup,
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
          exitOnFatalErrors,
          loggerFactory,
          futureSupervisor,
        )
      }

      override def makeTransport(
          connection: SequencerConnection,
          member: Member,
          requestSigner: RequestSigner,
          allowReplay: Boolean,
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
              synchronizerParameters.protocolVersion,
              recording.fullFilePath,
              processingTimeout,
              loggerFactoryWithSequencerAlias,
            )
          case Some(ReplayConfig(recording, replaySendsConfig: SequencerSends)) =>
            if (replaySendsConfig.usePekko) {
              val underlyingTransport = mkRealTransport()
              new ReplayingSendsSequencerClientTransportPekko(
                synchronizerParameters.protocolVersion,
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
                synchronizerParameters.protocolVersion,
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

      /** the wait-for-ready call option is added for when round-robin-ing through connections so
        * that if one of them gets closed, we try the next one instead of unnecessarily failing.
        * wait-for-ready semantics: https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
        * this is safe for non-idempotent RPCs.
        */
      private def callOptionsForEndpoints(endpoints: NonEmpty[Seq[Endpoint]]): CallOptions =
        if (endpoints.sizeIs > 1) CallOptions.DEFAULT.withWaitForReady()
        else CallOptions.DEFAULT

      private def grpcSequencerClientAuth(
          connection: GrpcSequencerConnection,
          member: Member,
      )(implicit executionContext: ExecutionContextExecutor): GrpcSequencerClientAuth = {
        val channelPerEndpoint = connection.endpoints.map { endpoint =>
          val subConnection = connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
          endpoint -> createChannel(subConnection)
        }.toMap
        new GrpcSequencerClientAuth(
          psid,
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
      ): SequencerClientTransport & SequencerClientTransportPekko = {
        val channel = createChannel(connection)
        val auth = grpcSequencerClientAuth(connection, member)
        val callOptions = callOptionsForEndpoints(connection.endpoints)
        new GrpcSequencerClientTransportPekko(
          channel,
          callOptions,
          auth,
          processingTimeout,
          SequencerClient
            .loggerFactoryWithSequencerAlias(loggerFactory, connection.sequencerAlias),
          synchronizerParameters.protocolVersion,
        )
      }
    }
}
