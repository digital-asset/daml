// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  Port,
  PositiveDouble,
  PositiveInt,
}
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  LoggingConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{HashPurpose, Nonce, SigningKeyUsage, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{
  LogEntry,
  NamedLoggerFactory,
  NamedLogging,
  SuppressingLogger,
  SuppressionRule,
  TracedLogger,
}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.SynchronizerParametersLookup.SequencerSynchronizerParameters
import com.digitalasset.canton.protocol.messages.UnsignedProtocolMessage
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersLookup,
  SynchronizerParametersLookup,
  TestSynchronizerParameters,
  v30 as protocolV30,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolConfig
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerParameters
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.time.{SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.DefaultTestIdentities.namespace
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.{EitherTUtil, PekkoUtil}
import com.digitalasset.canton.version.{
  IgnoreInSerializationTestExhaustivenessCheck,
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{config, *}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, StatusRuntimeException}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.mockito.ArgumentMatchersSugar
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.*

class Env(override val loggerFactory: SuppressingLogger)(implicit
    ec: ExecutionContextExecutor,
    tracer: Tracer,
    traceContext: TraceContext,
) extends AutoCloseable
    with NamedLogging
    with org.mockito.MockitoSugar
    with ArgumentMatchersSugar { self =>
  implicit val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem("GrpcSequencerIntegrationTest")
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory("GrpcSequencerIntegrationTest", noTracingLogger)
  val sequencer = mock[Sequencer]
  private val participant = ParticipantId("testing")
  val anotherParticipant = ParticipantId("another")
  val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  val sequencerId = DefaultTestIdentities.daSequencerId
  private val cryptoApi =
    TestingTopology()
      .withSimpleParticipants(participant, anotherParticipant)
      .build()
      .forOwnerAndSynchronizer(participant, synchronizerId)
  val clock = new SimClock(loggerFactory = loggerFactory)
  val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
  def timeouts = DefaultProcessingTimeouts.testing
  private val futureSupervisor = FutureSupervisor.Noop
  private val topologyClient = mock[SynchronizerTopologyClient]
  private val mockSynchronizerTopologyManager = mock[SynchronizerTopologyManager]
  private val mockTopologySnapshot = mock[TopologySnapshot]
  private val confirmationRequestsMaxRate =
    DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate
  private val maxRequestSize = DynamicSynchronizerParameters.defaultMaxRequestSize
  val topologyStateForInitializationService = mock[TopologyStateForInitializationService]
  implicit val closeContext: CloseContext = CloseContext(new FlagCloseable {
    override protected def timeouts: ProcessingTimeout = self.timeouts
    override protected[this] def logger: TracedLogger = self.logger
  })

  // TODO(i27260): cleanup when the new connection pool is stable
  val useNewConnectionPool: Boolean = true

  when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
    .thenReturn(mockTopologySnapshot)
  when(topologyClient.headSnapshot(any[TraceContext]))
    .thenReturn(mockTopologySnapshot)
  when(mockTopologySnapshot.timestamp).thenReturn(CantonTimestamp.Epoch)
  when(
    mockTopologySnapshot.trafficControlParameters(
      any[ProtocolVersion],
      anyBoolean,
    )(any[TraceContext])
  )
    .thenReturn(FutureUnlessShutdown.pure(None))
  when(
    mockTopologySnapshot.findDynamicSynchronizerParametersOrDefault(
      any[ProtocolVersion],
      anyBoolean,
    )(any[TraceContext])
  )
    .thenReturn(
      FutureUnlessShutdown.pure(
        TestSynchronizerParameters.defaultDynamic(
          confirmationRequestsMaxRate = confirmationRequestsMaxRate,
          maxRequestSize = maxRequestSize,
        )
      )
    )

  val synchronizerParamsLookup
      : DynamicSynchronizerParametersLookup[SequencerSynchronizerParameters] =
    SynchronizerParametersLookup.forSequencerSynchronizerParameters(
      None,
      topologyClient,
      loggerFactory,
    )

  val authenticationCheck = new AuthenticationCheck {

    override def authenticate(
        member: Member,
        authenticatedMember: Option[Member],
    ): Either[String, Unit] =
      Either.cond(
        member == participant,
        (),
        s"$participant attempted operation on behalf of $member",
      )

    override def lookupCurrentMember(): Option[Member] = None
  }
  val params = new SequencerParameters {
    override def maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.1)
    override def processingTimeouts: ProcessingTimeout = timeouts
  }
  private val service =
    new GrpcSequencerService(
      sequencer,
      SequencerTestMetrics,
      loggerFactory,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription[?]](
        clock,
        SequencerTestMetrics,
        timeouts,
        loggerFactory,
      ),
      sequencerSubscriptionFactory,
      synchronizerParamsLookup,
      params,
      topologyStateForInitializationService,
      BaseTest.testedProtocolVersion,
    ) {
      override def getTrafficStateForMember(
          request: v30.GetTrafficStateForMemberRequest
      ): Future[v30.GetTrafficStateForMemberResponse] =
        Future.successful(
          v30.GetTrafficStateForMemberResponse(None)
        )
    }
  def makeConnectService(sequencerId: SequencerId) = new GrpcSequencerConnectService(
    synchronizerId = synchronizerId,
    sequencerId = sequencerId,
    staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParameters,
    cryptoApi = cryptoApi,
    synchronizerTopologyManager = mockSynchronizerTopologyManager,
    loggerFactory = loggerFactory,
  )
  private val connectService = makeConnectService(sequencerId)

  lazy val authService = new SequencerAuthenticationService {
    override def challenge(
        request: v30.SequencerAuthentication.ChallengeRequest
    ): Future[v30.SequencerAuthentication.ChallengeResponse] =
      for {
        fingerprints <- cryptoApi.ips.currentSnapshotApproximation
          .signingKeys(participant, SigningKeyUsage.SequencerAuthenticationOnly)
          .map(_.map(_.fingerprint).toList)
          .onShutdown(throw new Exception("Aborted due to shutdown."))
      } yield v30.SequencerAuthentication.ChallengeResponse(
        Nonce.generate(cryptoApi.pureCrypto).toProtoPrimitive,
        fingerprints.map(_.unwrap),
      )
    override def authenticate(
        request: v30.SequencerAuthentication.AuthenticateRequest
    ): Future[v30.SequencerAuthentication.AuthenticateResponse] =
      Future.successful(
        v30.SequencerAuthentication.AuthenticateResponse(
          AuthenticationToken.generate(cryptoApi.pureCrypto).toProtoPrimitive,
          Some(clock.now.plusSeconds(100000).toProtoTimestamp),
        )
      )
    override def logout(
        request: v30.SequencerAuthentication.LogoutRequest
    ): Future[v30.SequencerAuthentication.LogoutResponse] =
      Future.successful(v30.SequencerAuthentication.LogoutResponse())
  }

  private val apiInfoService = new GrpcApiInfoService(CantonGrpcUtil.ApiName.SequencerPublicApi)

  private val serverPort = UniquePortGenerator.next
  logger.debug(s"Using port $serverPort for integration test")

  def spinUpSequencer(
      service: GrpcSequencerService,
      connectService: GrpcSequencerConnectService,
      port: Port,
  ): Server = {
    val server = NettyServerBuilder
      .forPort(port.unwrap)
      .addService(v30.SequencerConnectServiceGrpc.bindService(connectService, ec))
      .addService(v30.SequencerServiceGrpc.bindService(service, ec))
      .addService(v30.SequencerAuthenticationServiceGrpc.bindService(authService, ec))
      .addService(ApiInfoServiceGrpc.bindService(apiInfoService, ec))
      .build()
      .start()
    servers.updateAndGet(server +: _)
    server
  }

  private val servers = new AtomicReference[Seq[Server]](Seq.empty)
  // Start default server
  spinUpSequencer(service, connectService, serverPort)

  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  private val sendTrackerStore = new InMemorySendTrackerStore()
  def makeConnection(port: Port, alias: SequencerAlias = SequencerAlias.Default) =
    GrpcSequencerConnection(
      NonEmpty(Seq, Endpoint("localhost", port)),
      transportSecurity = false,
      None,
      alias,
      None,
    )
  val connection = makeConnection(serverPort)
  private val connections = SequencerConnections.single(connection)
  private val expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]] =
    NonEmpty.mk(Set, SequencerAlias.Default -> sequencerId).toMap

  val connectionPoolFactory = new GrpcSequencerConnectionXPoolFactory(
    clientProtocolVersions = NonEmpty(Seq, BaseTest.testedProtocolVersion),
    minimumProtocolVersion = None,
    authConfig = authConfig,
    member = participant,
    clock = clock,
    crypto = cryptoApi.crypto.crypto,
    seedForRandomnessO = None,
    futureSupervisor = futureSupervisor,
    metrics = CommonMockMetrics.sequencerClient.connectionPool,
    metricsContext = MetricsContext.Empty,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  lazy val authConfig = AuthenticationTokenManagerConfig()

  private val clients = new AtomicReference[Seq[SequencerClient]](Seq.empty)

  def makeClient(
      connections: SequencerConnections,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
      useNewConnectionPool: Boolean = useNewConnectionPool,
  ): EitherT[FutureUnlessShutdown, String, RichSequencerClient] = {
    val clientConfig =
      SequencerClientConfig(authToken = authConfig, useNewConnectionPool = useNewConnectionPool)
    val clientFactory = SequencerClientFactory(
      synchronizerId,
      cryptoApi,
      cryptoApi.crypto,
      clientConfig,
      TracingConfig.Propagation.Disabled,
      TestingConfigInternal(),
      BaseTest.defaultStaticSynchronizerParameters,
      DefaultProcessingTimeouts.testing,
      clock,
      topologyClient,
      futureSupervisor,
      _ => None,
      _ => None,
      CommonMockMetrics.sequencerClient,
      LoggingConfig(),
      exitOnFatalErrors = false,
      loggerFactory,
      ProtocolVersionCompatibility.supportedProtocols(
        includeAlphaVersions = BaseTest.testedProtocolVersion.isAlpha,
        includeBetaVersions = BaseTest.testedProtocolVersion.isBeta,
        release = ReleaseVersion.current,
      ),
    )

    val poolConfig = SequencerConnectionXPoolConfig.fromSequencerConnections(
      sequencerConnections = connections,
      tracingConfig = TracingConfig(TracingConfig.Propagation.Disabled),
      expectedPSIdO = None,
    )

    for {
      connectionPool <- EitherT.fromEither[FutureUnlessShutdown](
        connectionPoolFactory.create(poolConfig, name = "test").leftMap(error => error.toString)
      )
      _ <-
        if (useNewConnectionPool)
          connectionPool.start().leftMap(error => error.toString)
        else EitherTUtil.unitUS
      client <- clientFactory
        .create(
          participant,
          sequencedEventStore,
          sendTrackerStore,
          new RequestSigner {
            override def signRequest[A <: HasCryptographicEvidence](
                request: A,
                hashPurpose: HashPurpose,
                snapshot: Option[SyncCryptoApi],
            )(implicit
                ec: ExecutionContext,
                traceContext: TraceContext,
            ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] =
              EitherT.rightT(
                SignedContent(
                  request,
                  SymbolicCrypto.emptySignature,
                  None,
                  BaseTest.testedProtocolVersion,
                )
              )
          },
          connections,
          synchronizerPredecessor = None,
          Option.when(!useNewConnectionPool)(expectedSequencers),
          connectionPool = connectionPool,
        )
    } yield {
      clients.updateAndGet(client +: _)
      client
    }
  }

  def makeDefaultClient = makeClient(connections, expectedSequencers)

  override def close(): Unit =
    LifeCycle.close(
      SyncCloseable("sequencer-clients", clients.get().foreach(_.close())),
      service,
      SyncCloseable(
        "sequencer-clients",
        servers.get().foreach(s => LifeCycle.toCloseableServer(s, logger, "test").close()),
      ),
      executionSequencerFactory,
      LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts),
    )(logger)

  def mockSubscription(
      subscribeCallback: Unit => Unit = _ => (),
      unsubscribeCallback: Unit => Unit = _ => (),
  ): Unit =
    // when a subscription is made resolve the subscribe promise
    // return to caller a subscription that will resolve the unsubscribe promise on close
    when(
      sequencerSubscriptionFactory
        .create(
          any[Option[CantonTimestamp]],
          any[Member],
          any[SequencedEventOrErrorHandler[NotUsed]],
        )(any[TraceContext])
    )
      .thenAnswer {
        subscribeCallback(())
        EitherT.rightT[FutureUnlessShutdown, CreateSubscriptionError] {
          new SequencerSubscription[NotUsed] {
            override protected def loggerFactory: NamedLoggerFactory = Env.this.loggerFactory
            override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
              SyncCloseable(
                "anonymous-sequencer-subscription",
                unsubscribeCallback(()),
              )
            )
            override protected def timeouts: ProcessingTimeout = Env.this.timeouts
            override private[canton] def complete(reason: SubscriptionCloseReason[NotUsed])(implicit
                traceContext: TraceContext
            ): Unit = closeReasonPromise.trySuccess(reason).discard[Boolean]
          }
        }
      }
}

class GrpcSequencerIntegrationTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {
  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env(loggerFactory)
    try super.withFixture(test.toNoArgTest(env))
    finally env.close()
  }

  "GRPC Sequencer" should {
    "cancel the sequencer subscription when the client connection is cancelled" in { env =>
      val subscribePromise = Promise[Unit]()
      val unsubscribePromise = Promise[Unit]()
      val client = env.makeDefaultClient.futureValueUS.value

      // when a subscription is made resolve the subscribe promise
      // return to caller a subscription that will resolve the unsubscribe promise on close
      env.mockSubscription(_ => subscribePromise.success(()), _ => unsubscribePromise.success(()))

      val synchronizerTimeTracker = mock[SynchronizerTimeTracker]
      when(synchronizerTimeTracker.wrapHandler(any[OrdinaryApplicationHandler[Envelope[?]]]))
        .thenAnswer(Predef.identity[OrdinaryApplicationHandler[Envelope[?]]] _)

      // kick of subscription
      val initF = client.subscribeAfter(
        CantonTimestamp.MinValue,
        None,
        ApplicationHandler.success(),
        synchronizerTimeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

      val result = for {
        _ <- initF.failOnShutdown
        _ <- subscribePromise.future
        _ = client.close()
        _ <- unsubscribePromise.future
      } yield succeed // just getting here is good enough

      result.futureValue
    }

    "send from the client gets a message to the sequencer" in { env =>
      when(env.sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
        .thenReturn(EitherTUtil.unitUS[CantonBaseError])
      implicit val metricsContext: MetricsContext = MetricsContext.Empty
      val client = env.makeDefaultClient.futureValueUS.value
      val result = for {
        response <- client
          .send(
            Batch
              .of(
                testedProtocolVersion,
                (MockProtocolMessage, Recipients.cc(env.anotherParticipant)),
              ),
            None,
          )
          .value
          .onShutdown(fail())
      } yield {
        response shouldBe Either.unit
      }

      result.futureValue
    }

    "retry sequencer client creation if traffic state BFT read is unsuccessful" in { env =>
      import env.*
      val sequencerId2 = SequencerId(UniqueIdentifier.tryCreate("sequencer2", namespace))
      val sequencer2ConnectService = env.makeConnectService(sequencerId2)
      val port2 = UniquePortGenerator.next
      val sequencerAlias2 = SequencerAlias.tryCreate("Sequencer2")
      val trafficStateRpcCalled = new AtomicInteger(0)
      val service2 =
        new GrpcSequencerService(
          sequencer,
          SequencerTestMetrics,
          env.loggerFactory,
          authenticationCheck,
          new SubscriptionPool[GrpcManagedSubscription[?]](
            clock,
            SequencerTestMetrics,
            env.timeouts,
            env.loggerFactory,
          ),
          sequencerSubscriptionFactory,
          synchronizerParamsLookup,
          params,
          topologyStateForInitializationService,
          BaseTest.testedProtocolVersion,
        ) {
          override def getTrafficStateForMember(
              request: v30.GetTrafficStateForMemberRequest
          ): Future[v30.GetTrafficStateForMemberResponse] =
            if (trafficStateRpcCalled.getAndIncrement() == 0) {
              // Return an empty traffic state instead of a None at the start
              // We should observe retries of the client factory until both sequencers report the same traffic state
              Future.successful(
                v30.GetTrafficStateForMemberResponse(Some(TrafficState.empty.toProtoV30))
              )
            } else {
              Future.successful(
                v30.GetTrafficStateForMemberResponse(None)
              )
            }
        }

      env.spinUpSequencer(service2, sequencer2ConnectService, port2)

      // We need an event in the event store otherwise the factory will skip the traffic state call
      val now = clock.now
      val dummyEvent = SequencedEventWithTraceContext(
        SignedContent(
          SequencerTestUtils.mockDeliver(now, synchronizerId = synchronizerId),
          SymbolicCrypto.emptySignature,
          None,
          testedProtocolVersion,
        )
      )(
        TraceContext.empty
      )
      sequencedEventStore.store(Seq(dummyEvent))(traceContext, closeContext).futureValueUS

      env.loggerFactory.assertLogs(
        SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[SequencerClientFactory]
      )(
        makeClient(
          SequencerConnections
            .many(
              NonEmpty.mk(Seq, connection, makeConnection(port2, sequencerAlias2)),
              sequencerTrustThreshold = PositiveInt.two,
              sequencerLivenessMargin = NonNegativeInt.zero,
              submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
              sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
            )
            .value,
          expectedSequencers = NonEmpty
            .mk(Set, SequencerAlias.Default -> sequencerId, sequencerAlias2 -> sequencerId2)
            .toMap,
        ).futureValueUS,
        assertions = _.infoMessage should include(
          "Cannot reach threshold for Retrieving traffic state from synchronizer"
        ),
        _.infoMessage should include(
          "The operation 'Traffic State Initialization' was not successful"
        ),
        _.infoMessage should include("Now retrying operation 'Traffic State Initialization'"),
      )

    }
  }

  private case object MockProtocolMessage
      extends UnsignedProtocolMessage
      with IgnoreInSerializationTestExhaustivenessCheck {
    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      ???

    override protected lazy val companionObj = MockProtocolMessage

    override def psid: PhysicalSynchronizerId =
      DefaultTestIdentities.physicalSynchronizerId

    override def toProtoSomeEnvelopeContentV30: protocolV30.EnvelopeContent.SomeEnvelopeContent =
      protocolV30.EnvelopeContent.SomeEnvelopeContent.Empty
  }
}

final class EnvWithFailingTokenRefresh(override val loggerFactory: SuppressingLogger)(implicit
    ec: ExecutionContextExecutor,
    tracer: Tracer,
    traceContext: TraceContext,
) extends Env(loggerFactory) {
  override lazy val authConfig =
    AuthenticationTokenManagerConfig(minRetryInterval =
      config.NonNegativeFiniteDuration.ofMillis(10)
    )

  override lazy val authService = new SequencerAuthenticationService {
    override def challenge(
        request: v30.SequencerAuthentication.ChallengeRequest
    ): Future[v30.SequencerAuthentication.ChallengeResponse] =
      Future.failed(new StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription("test")))

    override def authenticate(
        request: v30.SequencerAuthentication.AuthenticateRequest
    ): Future[v30.SequencerAuthentication.AuthenticateResponse] =
      Future.failed(new StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription("test")))

    override def logout(
        request: v30.SequencerAuthentication.LogoutRequest
    ): Future[v30.SequencerAuthentication.LogoutResponse] =
      Future.failed(new StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription("test")))
  }
}

class GrpcSequencerIntegrationWithFailingTokenRefreshTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {
  override type FixtureParam = EnvWithFailingTokenRefresh

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new EnvWithFailingTokenRefresh(loggerFactory)
    try super.withFixture(test.toNoArgTest(env))
    finally env.close()
  }

  "the sequencer client's downloadTopologyStateForInit" when {
    "the token refresh fails" should {
      "handle it gracefully" in { env =>
        val client = env.makeDefaultClient.futureValueUS.value

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          inside(
            client
              .downloadTopologyStateForInit(maxRetries = 5, retryLogLevel = Some(Level.WARN))
              .value
              .futureValueUS
          ) { case Left(message) =>
            if (env.useNewConnectionPool) {
              // The error message is formatted differently with the connection pool
              message should include(
                "GrpcServiceUnavailable: UNAVAILABLE/Authentication token refresh error: test"
              )
            } else {
              message shouldBe
                "Status{code=UNAVAILABLE, description=Authentication token refresh error: test, cause=null}"
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage shouldBe
                  "Token refresh encountered error: Status{code=UNAVAILABLE, description=test, cause=null}",
                "Failing token refresh",
              ),
              (
                _.warningMessage should (include(
                  "Request failed"
                ) and include("Request: download-topology-state-for-init-hash")),
                "Request failure",
              ),
              (
                _.warningMessage should include(
                  "The operation 'Get hash for init topology state' was not successful."
                ),
                "Attempt failure",
              ),
              (
                _.warningMessage should include(
                  "Now retrying operation 'Get hash for init topology state'."
                ),
                "Retry message",
              ),
            )
          ),
        )
      }
    }
  }
}
