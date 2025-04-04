// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  LoggingConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{HashPurpose, Nonce, SigningKeyUsage, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
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
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerParameters
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.time.{SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.{EitherTUtil, PekkoUtil}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
  RepresentativeProtocolVersion,
}
import io.grpc.netty.NettyServerBuilder
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.mockito.ArgumentMatchersSugar
import org.scalatest.Outcome
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.*
import scala.concurrent.duration.*

final case class Env(loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContextExecutor,
    tracer: Tracer,
    traceContext: TraceContext,
) extends AutoCloseable
    with NamedLogging
    with org.mockito.MockitoSugar
    with ArgumentMatchersSugar
    with Matchers {
  implicit val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem("GrpcSequencerIntegrationTest")
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory("GrpcSequencerIntegrationTest", noTracingLogger)
  val sequencer = mock[Sequencer]
  private val participant = ParticipantId("testing")
  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val sequencerId = DefaultTestIdentities.daSequencerId
  private val cryptoApi =
    TestingTopology()
      .withSimpleParticipants(participant)
      .build()
      .forOwnerAndSynchronizer(participant, synchronizerId)
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
  def timeouts = DefaultProcessingTimeouts.testing
  private val futureSupervisor = FutureSupervisor.Noop
  private val topologyClient = mock[SynchronizerTopologyClient]
  private val mockSynchronizerTopologyManager = mock[SynchronizerTopologyManager]
  private val mockTopologySnapshot = mock[TopologySnapshot]
  private val confirmationRequestsMaxRate =
    DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate
  private val maxRequestSize = DynamicSynchronizerParameters.defaultMaxRequestSize
  private val topologyStateForInitializationService = mock[TopologyStateForInitializationService]

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

  private val synchronizerParamsLookup
      : DynamicSynchronizerParametersLookup[SequencerSynchronizerParameters] =
    SynchronizerParametersLookup.forSequencerSynchronizerParameters(
      BaseTest.defaultStaticSynchronizerParameters,
      None,
      topologyClient,
      loggerFactory,
    )

  private val authenticationCheck = new AuthenticationCheck {

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
  private val params = new SequencerParameters {
    override def maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.1)
    override def processingTimeouts: ProcessingTimeout = timeouts
  }
  private val service =
    new GrpcSequencerService(
      sequencer,
      SequencerTestMetrics,
      loggerFactory,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription[_]](
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
    )
  private val connectService = new GrpcSequencerConnectService(
    synchronizerId = synchronizerId,
    sequencerId = sequencerId,
    staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParameters,
    cryptoApi = cryptoApi,
    synchronizerTopologyManager = mockSynchronizerTopologyManager,
    loggerFactory = loggerFactory,
  )

  private val authService = new SequencerAuthenticationService {
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
  private val serverPort = UniquePortGenerator.next
  logger.debug(s"Using port $serverPort for integration test")
  private val server = NettyServerBuilder
    .forPort(serverPort.unwrap)
    .addService(v30.SequencerConnectServiceGrpc.bindService(connectService, ec))
    .addService(v30.SequencerServiceGrpc.bindService(service, ec))
    .addService(v30.SequencerAuthenticationServiceGrpc.bindService(authService, ec))
    .build()
    .start()

  private val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  private val sendTrackerStore = new InMemorySendTrackerStore()
  private val connection =
    GrpcSequencerConnection(
      NonEmpty(Seq, Endpoint("localhost", serverPort)),
      transportSecurity = false,
      None,
      SequencerAlias.Default,
    )
  private val connections = SequencerConnections.single(connection)
  private val expectedSequencers = NonEmpty.mk(Set, SequencerAlias.Default -> sequencerId).toMap

  val client = Await
    .result(
      SequencerClientFactory(
        synchronizerId,
        cryptoApi,
        cryptoApi.crypto,
        SequencerClientConfig(),
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
        exitOnTimeout = false,
        loggerFactory,
        ProtocolVersionCompatibility.supportedProtocols(
          includeAlphaVersions = BaseTest.testedProtocolVersion.isAlpha,
          includeBetaVersions = BaseTest.testedProtocolVersion.isBeta,
          release = ReleaseVersion.current,
        ),
      ).create(
        participant,
        sequencedEventStore,
        sendTrackerStore,
        new RequestSigner {
          override def signRequest[A <: HasCryptographicEvidence](
              request: A,
              hashPurpose: HashPurpose,
              snapshot: Option[SyncCryptoApi],
          )(implicit ec: ExecutionContext, traceContext: TraceContext)
              : EitherT[FutureUnlessShutdown, String, SignedContent[A]] =
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
        expectedSequencers,
      ).value,
      10.seconds,
    )
    .onShutdown(fail("Shutting down"))
    .fold(fail(_), Predef.identity)

  override def close(): Unit =
    LifeCycle.close(
      client,
      service,
      LifeCycle.toCloseableServer(server, logger, "test"),
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
        .createV2(
          any[Option[CantonTimestamp]],
          any[Member],
          any[SerializedEventOrErrorHandler[NotUsed]],
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
    val env = Env(loggerFactory)
    try super.withFixture(test.toNoArgTest(env))
    finally env.close()
  }

  "GRPC Sequencer" should {
    "cancel the sequencer subscription when the client connection is cancelled" in { env =>
      val subscribePromise = Promise[Unit]()
      val unsubscribePromise = Promise[Unit]()

      // when a subscription is made resolve the subscribe promise
      // return to caller a subscription that will resolve the unsubscribe promise on close
      env.mockSubscription(_ => subscribePromise.success(()), _ => unsubscribePromise.success(()))

      val synchronizerTimeTracker = mock[SynchronizerTimeTracker]
      when(synchronizerTimeTracker.wrapHandler(any[OrdinaryApplicationHandler[Envelope[_]]]))
        .thenAnswer(Predef.identity[OrdinaryApplicationHandler[Envelope[_]]] _)

      // kick of subscription
      val initF = env.client.subscribeAfter(
        CantonTimestamp.MinValue,
        None,
        ApplicationHandler.success(),
        synchronizerTimeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

      val result = for {
        _ <- initF.failOnShutdown
        _ <- subscribePromise.future
        _ = env.client.close()
        _ <- unsubscribePromise.future
      } yield succeed // just getting here is good enough

      result.futureValue
    }

    "send from the client gets a message to the sequencer" in { env =>
      val anotherParticipant = ParticipantId("another")

      when(env.sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
        .thenReturn(EitherTUtil.unitUS[SequencerDeliverError])
      implicit val metricsContext: MetricsContext = MetricsContext.Empty
      val result = for {
        response <- env.client
          .sendAsync(
            Batch
              .of(testedProtocolVersion, (MockProtocolMessage, Recipients.cc(anotherParticipant))),
            None,
          )
          .value
          .onShutdown(fail())
      } yield {
        response shouldBe Either.unit
      }

      result.futureValue
    }
  }

  private case object MockProtocolMessage extends UnsignedProtocolMessage {
    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      ???

    override protected lazy val companionObj = MockProtocolMessage

    override def synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId

    override def toProtoSomeEnvelopeContentV30: protocolV30.EnvelopeContent.SomeEnvelopeContent =
      protocolV30.EnvelopeContent.SomeEnvelopeContent.Empty
  }
}
