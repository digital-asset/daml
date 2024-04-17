// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
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
import com.digitalasset.canton.crypto.{HashPurpose, Nonce}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.domain.metrics.SequencerTestMetrics
import com.digitalasset.canton.domain.sequencing.SequencerParameters
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, Lifecycle, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.messages.UnsignedProtocolMessage
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParameters,
  DynamicDomainParametersLookup,
  TestDomainParameters,
  v30 as protocolV30,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  GrpcSequencerConnection,
  OrdinaryApplicationHandler,
  SequencerConnections,
  SerializedEventOrErrorHandler,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{DomainTimeTracker, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.PekkoUtil
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
  private val domainId = DefaultTestIdentities.domainId
  private val sequencerId = DefaultTestIdentities.sequencerId
  private val cryptoApi =
    TestingTopologyX()
      .withSimpleParticipants(participant)
      .build()
      .forOwnerAndDomain(participant, domainId)
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
  def timeouts = DefaultProcessingTimeouts.testing
  private val futureSupervisor = FutureSupervisor.Noop
  private val topologyClient = mock[DomainTopologyClient]
  private val mockTopologySnapshot = mock[TopologySnapshot]
  private val confirmationRequestsMaxRate =
    DynamicDomainParameters.defaultConfirmationRequestsMaxRate
  private val maxRequestSize = DynamicDomainParameters.defaultMaxRequestSize

  when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
    .thenReturn(mockTopologySnapshot)
  when(
    mockTopologySnapshot.findDynamicDomainParametersOrDefault(
      any[ProtocolVersion],
      anyBoolean,
    )(any[TraceContext])
  )
    .thenReturn(
      Future.successful(
        TestDomainParameters.defaultDynamic(
          confirmationRequestsMaxRate = confirmationRequestsMaxRate,
          maxRequestSize = maxRequestSize,
        )
      )
    )

  private val domainParamsLookup: DynamicDomainParametersLookup[SequencerDomainParameters] =
    DomainParametersLookup.forSequencerDomainParameters(
      BaseTest.defaultStaticDomainParameters,
      None,
      topologyClient,
      futureSupervisor,
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
    override def maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.1)
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
      domainParamsLookup,
      params,
      None,
      BaseTest.testedProtocolVersion,
      enableBroadcastOfUnauthenticatedMessages = false,
    )
  private val connectService = new GrpcSequencerConnectService(
    domainId = domainId,
    sequencerId = sequencerId,
    staticDomainParameters = BaseTest.defaultStaticDomainParameters,
    cryptoApi = cryptoApi,
    loggerFactory = loggerFactory,
  )

  private val authService = new SequencerAuthenticationService {
    override def challenge(
        request: v30.SequencerAuthentication.ChallengeRequest
    ): Future[v30.SequencerAuthentication.ChallengeResponse] =
      for {
        fingerprints <- cryptoApi.ips.currentSnapshotApproximation
          .signingKeys(participant)
          .map(_.map(_.fingerprint).toList)
      } yield v30.SequencerAuthentication.ChallengeResponse(
        v30.SequencerAuthentication.ChallengeResponse.Value
          .Success(
            v30.SequencerAuthentication.ChallengeResponse.Success(
              ReleaseVersion.current.toProtoPrimitive,
              Nonce.generate(cryptoApi.pureCrypto).toProtoPrimitive,
              fingerprints.map(_.unwrap),
            )
          )
      )
    override def authenticate(
        request: v30.SequencerAuthentication.AuthenticateRequest
    ): Future[v30.SequencerAuthentication.AuthenticateResponse] =
      Future.successful(
        v30.SequencerAuthentication.AuthenticateResponse(
          v30.SequencerAuthentication.AuthenticateResponse.Value
            .Success(
              v30.SequencerAuthentication.AuthenticateResponse.Success(
                AuthenticationToken.generate(cryptoApi.pureCrypto).toProtoPrimitive,
                Some(clock.now.plusSeconds(100000).toProtoTimestamp),
              )
            )
        )
      )
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

  private val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
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
        domainId,
        cryptoApi,
        cryptoApi.crypto,
        SequencerClientConfig(),
        TracingConfig.Propagation.Disabled,
        TestingConfigInternal(),
        BaseTest.defaultStaticDomainParameters,
        DefaultProcessingTimeouts.testing,
        clock,
        topologyClient,
        futureSupervisor,
        _ => None,
        _ => None,
        CommonMockMetrics.sequencerClient,
        LoggingConfig(),
        loggerFactory,
        ProtocolVersionCompatibility.supportedProtocolsParticipant(
          includeUnstableVersions = BaseTest.testedProtocolVersion.isUnstable,
          release = ReleaseVersion.current,
        ),
        Some(BaseTest.testedProtocolVersion),
      ).create(
        participant,
        sequencedEventStore,
        sendTrackerStore,
        new RequestSigner {
          override def signRequest[A <: HasCryptographicEvidence](
              request: A,
              hashPurpose: HashPurpose,
          )(implicit ec: ExecutionContext, traceContext: TraceContext)
              : EitherT[Future, String, SignedContent[A]] =
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
    .fold(fail(_), Predef.identity)

  override def close(): Unit =
    Lifecycle.close(
      service,
      Lifecycle.toCloseableServer(server, logger, "test"),
      client,
      executionSequencerFactory,
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts),
    )(logger)

  def mockSubscription(
      subscribeCallback: Unit => Unit = _ => (),
      unsubscribeCallback: Unit => Unit = _ => (),
  ): Unit = {
    // when a subscription is made resolve the subscribe promise
    // return to caller a subscription that will resolve the unsubscribe promise on close
    when(
      sequencerSubscriptionFactory
        .create(
          any[SequencerCounter],
          any[Member],
          any[SerializedEventOrErrorHandler[NotUsed]],
        )(any[TraceContext])
    )
      .thenAnswer {
        subscribeCallback(())
        EitherT.rightT[Future, CreateSubscriptionError] {
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

      val domainTimeTracker = mock[DomainTimeTracker]
      when(domainTimeTracker.wrapHandler(any[OrdinaryApplicationHandler[Envelope[_]]]))
        .thenAnswer(Predef.identity[OrdinaryApplicationHandler[Envelope[_]]] _)

      // kick of subscription
      val initF = env.client.subscribeAfter(
        CantonTimestamp.MinValue,
        None,
        ApplicationHandler.success(),
        domainTimeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

      val result = for {
        _ <- initF
        _ <- subscribePromise.future
        _ = env.client.close()
        _ <- unsubscribePromise.future
      } yield succeed // just getting here is good enough

      result.futureValue
    }

    "send from the client gets a message to the sequencer" in { env =>
      import cats.implicits.*

      val anotherParticipant = ParticipantId("another")

      when(env.sequencer.sendAsync(any[SubmissionRequest])(anyTraceContext))
        .thenReturn(EitherT.pure[Future, SendAsyncError](()))
      when(env.sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
        .thenReturn(EitherT.pure[Future, SendAsyncError](()))

      val result = for {
        response <- env.client
          .sendAsync(
            Batch
              .of(testedProtocolVersion, (MockProtocolMessage, Recipients.cc(anotherParticipant))),
            SendType.Other,
            None,
          )
          .value
          .onShutdown(fail())
      } yield {
        response shouldBe Right(())
      }

      result.futureValue
    }
  }

  private case object MockProtocolMessage extends UnsignedProtocolMessage {
    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      ???

    override protected lazy val companionObj = MockProtocolMessage

    override def domainId: DomainId = DefaultTestIdentities.domainId

    override def toProtoSomeEnvelopeContentV30: protocolV30.EnvelopeContent.SomeEnvelopeContent =
      protocolV30.EnvelopeContent.SomeEnvelopeContent.Empty
  }
}
