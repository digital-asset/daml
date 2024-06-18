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
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.v0.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.domain.sequencing.SequencerParameters
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, Lifecycle, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.messages.{
  ProtocolMessage,
  ProtocolMessageV0,
  ProtocolMessageV1,
  ProtocolMessageV2,
  ProtocolMessageV3,
}
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  TestDomainParameters,
  v0 as protocolV0,
  v1 as protocolV1,
  v2 as protocolV2,
  v3 as protocolV3,
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
    TestingTopology()
      .withSimpleParticipants(participant)
      .build()
      .forOwnerAndDomain(participant, domainId)
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
  def timeouts = DefaultProcessingTimeouts.testing
  private val futureSupervisor = FutureSupervisor.Noop
  private val topologyClient = mock[DomainTopologyClient]
  private val mockTopologySnapshot = mock[TopologySnapshot]
  private val maxRatePerParticipant = BaseTest.defaultMaxRatePerParticipant
  private val maxRequestSize = BaseTest.defaultMaxRequestSize

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
          maxRatePerParticipant = maxRatePerParticipant,
          maxRequestSize = maxRequestSize,
        )
      )
    )

  private val domainParamsLookup: DomainParametersLookup[SequencerDomainParameters] =
    DomainParametersLookup.forSequencerDomainParameters(
      BaseTest.defaultStaticDomainParametersWith(maxRatePerParticipant =
        maxRatePerParticipant.unwrap
      ),
      None,
      topologyClient,
      futureSupervisor,
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
  private val params = new SequencerParameters {
    override def maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.1)
    override def processingTimeouts: ProcessingTimeout = timeouts
  }
  private val service =
    new GrpcSequencerService(
      sequencer,
      DomainTestMetrics.sequencer,
      loggerFactory,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription[_]](
        clock,
        DomainTestMetrics.sequencer,
        timeouts,
        loggerFactory,
      ),
      sequencerSubscriptionFactory,
      domainParamsLookup,
      params,
      BaseTest.testedProtocolVersion,
    )
  private val connectService = new GrpcSequencerConnectService(
    domainId = domainId,
    sequencerId = sequencerId,
    staticDomainParameters = BaseTest.defaultStaticDomainParameters,
    cryptoApi = cryptoApi,
    agreementManager = None,
    loggerFactory = loggerFactory,
  )

  private val authService = new SequencerAuthenticationService {
    override def challenge(request: v0.Challenge.Request): Future[v0.Challenge.Response] =
      for {
        fingerprints <- cryptoApi.ips.currentSnapshotApproximation
          .signingKeys(participant)
          .map(_.map(_.fingerprint).toList)
      } yield v0.Challenge.Response(
        v0.Challenge.Response.Value
          .Success(
            v0.Challenge.Success(
              ReleaseVersion.current.toProtoPrimitive,
              Nonce.generate(cryptoApi.pureCrypto).toProtoPrimitive,
              fingerprints.map(_.unwrap),
            )
          )
      )
    override def authenticate(
        request: v0.Authentication.Request
    ): Future[v0.Authentication.Response] =
      Future.successful(
        v0.Authentication.Response(
          v0.Authentication.Response.Value
            .Success(
              v0.Authentication.Success(
                AuthenticationToken.generate(cryptoApi.pureCrypto).toProtoPrimitive,
                Some(clock.now.plusSeconds(100000).toProtoPrimitive),
              )
            )
        )
      )
  }
  private val serverPort = UniquePortGenerator.next
  logger.debug(s"Using port ${serverPort} for integration test")
  private val server = NettyServerBuilder
    .forPort(serverPort.unwrap)
    .addService(v0.SequencerConnectServiceGrpc.bindService(connectService, ec))
    .addService(v0.SequencerServiceGrpc.bindService(service, ec))
    .addService(v0.SequencerAuthenticationServiceGrpc.bindService(authService, ec))
    .build()
    .start()

  private val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  private val sendTrackerStore = new InMemorySendTrackerStore()
  private val connection =
    GrpcSequencerConnection(
      NonEmpty(Seq, Endpoint("localhost", serverPort)),
      false,
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
        agreedAgreementId = None,
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
          includePreviewVersions = BaseTest.testedProtocolVersion.isPreview,
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
          any[String],
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
      } yield {
        response shouldBe Right(())
      }

      result.futureValue
    }
  }

  private case object MockProtocolMessage
      extends ProtocolMessage
      with ProtocolMessageV0
      with ProtocolMessageV1
      with ProtocolMessageV2
      with ProtocolMessageV3 {
    // no significance to this payload, just need anything valid and this was the easiest to construct
    private val payload =
      protocolV0.SignedProtocolMessage(
        None,
        protocolV0.SignedProtocolMessage.SomeSignedProtocolMessage.Empty,
      )

    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      ???

    override protected lazy val companionObj = MockProtocolMessage

    override def domainId: DomainId = DefaultTestIdentities.domainId
    override def toProtoEnvelopeContentV0: protocolV0.EnvelopeContent =
      protocolV0.EnvelopeContent(
        protocolV0.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload)
      )

    override def toProtoEnvelopeContentV1: protocolV1.EnvelopeContent =
      protocolV1.EnvelopeContent(
        protocolV1.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload)
      )

    override def toProtoEnvelopeContentV2: protocolV2.EnvelopeContent =
      protocolV2.EnvelopeContent(
        protocolV2.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload)
      )

    override def toProtoEnvelopeContentV3: protocolV3.EnvelopeContent =
      protocolV3.EnvelopeContent(
        protocolV3.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload)
      )
  }
}
