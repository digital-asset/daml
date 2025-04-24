// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Signature, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule.Level
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.SynchronizerParametersLookup.SequencerSynchronizerParameters
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersLookup,
  SynchronizerParametersLookup,
  TestSynchronizerParameters,
  v30 as protocolV30,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.BytestringWithCryptographicEvidence
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerParameters
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.service.{
  RecordStreamObserverItems,
  StreamComplete,
  StreamError,
  StreamNext,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStateForInitializationService,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.{ProtocolVersion, VersionedMessage}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAsyncWordSpec,
}
import com.google.protobuf.ByteString
import io.grpc.Status.Code.*
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{StatusException, StatusRuntimeException}
import monocle.macros.syntax.lens.*
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

import SubscriptionPool.PoolClosed

class GrpcSequencerServiceTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with HasExecutionContext {
  type Subscription = GrpcManagedSubscription[?]

  import GrpcSequencerServiceTest.*

  private lazy val participant = DefaultTestIdentities.participant1

  class Environment(member: Member) extends Matchers {
    val sequencer: Sequencer = mock[Sequencer]
    when(sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
      .thenReturn(EitherTUtil.unitUS)
    when(sequencer.acknowledgeSigned(any[SignedContent[AcknowledgeRequest]])(anyTraceContext))
      .thenReturn(EitherTUtil.unitUS)
    val cryptoApi: SynchronizerCryptoClient =
      TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(member)
    val subscriptionPool: SubscriptionPool[Subscription] =
      mock[SubscriptionPool[GrpcManagedSubscription[?]]]

    private val confirmationRequestsMaxRate = NonNegativeInt.tryCreate(5)
    private val maxRequestSize = NonNegativeInt.tryCreate(1000)
    val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
    private val topologyClient = mock[SynchronizerTopologyClient]
    private val mockTopologySnapshot = mock[TopologySnapshot]
    when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
      .thenReturn(mockTopologySnapshot)
    when(
      mockTopologySnapshot.findDynamicSynchronizerParametersOrDefault(
        any[ProtocolVersion],
        anyBoolean,
      )(
        any[TraceContext]
      )
    )
      .thenReturn(
        FutureUnlessShutdown.pure(
          TestSynchronizerParameters.defaultDynamic(
            confirmationRequestsMaxRate = confirmationRequestsMaxRate,
            maxRequestSize = MaxRequestSize(maxRequestSize),
          )
        )
      )

    private val synchronizerParamLookup
        : DynamicSynchronizerParametersLookup[SequencerSynchronizerParameters] =
      SynchronizerParametersLookup.forSequencerSynchronizerParameters(
        BaseTest.defaultStaticSynchronizerParameters,
        None,
        topologyClient,
        loggerFactory,
      )
    private val params = new SequencerParameters {
      override def maxConfirmationRequestsBurstFactor: PositiveDouble =
        PositiveDouble.tryCreate(1e-6)
      override def processingTimeouts: ProcessingTimeout = timeouts
    }

    val maxItemsInTopologyBatch = 5
    private val numBatches = 3
    private val topologyInitService: TopologyStateForInitializationService =
      new TopologyStateForInitializationService {
        val factory =
          new TopologyTransactionTestFactory(loggerFactory, initEc = parallelExecutionContext)

        override def initialSnapshot(member: Member)(implicit
            executionContext: ExecutionContext,
            traceContext: TraceContext,
        ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = FutureUnlessShutdown.pure(
          StoredTopologyTransactions(
            // As we don't expect the actual transactions in this test, we can repeat the same transaction a bunch of times
            List
              .fill(maxItemsInTopologyBatch * numBatches)(factory.ns1k1_k1)
              .map(
                StoredTopologyTransaction(
                  SequencedTime.MinValue,
                  EffectiveTime.MinValue,
                  None,
                  _,
                  None,
                )
              )
          )
        )
      }

    val acknowledgementsConflateWindow = PositiveFiniteDuration.ofSeconds(3)
    val service =
      new GrpcSequencerService(
        sequencer,
        SequencerTestMetrics,
        loggerFactory,
        new AuthenticationCheck.MatchesAuthenticatedMember {
          override def lookupCurrentMember(): Option[Member] = member.some
        },
        subscriptionPool,
        sequencerSubscriptionFactory,
        synchronizerParamLookup,
        params,
        topologyInitService,
        BaseTest.testedProtocolVersion,
        maxItemsInTopologyResponse = PositiveInt.tryCreate(maxItemsInTopologyBatch),
        acknowledgementsConflateWindow = Some(acknowledgementsConflateWindow),
      )
  }

  override type FixtureParam = Environment

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Environment(participant)
    withFixture(test.toNoArgAsyncTest(env))
  }

  private def mkSubmissionRequest(
      batch: Batch[ClosedEnvelope],
      sender: Member,
  ): SubmissionRequest = {
    val id = MessageId.tryCreate("messageId")
    SubmissionRequest.tryCreate(
      sender,
      id,
      batch,
      CantonTimestamp.MaxValue,
      None,
      None,
      Option.empty[SequencingSubmissionCost],
      testedProtocolVersion,
    )
  }

  private def signedContent(bytes: ByteString): SignedContent[BytestringWithCryptographicEvidence] =
    SignedContent(
      BytestringWithCryptographicEvidence(bytes),
      Signature.noSignature,
      None,
      testedProtocolVersion,
    )

  private lazy val content = ByteString.copyFromUtf8("123")
  private lazy val defaultRequest: SubmissionRequest = {
    val sender: Member = participant
    val recipient = DefaultTestIdentities.participant2
    mkSubmissionRequest(
      Batch(
        List(
          ClosedEnvelope.create(content, Recipients.cc(recipient), Seq.empty, testedProtocolVersion)
        ),
        testedProtocolVersion,
      ),
      sender,
    )
  }

  private lazy val defaultConfirmationRequest: SubmissionRequest = {
    val sender: Member = participant
    val recipientPar = MemberRecipient(DefaultTestIdentities.participant2)
    val recipientMed = MediatorGroupRecipient(NonNegativeInt.zero)
    mkSubmissionRequest(
      Batch(
        List(
          ClosedEnvelope.create(
            content,
            Recipients.cc(recipientPar, recipientMed),
            Seq.empty,
            testedProtocolVersion,
          )
        ),
        testedProtocolVersion,
      ),
      sender,
    )
  }

  "send signed" should {
    def signedSubmissionReq(
        request: SubmissionRequest
    ): SignedContent[BytestringWithCryptographicEvidence] =
      signedContent(request.toByteString)

    def sendProto(signedRequest: ByteString)(implicit env: Environment): Future[Unit] = {
      import env.*

      val requestP = v30.SendAsyncRequest(signedRequest)
      val response = service.sendAsync(requestP)

      response.map(_ => ())
    }

    def send(request: SubmissionRequest)(implicit
        env: Environment
    ): Future[Unit] =
      sendProto(signedSubmissionReq(request).toByteString)

    def sendAndCheckSucceed(request: SubmissionRequest)(implicit
        env: Environment
    ): Future[Assertion] =
      send(request).map(_ => succeed)

    def sendAndCheckError(
        request: SubmissionRequest
    )(assertion: PartialFunction[Throwable, Assertion])(implicit
        env: Environment
    ): Future[Assertion] =
      send(request).failed.map { error =>
        assertion(error)
      }

    def sendProtoAndCheckError(
        versionedSignedRequest: ByteString,
        assertion: PartialFunction[Throwable, Assertion],
    )(implicit env: Environment): Future[Assertion] =
      sendProto(versionedSignedRequest).failed.map { error =>
        assertion(error)
      }

    "reject empty request" in { implicit env =>
      val requestV1 =
        protocolV30.SubmissionRequest(
          sender = "",
          messageId = "",
          batch = None,
          maxSequencingTime = 0L,
          topologyTimestamp = None,
          aggregationRule = None,
          submissionCost = None,
        )
      val signedRequestV0 = signedContent(
        VersionedMessage[SubmissionRequest](requestV1.toByteString, 0).toByteString
      )

      loggerFactory.assertLogs(
        sendProtoAndCheckError(
          signedRequestV0.toByteString,
          { case ex: StatusRuntimeException =>
            ex.getMessage should include("ValueConversionError(sender,Invalid member ``")
          },
        ),
        _.warningMessage should include("ValueConversionError(sender,Invalid member ``"),
      )
    }

    "reject envelopes with empty content" in { implicit env =>
      val request = defaultRequest
        .focus(_.batch.envelopes)
        .modify(_.map(_.focus(_.bytes).replace(ByteString.EMPTY)))

      loggerFactory.assertLogs(
        sendAndCheckError(request) { case ex: StatusRuntimeException =>
          ex.getMessage should include("Batch contains envelope without content.")
        },
        _.warningMessage should include("Batch contains envelope without content."),
      )
    }

    "reject envelopes with invalid sender" in { implicit env =>
      val requestV1 = defaultRequest.toProtoV30.focus(_.sender).modify {
        case "" => fail("sender should be set")
        case _sender => "THISWILLFAIL"
      }
      val signedRequestV0 = signedContent(
        VersionedMessage[SubmissionRequest](requestV1.toByteString, 0).toByteString
      )
      loggerFactory.assertLogs(
        sendProtoAndCheckError(
          signedRequestV0.toByteString,
          { case ex: StatusRuntimeException =>
            ex.getMessage should include(
              "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
            )
          },
        ),
        _.warningMessage should include(
          "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
        ),
      )
    }

    "reject large messages" in { implicit env =>
      val bigEnvelope =
        ClosedEnvelope.create(
          ByteString.copyFromUtf8(scala.util.Random.nextString(5000)),
          Recipients.cc(participant),
          Seq.empty,
          testedProtocolVersion,
        )
      val request = defaultRequest.focus(_.batch.envelopes).replace(List(bigEnvelope))

      val alarmMsg = s"Max bytes to decompress is exceeded. The limit is 1000 bytes."
      loggerFactory.assertLogs(
        sendAndCheckError(request) { case ex: StatusRuntimeException =>
          ex.getMessage should include(alarmMsg)
        },
        _.shouldBeCantonError(
          SequencerError.MaxRequestSizeExceeded,
          _ shouldBe alarmMsg,
        ),
      )
    }

    "reject unauthorized authenticated participant" in { implicit env =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.participant2)

      loggerFactory.assertLogs(
        sendAndCheckError(request) { case ex: StatusRuntimeException =>
          ex.getMessage should (include("is not authorized to send:")
            and include("just tried to use sequencer on behalf of"))
        },
        _.warningMessage should (include("is not authorized to send:")
          and include("just tried to use sequencer on behalf of")),
      )
    }

    "reject on confirmation rate excess" in { implicit env =>
      def expectSuccess(): Future[Assertion] =
        sendAndCheckSucceed(defaultConfirmationRequest)

      def expectOneSuccessOneOverloaded(): Future[Assertion] = {
        val result1F =
          send(defaultConfirmationRequest).transform(res => Success(res.toEither.swap.toOption))
        val result2F =
          send(defaultConfirmationRequest).transform(res => Success(res.toEither.swap.toOption))
        for {
          result1 <- result1F
          result2 <- result2F
        } yield {
          def assertOverloadedError(error: Throwable): Assertion =
            error match {
              case ex: StatusRuntimeException =>
                ex.getMessage should include(SequencerErrors.Overloaded.id)
                ex.getMessage should endWith("Submission rate exceeds rate limit of 5/s.")
              case wrongError =>
                fail(s"Unexpected error: $wrongError, expected Overloaded error instead")
            }
          (result1, result2) match {
            case (Some(error), None) => assertOverloadedError(error)
            case (None, Some(error)) => assertOverloadedError(error)
            case (Some(_), Some(_)) =>
              fail("at least one successful submission expected, but both failed")
            case (None, None) =>
              fail("at least one overloaded submission expected, but none failed")
          }
        }
      }

      for {
        _ <- expectSuccess() // push us beyond the max rate
        // Don't submit as we don't know when the current cycle ends
        _ = Threading.sleep(1000) // recover
        _ <- expectOneSuccessOneOverloaded()
        _ = Threading.sleep(1000)
        _ <- expectOneSuccessOneOverloaded()
      } yield succeed
    }

    def multipleMediatorTestCase(
        mediator1: RecipientsTree,
        mediator2: RecipientsTree,
    ): FixtureParam => Future[Assertion] = { _ =>
      val differentEnvelopes = Batch.fromClosed(
        testedProtocolVersion,
        ClosedEnvelope.create(
          ByteString.copyFromUtf8("message to first mediator"),
          Recipients(NonEmpty.mk(Seq, mediator1)),
          Seq.empty,
          testedProtocolVersion,
        ),
        ClosedEnvelope.create(
          ByteString.copyFromUtf8("message to second mediator"),
          Recipients(NonEmpty.mk(Seq, mediator2)),
          Seq.empty,
          testedProtocolVersion,
        ),
      )
      val sameEnvelope = Batch.fromClosed(
        testedProtocolVersion,
        ClosedEnvelope.create(
          ByteString.copyFromUtf8("message to two mediators and the participant"),
          Recipients(
            NonEmpty(
              Seq,
              RecipientsTree.ofMembers(
                NonEmpty.mk(Set, participant),
                Seq(
                  mediator1,
                  mediator2,
                ),
              ),
            )
          ),
          Seq.empty,
          testedProtocolVersion,
        ),
      )

      val batches = Seq(differentEnvelopes, sameEnvelope)
      val badRequests = for {
        batch <- batches
        sender <- Seq(
          participant,
          DefaultTestIdentities.daMediator,
          DefaultTestIdentities.daSequencerId,
        )
      } yield mkSubmissionRequest(batch, sender) -> sender
      for {
        _ <- MonadUtil.sequentialTraverse_(badRequests.zipWithIndex) {
          case ((badRequest, sender), index) =>
            withClue(s"bad request #$index") {
              // create a fresh environment for each request such that the rate limiter does not complain
              val participantEnv = new Environment(sender)
              loggerFactory.assertLogs(
                sendAndCheckError(badRequest) { case ex: StatusRuntimeException =>
                  ex.getMessage should include("Batch contains multiple mediators as recipients.")
                }(participantEnv),
                _.warningMessage should include("Batch contains multiple mediators as recipients."),
              )
            }
        }
      } yield succeed
    }

    "reject sending to multiple mediators" in multipleMediatorTestCase(
      RecipientsTree.leaf(NonEmpty.mk(Set, DefaultTestIdentities.daMediator)),
      RecipientsTree.leaf(
        NonEmpty.mk(Set, MediatorId(UniqueIdentifier.tryCreate("another", "mediator")))
      ),
    )

    "reject sending to multiple mediator groups" in multipleMediatorTestCase(
      RecipientsTree(
        NonEmpty.mk(
          Set,
          MediatorGroupRecipient(MediatorGroupIndex.one),
        ),
        Seq.empty,
      ),
      RecipientsTree(
        NonEmpty.mk(
          Set,
          MediatorGroupRecipient(MediatorGroupIndex.tryCreate(2)),
        ),
        Seq.empty,
      ),
    )

    "reject unachievable threshold in aggregation rule" in { implicit env =>
      val request = defaultRequest
        .focus(_.topologyTimestamp)
        .replace(Some(CantonTimestamp.ofEpochSecond(1)))
        .focus(_.aggregationRule)
        .replace(
          Some(
            AggregationRule(
              eligibleMembers = NonEmpty(Seq, participant, participant),
              threshold = PositiveInt.tryCreate(2),
              testedProtocolVersion,
            )
          )
        )
      loggerFactory.assertLogs(
        sendAndCheckError(request) { case ex: StatusRuntimeException =>
          ex.getMessage should include("Threshold 2 cannot be reached")
        },
        _.warningMessage should include("Threshold 2 cannot be reached"),
      )
    }

    "reject uneligible sender in aggregation rule" in { implicit env =>
      val request = defaultRequest
        .focus(_.topologyTimestamp)
        .replace(Some(CantonTimestamp.ofEpochSecond(1)))
        .focus(_.aggregationRule)
        .replace(
          Some(
            AggregationRule(
              eligibleMembers = NonEmpty(Seq, DefaultTestIdentities.participant2),
              threshold = PositiveInt.tryCreate(1),
              testedProtocolVersion,
            )
          )
        )
      loggerFactory.assertLogs(
        sendAndCheckError(request) { case ex: StatusRuntimeException =>
          ex.getMessage should include(
            s"Sender [$participant] is not eligible according to the aggregation rule"
          )
        },
        _.warningMessage should include(
          s"Sender [$participant] is not eligible according to the aggregation rule"
        ),
      )
    }
  }

  "versionedSubscribe" should {
    "return error if called with observer not capable of observing server calls" in { env =>
      val observer = new MockStreamObserver[v30.SubscriptionResponse]()
      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeV2(
          v30.SubscriptionRequestV2(
            member = "",
            timestamp = Some(CantonTimestamp.Epoch.toProtoPrimitive),
          ),
          observer,
        )
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INTERNAL =>
      }
    }

    "return error if request cannot be deserialized" in { env =>
      val observer = new MockServerStreamObserver[v30.SubscriptionResponse]()
      env.service.subscribeV2(
        v30.SubscriptionRequestV2(
          member = "",
          timestamp = Some(CantonTimestamp.Epoch.toProtoPrimitive),
        ),
        observer,
      )

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INVALID_ARGUMENT =>
      }
    }

    "return error if pool registration fails" in { env =>
      val observer = new MockServerStreamObserver[v30.SubscriptionResponse]()
      val requestP =
        SubscriptionRequestV2(
          participant,
          timestamp = None,
          testedProtocolVersion,
        ).toProtoV30

      Mockito
        .when(
          env.subscriptionPool.create(
            ArgumentMatchers.any[() => Subscription](),
            ArgumentMatchers.any[Member](),
          )(anyTraceContext)
        )
        .thenReturn(Left(PoolClosed))

      env.service.subscribeV2(requestP, observer)

      inside(observer.items.loneElement) { case StreamError(ex: StatusException) =>
        ex.getStatus.getCode shouldBe UNAVAILABLE
        ex.getStatus.getDescription shouldBe "Subscription pool is closed."
      }
    }

    "return error if sending request with member that is not authenticated" in { env =>
      val observer = new MockServerStreamObserver[v30.SubscriptionResponse]()
      val requestP =
        SubscriptionRequestV2(
          ParticipantId("Wrong participant"),
          timestamp = Some(CantonTimestamp.Epoch),
          testedProtocolVersion,
        ).toProtoV30

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeV2(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }
  }

  def performAcknowledgeRequest(env: Environment)(request: AcknowledgeRequest) =
    env.service.acknowledgeSigned(signedAcknowledgeReq(request.toProtoV30))

  def signedAcknowledgeReq(requestP: v30.AcknowledgeRequest): v30.AcknowledgeSignedRequest =
    v30.AcknowledgeSignedRequest(
      signedContent(VersionedMessage(requestP.toByteString, 0).toByteString).toByteString
    )

  "acknowledgeSigned" should {
    val defaultAcknowledgedRequest =
      AcknowledgeRequest(participant, CantonTimestamp.Epoch, testedProtocolVersion)

    "reject unauthorized authenticated participant" in { implicit env =>
      val unauthorizedParticipant = DefaultTestIdentities.participant2
      val req =
        AcknowledgeRequest(
          unauthorizedParticipant,
          CantonTimestamp.Epoch,
          testedProtocolVersion,
        )

      loggerFactory.assertLogs(
        performAcknowledgeRequest(env)(req).failed.map(error =>
          error.getMessage should include("PERMISSION_DENIED")
        ),
        _.warningMessage should (include("Authentication check failed:")
          and include("just tried to use sequencer on behalf of")),
      )
    }

    "succeed with correct participant" in { implicit env =>
      performAcknowledgeRequest(env)(defaultAcknowledgedRequest).map(_ => succeed)
    }

    "conflate acknowledgements within the conflate window" in { implicit env =>
      import com.digitalasset.canton.discard.Implicits.DiscardOps

      loggerFactory.assertLogs(Level(event.Level.DEBUG))(
        {
          // Assume that those 2 happen within acknowledgementsConflateWindow (set to 3s in the test)
          performAcknowledgeRequest(env)(defaultAcknowledgedRequest).futureValue
          performAcknowledgeRequest(env)(defaultAcknowledgedRequest).futureValue
          verify(env.sequencer, times(1))
            .acknowledgeSigned(any[SignedContent[AcknowledgeRequest]])(any[TraceContext])
            .discard
        },
        _.debugMessage should include("Discarding acknowledgement"),
      )
      // Wait for the window
      Threading.sleep(env.acknowledgementsConflateWindow.duration.toMillis)
      // Next ack should go through
      performAcknowledgeRequest(env)(defaultAcknowledgedRequest).futureValue
      verify(env.sequencer, times(2))
        .acknowledgeSigned(any[SignedContent[AcknowledgeRequest]])(any[TraceContext])
        .discard
      Future.successful(succeed)
    }
  }

  "downloadTopologyStateForInit" should {
    "stream batches of topology transactions" in { env =>
      val observer = new MockStreamObserver[v30.DownloadTopologyStateForInitResponse]()
      env.service.downloadTopologyStateForInit(
        TopologyStateForInitRequest(participant, testedProtocolVersion).toProtoV30,
        observer,
      )

      eventually() {
        // wait for the steam to be complete
        observer.items.lastOption shouldBe Some(StreamComplete)
      }
      val parsed = observer.items.toSeq.map {
        case StreamNext(response: v30.DownloadTopologyStateForInitResponse) =>
          StreamNext[TopologyStateForInitResponse](
            TopologyStateForInitResponse
              .fromProtoV30(response)
              .getOrElse(sys.error("error converting response from protobuf"))
          )
        case StreamComplete => StreamComplete
        case StreamError(t) => StreamError(t)
      }
      parsed should matchPattern {
        case Seq(
              StreamNext(batch1: TopologyStateForInitResponse),
              StreamNext(batch2: TopologyStateForInitResponse),
              StreamNext(batch3: TopologyStateForInitResponse),
              StreamComplete,
            )
            if Seq(batch1, batch2, batch3).forall(
              _.topologyTransactions.value.result.sizeIs == env.maxItemsInTopologyBatch
            ) =>
      }
    }
  }
}

private object GrpcSequencerServiceTest {
  class MockStreamObserver[T] extends StreamObserver[T] with RecordStreamObserverItems[T]

  class MockServerStreamObserver[T]
      extends ServerCallStreamObserver[T]
      with RecordStreamObserverItems[T] {
    override def isCancelled: Boolean = ???

    override def setOnCancelHandler(onCancelHandler: Runnable): Unit = ???

    override def setCompression(compression: String): Unit = ???

    override def isReady: Boolean = ???

    override def setOnReadyHandler(onReadyHandler: Runnable): Unit = ???

    override def disableAutoInboundFlowControl(): Unit = ???

    override def request(count: Int): Unit = ???

    override def setMessageCompression(enable: Boolean): Unit = ???
  }

  class MockSubscription extends CloseNotification with AutoCloseable {
    override def close(): Unit = {}
  }
}
