// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.metrics.SequencerTestMetrics
import com.digitalasset.canton.domain.sequencing.SequencerParameters
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.service.SubscriptionPool.PoolClosed
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParametersLookup,
  TestDomainParameters,
  v30 as protocolV30,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.BytestringWithCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactoryX,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TopologyStateForInitializationService,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{ProtocolVersion, VersionedMessage}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAsyncWordSpec,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import io.grpc.Status.Code.*
import io.grpc.StatusException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import monocle.macros.syntax.lens.*
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class GrpcSequencerServiceTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with HasExecutionContext {
  type Subscription = GrpcManagedSubscription[?]

  import GrpcSequencerServiceTest.*

  private lazy val participant = DefaultTestIdentities.participant1
  private lazy val crypto = new SymbolicPureCrypto
  private lazy val unauthenticatedMember =
    UnauthenticatedMemberId.tryCreate(participant.uid.namespace)(crypto)

  class Environment(member: Member) extends Matchers {
    val sequencer: Sequencer = mock[Sequencer]
    when(sequencer.sendAsync(any[SubmissionRequest])(anyTraceContext))
      .thenReturn(EitherT.rightT[Future, SendAsyncError](()))
    when(sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
      .thenReturn(EitherT.rightT[Future, SendAsyncError](()))
    when(sequencer.acknowledge(any[Member], any[CantonTimestamp])(anyTraceContext))
      .thenReturn(Future.unit)
    when(sequencer.acknowledgeSigned(any[SignedContent[AcknowledgeRequest]])(anyTraceContext))
      .thenReturn(EitherT.rightT(()))
    val cryptoApi: DomainSyncCryptoClient =
      TestingIdentityFactoryX(loggerFactory).forOwnerAndDomain(member)
    val subscriptionPool: SubscriptionPool[Subscription] =
      mock[SubscriptionPool[GrpcManagedSubscription[?]]]

    private val confirmationRequestsMaxRate = NonNegativeInt.tryCreate(5)
    private val maxRequestSize = NonNegativeInt.tryCreate(1000)
    val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
    private val topologyClient = mock[DomainTopologyClient]
    private val mockTopologySnapshot = mock[TopologySnapshot]
    when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
      .thenReturn(mockTopologySnapshot)
    when(
      mockTopologySnapshot.findDynamicDomainParametersOrDefault(any[ProtocolVersion], anyBoolean)(
        any[TraceContext]
      )
    )
      .thenReturn(
        Future.successful(
          TestDomainParameters.defaultDynamic(
            confirmationRequestsMaxRate = confirmationRequestsMaxRate,
            maxRequestSize = MaxRequestSize(maxRequestSize),
          )
        )
      )

    private val domainParamLookup: DynamicDomainParametersLookup[SequencerDomainParameters] =
      DomainParametersLookup.forSequencerDomainParameters(
        BaseTest.defaultStaticDomainParameters,
        None,
        topologyClient,
        FutureSupervisor.Noop,
        loggerFactory,
      )
    private val params = new SequencerParameters {
      override def maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(1e-6)
      override def processingTimeouts: ProcessingTimeout = timeouts
    }

    val maxItemsInTopologyBatch = 5
    private val numBatches = 3
    private val topologyInitService: TopologyStateForInitializationService =
      new TopologyStateForInitializationService {
        val factoryX =
          new TopologyTransactionTestFactoryX(loggerFactory, initEc = parallelExecutionContext)

        override def initialSnapshot(member: Member)(implicit
            executionContext: ExecutionContext,
            traceContext: TraceContext,
        ): Future[GenericStoredTopologyTransactionsX] = Future.successful(
          StoredTopologyTransactionsX(
            // As we don't expect the actual transactions in this test, we can repeat the same transaction a bunch of times
            List
              .fill(maxItemsInTopologyBatch * numBatches)(factoryX.ns1k1_k1)
              .map(
                StoredTopologyTransactionX(
                  SequencedTime.MinValue,
                  EffectiveTime.MinValue,
                  None,
                  _,
                )
              )
          )
        )
      }
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
        domainParamLookup,
        params,
        Some(topologyInitService),
        BaseTest.testedProtocolVersion,
        enableBroadcastOfUnauthenticatedMessages = false,
        maxItemsInTopologyResponse = PositiveInt.tryCreate(maxItemsInTopologyBatch),
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
      isRequest = true,
      batch,
      CantonTimestamp.MaxValue,
      None,
      None,
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

  "send signed" should {
    def signedSubmissionReq(
        request: SubmissionRequest
    ): SignedContent[BytestringWithCryptographicEvidence] =
      signedContent(request.toByteString)

    def sendProto(
        versionedRequest: ByteString,
        versionedSignedRequest: ByteString,
        authenticated: Boolean,
    )(implicit
        env: Environment
    ): Future[ParsingResult[SendAsyncUnauthenticatedVersionedResponse]] = {
      import env.*

      if (!authenticated) {
        val requestP = v30.SendAsyncUnauthenticatedVersionedRequest(versionedRequest)
        val response = service.sendAsyncUnauthenticatedVersioned(requestP)
        response.map(
          SendAsyncUnauthenticatedVersionedResponse.fromSendAsyncUnauthenticatedVersionedResponseProto
        )
      } else {
        val requestP = v30.SendAsyncVersionedRequest(versionedSignedRequest)
        val response = service.sendAsyncVersioned(requestP)

        response.map(SendAsyncUnauthenticatedVersionedResponse.fromSendAsyncVersionedResponseProto)
      }
    }

    def send(request: SubmissionRequest, authenticated: Boolean)(implicit
        env: Environment
    ): Future[ParsingResult[SendAsyncUnauthenticatedVersionedResponse]] = {
      val signedRequest = signedSubmissionReq(request)
      sendProto(
        request.toByteString,
        signedRequest.toByteString,
        authenticated,
      )
    }

    def sendAndCheckSucceed(request: SubmissionRequest)(implicit
        env: Environment
    ): Future[Assertion] =
      send(request, authenticated = true).map { responseP =>
        responseP.value.error shouldBe None
      }

    def sendAndCheckError(
        request: SubmissionRequest,
        authenticated: Boolean = true,
    )(assertion: PartialFunction[SendAsyncError, Assertion])(implicit
        env: Environment
    ): Future[Assertion] =
      send(request, authenticated).map { responseP =>
        assertion(responseP.value.error.value)
      }

    def sendProtoAndCheckError(
        versionedRequest: ByteString,
        versionedSignedRequest: ByteString,
        assertion: PartialFunction[SendAsyncError, Assertion],
        authenticated: Boolean = true,
    )(implicit env: Environment): Future[Assertion] =
      sendProto(
        versionedRequest,
        versionedSignedRequest,
        authenticated,
      ).map { responseP =>
        assertion(responseP.value.error.value)
      }

    "reject empty request" in { implicit env =>
      val requestV1 = protocolV30.SubmissionRequest("", "", isRequest = false, None, 0L, None, None)
      val signedRequestV0 = signedContent(
        VersionedMessage[SubmissionRequest](requestV1.toByteString, 0).toByteString
      )

      loggerFactory.assertLogs(
        sendProtoAndCheckError(
          VersionedMessage(requestV1.toByteString, 0).toByteString,
          signedRequestV0.toByteString,
          { case SendAsyncError.RequestInvalid(message) =>
            message should startWith("ValueConversionError(sender,Invalid member ``")
          },
        ),
        _.warningMessage should startWith("ValueConversionError(sender,Invalid member ``"),
      )
    }

    "reject envelopes with empty content" in { implicit env =>
      val request = defaultRequest
        .focus(_.batch.envelopes)
        .modify(_.map(_.focus(_.bytes).replace(ByteString.EMPTY)))

      loggerFactory.assertLogs(
        sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
          message shouldBe "Batch contains envelope without content."
        },
        _.warningMessage should endWith(
          "is invalid: Batch contains envelope without content."
        ),
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
          VersionedMessage(requestV1.toByteString, 0).toByteString,
          signedRequestV0.toByteString,
          { case SendAsyncError.RequestInvalid(message) =>
            message should startWith(
              "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
            )
          },
        ),
        _.warningMessage should startWith(
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
        sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
          message should include(alarmMsg)
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
        sendAndCheckError(request) { case SendAsyncError.RequestRefused(message) =>
          message should (include("is not authorized to send:")
            and include("just tried to use sequencer on behalf of"))
        },
        _.warningMessage should (include("is not authorized to send:")
          and include("just tried to use sequencer on behalf of")),
      )
    }

    "reject unauthenticated member that uses authenticated send" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)

      loggerFactory.assertLogs(
        sendAndCheckError(request, authenticated = true) {
          case SendAsyncError.RequestRefused(message) =>
            message should include("needs to use unauthenticated send operation")
        }(new Environment(unauthenticatedMember)),
        _.warningMessage should include("needs to use unauthenticated send operation"),
      )
    }

    "reject non domain manager authenticated member sending message to unauthenticated member" in {
      implicit env =>
        val request = defaultRequest
          .focus(_.batch)
          .replace(
            Batch(
              List(
                ClosedEnvelope.create(
                  content,
                  Recipients.cc(unauthenticatedMember),
                  Seq.empty,
                  testedProtocolVersion,
                )
              ),
              testedProtocolVersion,
            )
          )
        loggerFactory.assertLogs(
          sendAndCheckError(request, authenticated = true) {
            case SendAsyncError.RequestRefused(message) =>
              message should include("Member is trying to send message to unauthenticated")
          },
          _.warningMessage should include(
            "Member is trying to send message to unauthenticated"
          ),
        )
    }

    "succeed authenticated domain manager sending message to unauthenticated member" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.domainManager)
        .focus(_.batch)
        .replace(
          Batch(
            List(
              ClosedEnvelope.create(
                content,
                Recipients.cc(unauthenticatedMember),
                Seq.empty,
                testedProtocolVersion,
              )
            ),
            testedProtocolVersion,
          )
        )
      val domEnvironment = new Environment(DefaultTestIdentities.domainManager)
      sendAndCheckSucceed(request)(domEnvironment)
    }

    "succeed unauthenticated member sending message to domain manager" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)
        .focus(_.batch)
        .replace(
          Batch(
            List(
              ClosedEnvelope.create(
                content,
                Recipients.cc(DefaultTestIdentities.domainManager),
                Seq.empty,
                testedProtocolVersion,
              )
            ),
            testedProtocolVersion,
          )
        )
      val newEnv = new Environment(unauthenticatedMember).service
      val responseF =
        newEnv.sendAsyncUnauthenticatedVersioned(
          v30.SendAsyncUnauthenticatedVersionedRequest(request.toByteString)
        )

      responseF
        .map { responseP =>
          val response = SendAsyncUnauthenticatedVersionedResponse
            .fromSendAsyncUnauthenticatedVersionedResponseProto(responseP)
          response.value.error shouldBe None
        }
    }

    "reject on rate excess" in { implicit env =>
      def expectSuccess(): Future[Assertion] = {
        sendAndCheckSucceed(defaultRequest)
      }

      def expectOneSuccessOneOverloaded(): Future[Assertion] = {
        val result1F = send(defaultRequest, authenticated = true)
        val result2F = send(defaultRequest, authenticated = true)
        for {
          result1 <- result1F
          result2 <- result2F
        } yield {
          def assertOverloadedError(error: SendAsyncError): Assertion =
            error match {
              case SendAsyncError.Overloaded(message) =>
                message should endWith("Submission rate exceeds rate limit of 5/s.")
              case wrongError =>
                fail(s"Unexpected error: $wrongError, expected Overloaded error instead")
            }
          (result1.value.error, result2.value.error) match {
            case (Some(error), None) => assertOverloadedError(error)
            case (None, Some(error)) => assertOverloadedError(error)
            case (Some(_), Some(_)) =>
              fail("at least one successful submition expected, but both failed")
            case (None, None) => fail("at least one overloaded submition expected, but none failed")
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

      val domainManager: Member = DefaultTestIdentities.domainManager

      val batches = Seq(differentEnvelopes, sameEnvelope)
      val badRequests = batches.map(batch => mkSubmissionRequest(batch, participant))
      val goodRequests = batches.map(batch =>
        mkSubmissionRequest(
          batch,
          DefaultTestIdentities.mediator,
        ) -> DefaultTestIdentities.mediator
      ) ++ batches.map(batch =>
        mkSubmissionRequest(
          batch,
          domainManager,
        ) -> domainManager
      )
      for {
        _ <- MonadUtil.sequentialTraverse_(badRequests.zipWithIndex) { case (badRequest, index) =>
          withClue(s"bad request #$index") {
            // create a fresh environment for each request such that the rate limiter does not complain
            val participantEnv = new Environment(participant)
            loggerFactory.assertLogs(
              sendAndCheckError(badRequest) { case SendAsyncError.RequestRefused(message) =>
                message shouldBe "Batch from participant contains multiple mediators as recipients."
              }(participantEnv),
              _.warningMessage should include(
                "refused: Batch from participant contains multiple mediators as recipients."
              ),
            )
          }
        }
        // We don't need log suppression for the good requests so we can run them in parallel
        _ <- goodRequests.zipWithIndex.parTraverse_ { case ((goodRequest, sender), index) =>
          withClue(s"good request #$index") {
            val senderEnv = new Environment(sender)
            sendAndCheckSucceed(goodRequest)(senderEnv)
          }
        }
      } yield succeed
    }

    "reject sending to multiple mediators iff the sender is a participant" in multipleMediatorTestCase(
      RecipientsTree.leaf(NonEmpty.mk(Set, DefaultTestIdentities.mediator)),
      RecipientsTree.leaf(
        NonEmpty.mk(Set, MediatorId(UniqueIdentifier.tryCreate("another", "mediator")))
      ),
    )

    "reject sending to multiple mediator groups iff the sender is a participant" in multipleMediatorTestCase(
      RecipientsTree(
        NonEmpty.mk(
          Set,
          MediatorsOfDomain(MediatorGroupIndex.one),
        ),
        Seq.empty,
      ),
      RecipientsTree(
        NonEmpty.mk(
          Set,
          MediatorsOfDomain(MediatorGroupIndex.tryCreate(2)),
        ),
        Seq.empty,
      ),
    )

    "reject requests to unauthenticated members with a signing key timestamps" in { implicit env =>
      val request = defaultRequest
        .focus(_.topologyTimestamp)
        .replace(Some(CantonTimestamp.ofEpochSecond(1)))
        .focus(_.batch)
        .replace(
          Batch(
            List(
              ClosedEnvelope.create(
                content,
                Recipients.cc(unauthenticatedMember),
                Seq.empty,
                testedProtocolVersion,
              )
            ),
            testedProtocolVersion,
          )
        )

      loggerFactory.assertLogs(
        sendAndCheckError(request) { case SendAsyncError.RequestRefused(message) =>
          message should include(
            "Requests sent from or to unauthenticated members must not specify the topology timestamp"
          )
        },
        _.warningMessage should include(
          "Requests sent from or to unauthenticated members must not specify the topology timestamp"
        ),
      )
    }

    "reject unauthenticated eligible members in aggregation rule" in { implicit env =>
      val request = defaultRequest
        .focus(_.topologyTimestamp)
        .replace(Some(CantonTimestamp.ofEpochSecond(1)))
        .focus(_.aggregationRule)
        .replace(
          Some(
            AggregationRule(
              eligibleMembers = NonEmpty(Seq, participant, unauthenticatedMember),
              threshold = PositiveInt.tryCreate(1),
              testedProtocolVersion,
            )
          )
        )
      loggerFactory.assertLogs(
        sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
          message should include(
            "Eligible senders in aggregation rule must be authenticated, but found unauthenticated members"
          )
        },
        _.warningMessage should include(
          "Eligible senders in aggregation rule must be authenticated, but found unauthenticated members"
        ),
      )
    }

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
        sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
          message should include("Threshold 2 cannot be reached")
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
        sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
          message should include("Sender is not eligible according to the aggregation rule")
        },
        _.warningMessage should include(
          "Sender is not eligible according to the aggregation rule"
        ),
      )
    }

    "reject unauthenticated member sending message to non domain manager member" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)

      val errorMsg =
        if (testedProtocolVersion >= ProtocolVersion.v30)
          "Unauthenticated member is trying to send message to members other than the topology broadcast address All"
        else
          "Unauthenticated member is trying to send message to members other than the domain manager"

      loggerFactory.assertLogs(
        sendAndCheckError(request, authenticated = false) {
          case SendAsyncError.RequestRefused(message) =>
            message should include(errorMsg)
        }(new Environment(unauthenticatedMember)),
        _.warningMessage should include(errorMsg),
      )
    }

    "reject authenticated member that uses unauthenticated send" in { implicit env =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.participant1)

      loggerFactory.assertLogs(
        sendAndCheckError(request, authenticated = false) {
          case SendAsyncError.RequestRefused(message) =>
            message should include("needs to use authenticated send operation")
        },
        _.warningMessage should include("needs to use authenticated send operation"),
      )
    }

    "reject requests from unauthenticated senders with a signing key timestamp" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)
        .focus(_.topologyTimestamp)
        .replace(Some(CantonTimestamp.Epoch))
        .focus(_.batch)
        .replace(
          Batch(
            List(
              ClosedEnvelope.create(
                content,
                Recipients.cc(DefaultTestIdentities.domainManager),
                Seq.empty,
                testedProtocolVersion,
              )
            ),
            testedProtocolVersion,
          )
        )

      loggerFactory.assertLogs(
        sendAndCheckError(request, authenticated = false) {
          case SendAsyncError.RequestRefused(message) =>
            message should include(
              "Requests sent from or to unauthenticated members must not specify the topology timestamp"
            )
        }(new Environment(unauthenticatedMember)),
        _.warningMessage should include(
          "Requests sent from or to unauthenticated members must not specify the topology timestamp"
        ),
      )
    }
  }

  "versionedSubscribe" should {
    "return error if called with observer not capable of observing server calls" in { env =>
      val observer = new MockStreamObserver[v30.VersionedSubscriptionResponse]()
      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeVersioned(
          v30.SubscriptionRequest(member = "", counter = 0L),
          observer,
        )
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INTERNAL =>
      }
    }

    "return error if request cannot be deserialized" in { env =>
      val observer = new MockServerStreamObserver[v30.VersionedSubscriptionResponse]()
      env.service.subscribeVersioned(v30.SubscriptionRequest(member = "", counter = 0L), observer)

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INVALID_ARGUMENT =>
      }
    }

    "return error if pool registration fails" in { env =>
      val observer = new MockServerStreamObserver[v30.VersionedSubscriptionResponse]()
      val requestP =
        SubscriptionRequest(
          participant,
          SequencerCounter.Genesis,
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

      env.service.subscribeVersioned(requestP, observer)

      inside(observer.items.loneElement) { case StreamError(ex: StatusException) =>
        ex.getStatus.getCode shouldBe UNAVAILABLE
        ex.getStatus.getDescription shouldBe "Subscription pool is closed."
      }
    }

    "return error if sending request with member that is not authenticated" in { env =>
      val observer = new MockServerStreamObserver[v30.VersionedSubscriptionResponse]()
      val requestP =
        SubscriptionRequest(
          ParticipantId("Wrong participant"),
          SequencerCounter.Genesis,
          testedProtocolVersion,
        ).toProtoV30

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeVersioned(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }

    "return error if authenticated member sending request unauthenticated endpoint" in { env =>
      val observer = new MockServerStreamObserver[v30.VersionedSubscriptionResponse]()
      val requestP =
        SubscriptionRequest(
          participant,
          SequencerCounter.Genesis,
          testedProtocolVersion,
        ).toProtoV30

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeUnauthenticatedVersioned(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }

    "return error if unauthenticated member sending request authenticated endpoint" in { env =>
      val observer = new MockServerStreamObserver[v30.VersionedSubscriptionResponse]()
      val requestP =
        SubscriptionRequest(
          unauthenticatedMember,
          SequencerCounter.Genesis,
          testedProtocolVersion,
        ).toProtoV30

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeVersioned(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }
  }

  "subscribe" should {

    "return protocol version error if protocol version >= v5 is used for subscribe" in { env =>
      val observer = new MockServerStreamObserver[v30.SubscriptionResponse]()
      val requestP =
        SubscriptionRequest(
          participant,
          SequencerCounter.Genesis,
          testedProtocolVersion,
        ).toProtoV30

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribe(requestP, observer)
      }

      val expectedMessage =
        "The versioned subscribe endpoints must be used with protocol version"

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException))
            if err.getStatus.getCode == UNIMPLEMENTED && err.getMessage.contains(
              expectedMessage
            ) =>
      }
    }

    "return protocol version error if protocol version >= v5 is used for subscribeUnauthenticated" in {
      env =>
        val observer = new MockServerStreamObserver[v30.SubscriptionResponse]()
        val requestP =
          SubscriptionRequest(
            unauthenticatedMember,
            SequencerCounter.Genesis,
            testedProtocolVersion,
          ).toProtoV30

        loggerFactory.suppressWarningsAndErrors {
          env.service.subscribeUnauthenticated(requestP, observer)
        }

        val expectedMessage =
          "The versioned subscribe endpoints must be used with protocol version"

        observer.items.toSeq should matchPattern {
          case Seq(StreamError(err: StatusException))
              if err.getStatus.getCode == UNIMPLEMENTED && err.getMessage.contains(
                expectedMessage
              ) =>
        }
    }
  }

  Seq(("acknowledge", false), ("acknowledgeSigned", true)).foreach { case (name, useSignedAck) =>
    def performAcknowledgeRequest(env: Environment)(request: AcknowledgeRequest) =
      if (useSignedAck) {
        env.service.acknowledgeSigned(signedAcknowledgeReq(request.toProtoV30))
      } else
        env.service.acknowledge(request.toProtoV30)

    def signedAcknowledgeReq(requestP: v30.AcknowledgeRequest): v30.AcknowledgeSignedRequest =
      v30.AcknowledgeSignedRequest(
        Some(signedContent(VersionedMessage(requestP.toByteString, 0).toByteString).toProtoV30)
      )

    name should {
      if (useSignedAck) {
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
          val req = AcknowledgeRequest(participant, CantonTimestamp.Epoch, testedProtocolVersion)
          performAcknowledgeRequest(env)(req).map(_ => succeed)
        }
      } else {
        "reject the acknowledgement" in { implicit env =>
          val req = AcknowledgeRequest(participant, CantonTimestamp.Epoch, testedProtocolVersion)
          performAcknowledgeRequest(env)(req).failed.map { error =>
            error.getMessage should (include("UNIMPLEMENTED") and include(
              s"acknowledgement endpoints must be used with protocol version $testedProtocolVersion"
            ))
          }
        }
      }
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
          StreamNext(
            TopologyStateForInitResponse
              .fromProtoV30(response)
              .getOrElse(sys.error("error converting response from protobuf"))
          )
        case otherwise => otherwise
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
  sealed trait StreamItem

  final case class StreamNext[A](value: A) extends StreamItem

  final case class StreamError(t: Throwable) extends StreamItem

  object StreamComplete extends StreamItem

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

  trait RecordStreamObserverItems[T] {
    this: StreamObserver[T] =>

    val items: mutable.Buffer[StreamItem] = mutable.Buffer[StreamItem]()

    override def onNext(value: T): Unit = items += StreamNext(value)

    override def onError(t: Throwable): Unit = items += StreamError(t)

    override def onCompleted(): Unit = items += StreamComplete
  }

  class MockSubscription extends CloseNotification with AutoCloseable {
    override def close(): Unit = {}
  }
}
