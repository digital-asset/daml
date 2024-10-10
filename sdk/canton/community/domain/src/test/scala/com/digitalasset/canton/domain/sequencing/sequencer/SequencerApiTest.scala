// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.ExceededMaxSequencingTime
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.SendAsyncError.RequestInvalid
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class SequencerApiTest
    extends SequencerApiTestUtils
    with ProtocolVersionChecksFixtureAsyncWordSpec {

  import RecipientsTest.*

  protected class Env extends AutoCloseable {

    implicit lazy val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    lazy val sequencer: CantonSequencer = {
      val sequencer = SequencerApiTest.this.createSequencer(
        topologyFactory.forOwnerAndDomain(owner = mediatorId, domainId)
      )
      registerAllTopologyMembers(topologyFactory.topologySnapshot(), sequencer)
      sequencer
    }

    val topologyFactory: TestingIdentityFactory =
      TestingTopology(domainParameters = List.empty)
        .withSimpleParticipants(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p17,
          p18,
          p19,
        )
        .build(loggerFactory)

    def sign(
        request: SubmissionRequest
    ): SignedContent[SubmissionRequest] = {
      val cryptoSnapshot =
        topologyFactory.forOwnerAndDomain(request.sender).currentSnapshotApproximation
      SignedContent
        .create(
          cryptoSnapshot.pureCrypto,
          cryptoSnapshot,
          request,
          Some(cryptoSnapshot.ipsSnapshot.timestamp),
          HashPurpose.SubmissionRequestSignature,
          testedProtocolVersion,
        )
        .futureValueUS
        .value
    }

    def close(): Unit = {
      sequencer.close()
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    }
  }

  override protected type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env
    complete {
      super.withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  var clock: Clock = _
  var driverClock: Clock = _

  def createClock(): Clock = new SimClock(loggerFactory = loggerFactory)

  def simClockOrFail(clock: Clock): SimClock =
    clock match {
      case simClock: SimClock => simClock
      case _ =>
        fail(
          "This test case is only compatible with SimClock for `clock` and `driverClock` fields"
        )
    }
  def domainId: DomainId = DefaultTestIdentities.domainId
  def mediatorId: MediatorId = DefaultTestIdentities.mediatorId
  def sequencerId: SequencerId = DefaultTestIdentities.sequencerId

  def createSequencer(crypto: DomainSyncCryptoClient)(implicit
      materializer: Materializer
  ): CantonSequencer

  protected def supportAggregation: Boolean

  protected def defaultExpectedTrafficReceipt: Option[TrafficReceipt]

  protected def runSequencerApiTests(): Unit = {
    "The sequencers" should {
      "send a batch to one recipient" in { env =>
        import env.*
        val messageContent = "hello"
        val sender = p7.member
        val recipients = Recipients.cc(sender)

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFailShutdown("Sent async")
          messages <- readForMembers(List(sender), sequencer)
        } yield {
          val details = EventDetails(
            SequencerCounter(0),
            sender,
            Some(request.messageId),
            defaultExpectedTrafficReceipt,
            EnvelopeDetails(messageContent, recipients),
          )
          checkMessages(List(details), messages)
        }
      }

      "not fail when a block is empty due to suppressed events" in { env =>
        import env.*

        val suppressedMessageContent = "suppressed message"
        // TODO(i10412): The sequencer implementations for tests currently do not all behave in the same way.
        // Until this is fixed, we are currently sidestepping the issue by using a different set of recipients
        // for each test to ensure "isolation".
        val sender = p7.member
        val recipients = Recipients.cc(sender)

        val tsInThePast = CantonTimestamp.MinValue

        val request = createSendRequest(
          sender,
          suppressedMessageContent,
          recipients,
          maxSequencingTime = tsInThePast,
        )

        for {
          messages <- loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
            sequencer
              .sendAsyncSigned(sign(request))
              .valueOrFailShutdown("Sent async")
              .flatMap(_ =>
                readForMembers(
                  List(sender),
                  sequencer,
                  timeout = 5.seconds, // We don't need the full timeout here
                )
              ),
            forAll(_) { entry =>
              entry.message should ((include(suppressedMessageContent) and {
                include(ExceededMaxSequencingTime.id) or include("Observed Send")
              }) or include("Detected new members without sequencer counter") or
                include regex "Creating .* at block height None" or
                include("Completing init") or
                include("Subscribing to block source from") or
                include("Advancing sim clock") or
                (include("Creating ForkJoinPool with parallelism") and include(
                  "to avoid starvation"
                )))
            },
          )
        } yield {
          checkMessages(List(), messages)
        }
      }

      "not fail when some events in a block are suppressed" in { env =>
        import env.*

        val normalMessageContent = "normal message"
        val suppressedMessageContent = "suppressed message"
        // TODO(i10412): See above
        val sender = p8.member
        val recipients = Recipients.cc(sender)

        val tsInThePast = CantonTimestamp.MinValue

        val request1 = createSendRequest(sender, normalMessageContent, recipients)
        val request2 = createSendRequest(
          sender,
          suppressedMessageContent,
          recipients,
          maxSequencingTime = tsInThePast,
        )

        for {
          _ <- sequencer.sendAsyncSigned(sign(request1)).valueOrFailShutdown("Sent async #1")
          messages <- loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
            sequencer
              .sendAsyncSigned(sign(request2))
              .valueOrFailShutdown("Sent async #2")
              .flatMap(_ => readForMembers(List(sender), sequencer)),
            forAll(_) { entry =>
              // block update generator will log every send
              entry.message should (include("Detected new members without sequencer counter") or
                (include(ExceededMaxSequencingTime.id) or include(
                  "Observed Send"
                ) and include(
                  suppressedMessageContent
                )) or (include("Observed Send") and include(normalMessageContent)))
            },
          )
        } yield {
          val details = EventDetails(
            SequencerCounter.Genesis,
            sender,
            Some(request1.messageId),
            defaultExpectedTrafficReceipt,
            EnvelopeDetails(normalMessageContent, recipients),
          )
          checkMessages(List(details), messages)
        }
      }

      "send recipients only the subtrees that they should see" in { env =>
        import env.*
        val messageContent = "msg1"
        val sender: MediatorId = mediatorId
        // TODO(i10412): See above
        val recipients = Recipients(NonEmpty(Seq, t5, t3))
        val readFor: List[Member] = recipients.allRecipients.collect {
          case MemberRecipient(member) =>
            member
        }.toList

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        val expectedDetailsForMembers = readFor.map { member =>
          EventDetails(
            SequencerCounter.Genesis,
            member,
            Option.when(member == sender)(request.messageId),
            if (member == sender) defaultExpectedTrafficReceipt else None,
            EnvelopeDetails(messageContent, recipients.forMember(member, Set.empty).value),
          )
        }

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFailShutdown("Sent async")
          reads <- readForMembers(readFor, sequencer)
        } yield {
          checkMessages(expectedDetailsForMembers, reads)
        }
      }

      def testAggregation: Boolean = supportAggregation

      "aggregate submission requests" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageContent = "aggregatable-message"
        // TODO(i10412): See above
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p6, p9), PositiveInt.tryCreate(2), testedProtocolVersion)
        val request1 = createSendRequest(
          p6,
          messageContent,
          Recipients.cc(p10),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          aggregationRule = Some(aggregationRule),
          topologyTimestamp = Some(CantonTimestamp.Epoch),
        )
        val request2 = request1.copy(sender = p9, messageId = MessageId.fromUuid(new UUID(1, 2)))

        for {
          _ <- sequencer
            .sendAsyncSigned(sign(request1))
            .valueOrFailShutdown("Sent async for participant1")
          reads1 <- readForMembers(Seq(p6), sequencer)
          _ <- sequencer
            .sendAsyncSigned(sign(request2))
            .valueOrFailShutdown("Sent async for participant2")
          reads2 <- readForMembers(Seq(p9), sequencer)
          reads3 <- readForMembers(Seq(p10), sequencer)
        } yield {
          // p6 gets the receipt immediately
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p6,
                Some(request1.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads1,
          )
          // p9 gets the receipt only
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p9,
                Some(request2.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads2,
          )
          // p10 gets the message
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p10,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(messageContent, Recipients.cc(p10)),
              )
            ),
            reads3,
          )
        }
      }

      "bounce on write path aggregate submissions with maxSequencingTime exceeding bound" onlyRunWhen testAggregation in {
        env =>
          import env.*

          val messageContent = "bounce-write-path-message"
          // TODO(i10412): See above
          val aggregationRule =
            AggregationRule(NonEmpty(Seq, p6, p9), PositiveInt.tryCreate(2), testedProtocolVersion)
          val request1 = createSendRequest(
            p6,
            messageContent,
            Recipients.cc(p10),
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofMinutes(10)),
            aggregationRule = Some(aggregationRule),
            topologyTimestamp = Some(CantonTimestamp.Epoch.add(Duration.ofSeconds(1))),
          )
          val request2 = request1.copy(
            sender = p9,
            messageId = MessageId.fromUuid(new UUID(1, 2)),
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofMinutes(-10)),
          )

          for {
            tooFarInTheFuture <- sequencer
              .sendAsyncSigned(sign(request1))
              .leftOrFailShutdown(
                "A sendAsync of submission with maxSequencingTime too far in the future"
              )
            inThePast <- sequencer
              .sendAsyncSigned(sign(request2))
              .leftOrFailShutdown(
                "A sendAsync of submission with maxSequencingTime in the past"
              )
          } yield {
            inside(tooFarInTheFuture) {
              case RequestInvalid(message)
                  if message.contains("is too far in the future") && message.contains(
                    "Max sequencing time"
                  ) =>
                succeed
            }
            inside(inThePast) {
              case RequestInvalid(message)
                  if message.contains("is already past the max sequencing time") && message
                    .contains("The sequencer clock timestamp") =>
                succeed
            }
          }
      }

      "bounce on read path aggregate submissions with maxSequencingTime exceeding bound" onlyRunWhen testAggregation in {
        env =>
          import env.*
          sequencer.discard // This is necessary to init the lazy val in the Env before manipulating the clocks

          val messageContent = "bounce-read-path-message"
          // TODO(i10412): See above
          val aggregationRule =
            AggregationRule(NonEmpty(Seq, p6, p9), PositiveInt.tryCreate(2), testedProtocolVersion)

          simClockOrFail(clock).advanceTo(CantonTimestamp.Epoch.add(Duration.ofSeconds(100)))

          val request1 = createSendRequest(
            p6,
            messageContent,
            Recipients.cc(p10),
            // Note:  write side clock is at 100s, which lets the request pass,
            //        read side clock is at 0s, which should produce an error due to the MST bound at 6m(=360s)
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(370)),
            aggregationRule = Some(aggregationRule),
            topologyTimestamp = Some(CantonTimestamp.Epoch),
          )

          for {
            _ <- sequencer
              .sendAsyncSigned(sign(request1))
              .valueOrFailShutdown("Sent async for participant1")
            _ = {
              simClockOrFail(clock).reset()
            }
            reads3 <- readForMembers(Seq(p6), sequencer)
          } yield {
            checkRejection(reads3, p6, request1.messageId, defaultExpectedTrafficReceipt) {
              case SequencerErrors.MaxSequencingTimeTooFar(reason) =>
                reason should (
                  include(s"Max sequencing time") and
                    include("is too far in the future")
                )
            }
          }
      }

      "aggregate signatures" onlyRunWhen testAggregation in { env =>
        import env.*

        // TODO(i10412): See above
        val aggregationRule =
          AggregationRule(
            NonEmpty(Seq, p11, p12, p13),
            PositiveInt.tryCreate(2),
            testedProtocolVersion,
          )

        val content1 = "message1-to-sign"
        val content2 = "message2-to-sign"
        val recipients1 = Recipients.cc(p11, p13)
        val envelope1 = ClosedEnvelope.create(
          ByteString.copyFromUtf8(content1),
          recipients1,
          Seq.empty,
          testedProtocolVersion,
        )
        val recipients2 = Recipients.cc(p12, p13)
        val envelope2 = ClosedEnvelope.create(
          ByteString.copyFromUtf8(content2),
          recipients2,
          Seq.empty,
          testedProtocolVersion,
        )
        val envelopes = List(envelope1, envelope2)
        val messageId1 = MessageId.tryCreate(s"request1")
        val messageId2 = MessageId.tryCreate(s"request2")
        val messageId3 = MessageId.tryCreate(s"request3")
        val p11Crypto = topologyFactory.forOwnerAndDomain(p11, domainId)
        val p12Crypto = topologyFactory.forOwnerAndDomain(p12, domainId)
        val p13Crypto = topologyFactory.forOwnerAndDomain(p13, domainId)

        def mkRequest(
            sender: Member,
            messageId: MessageId,
            envelopes: List[ClosedEnvelope],
        ): SubmissionRequest =
          SubmissionRequest.tryCreate(
            sender,
            messageId,
            Batch(envelopes, testedProtocolVersion),
            CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
            topologyTimestamp = Some(CantonTimestamp.Epoch),
            Some(aggregationRule),
            Option.empty[SequencingSubmissionCost],
            testedProtocolVersion,
          )

        for {
          envs1 <- envelopes.parTraverse(signEnvelope(p11Crypto, _))
          request1 = mkRequest(p11, messageId1, envs1)
          envs2 <- envelopes.parTraverse(signEnvelope(p12Crypto, _))
          request2 = mkRequest(p12, messageId2, envs2)
          _ <- sequencer
            .sendAsyncSigned(sign(request1))
            .valueOrFailShutdown("Sent async for participant11")
          reads11 <- readForMembers(Seq(p11), sequencer)
          _ <- sequencer
            .sendAsyncSigned(sign(request2))
            .valueOrFailShutdown("Sent async for participant13")
          reads12 <- readForMembers(Seq(p12, p13), sequencer)
          reads12a <- readForMembers(
            Seq(p11),
            sequencer,
            firstSequencerCounter = SequencerCounter.Genesis + 1,
          )

          // participant13 is late to the party and its request is refused
          envs3 <- envelopes.parTraverse(signEnvelope(p13Crypto, _))
          request3 = mkRequest(p13, messageId3, envs3)
          _ <- sequencer
            .sendAsyncSigned(sign(request3))
            .valueOrFailShutdown("Sent async for participant13")
          reads13 <- readForMembers(
            Seq(p13),
            sequencer,
            firstSequencerCounter = SequencerCounter.Genesis + 1,
          )
        } yield {
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p11,
                Some(request1.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads11,
          )
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p12,
                Some(request1.messageId),
                defaultExpectedTrafficReceipt,
                EnvelopeDetails(content2, recipients2, envs1(1).signatures ++ envs2(1).signatures),
              ),
              EventDetails(
                SequencerCounter.Genesis,
                p13,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(content1, recipients1, envs1(0).signatures ++ envs2(0).signatures),
                EnvelopeDetails(content2, recipients2, envs1(1).signatures ++ envs2(1).signatures),
              ),
            ),
            reads12,
          )
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis + 1,
                p11,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(content1, recipients1, envs1(0).signatures ++ envs2(0).signatures),
              )
            ),
            reads12a,
          )

          checkRejection(reads13, p13, messageId3, defaultExpectedTrafficReceipt) {
            case SequencerErrors.AggregateSubmissionAlreadySent(reason) =>
              reason should (
                include(s"The aggregatable request with aggregation ID") and
                  include("was previously delivered at")
              )
          }
        }
      }

      "prevent aggregation stuffing" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageContent = "aggregatable-message-stuffing"
        // TODO(i10412): See above
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p14, p15), PositiveInt.tryCreate(2), testedProtocolVersion)
        val recipients = Recipients.cc(p14, p15)
        val envelope = ClosedEnvelope.create(
          ByteString.copyFromUtf8(messageContent),
          recipients,
          Seq.empty,
          testedProtocolVersion,
        )
        val messageId1 = MessageId.tryCreate(s"request1")
        val messageId2 = MessageId.tryCreate(s"request2")
        val messageId3 = MessageId.tryCreate(s"request3")
        val p14Crypto = topologyFactory.forOwnerAndDomain(p14, domainId)
        val p15Crypto = topologyFactory.forOwnerAndDomain(p15, domainId)

        def mkRequest(
            sender: Member,
            messageId: MessageId,
            envelope: ClosedEnvelope,
        ): SubmissionRequest =
          SubmissionRequest.tryCreate(
            sender,
            messageId,
            Batch(List(envelope), testedProtocolVersion),
            CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
            topologyTimestamp = Some(CantonTimestamp.Epoch),
            Some(aggregationRule),
            Option.empty[SequencingSubmissionCost],
            testedProtocolVersion,
          )

        for {
          env1 <- signEnvelope(p14Crypto, envelope)
          request1 = mkRequest(p14, messageId1, env1)
          env2 <- signEnvelope(p14Crypto, envelope)
          request2 = mkRequest(p14, messageId2, env2)
          env3 <- signEnvelope(p15Crypto, envelope)
          request3 = mkRequest(p15, messageId3, env3)
          _ <- sequencer
            .sendAsyncSigned(sign(request1))
            .valueOrFailShutdown("Sent async for participant14")
          reads14 <- readForMembers(Seq(p14), sequencer)
          _ <- sequencer
            .sendAsyncSigned(sign(request2))
            .valueOrFailShutdown("Sent async stuffing for participant14")
          reads14a <- readForMembers(
            Seq(p14),
            sequencer,
            firstSequencerCounter = SequencerCounter.Genesis + 1,
          )
          // p15 can still continue and finish the aggregation
          _ <- sequencer
            .sendAsyncSigned(sign(request3))
            .valueOrFailShutdown("Sent async for participant15")
          reads14b <- readForMembers(
            Seq(p14),
            sequencer,
            firstSequencerCounter = SequencerCounter.Genesis + 2,
          )
          reads15 <- readForMembers(Seq(p15), sequencer)
        } yield {
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p14,
                Some(request1.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads14,
          )
          checkRejection(reads14a, p14, messageId2, defaultExpectedTrafficReceipt) {
            case SequencerErrors.AggregateSubmissionStuffing(reason) =>
              reason should include(
                s"The sender $p14 previously contributed to the aggregatable submission with ID"
              )
          }
          val deliveredEnvelopeDetails = EnvelopeDetails(
            messageContent,
            recipients,
            // Only the first signature from p1 is included
            env1.signatures ++ env3.signatures,
          )

          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis + 2,
                p14,
                messageId = None,
                trafficReceipt = None,
                deliveredEnvelopeDetails,
              )
            ),
            reads14b,
          )
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p15,
                Some(messageId3),
                defaultExpectedTrafficReceipt,
                deliveredEnvelopeDetails,
              )
            ),
            reads15,
          )
        }
      }

      "require eligible senders be registered" onlyRunWhen testAggregation in { env =>
        import env.*

        // We expect synchronous rejections and can therefore reuse participant1.
        // But we need a fresh unregistered participant16
        // TODO(i10412): remove this comment
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p1, p16), PositiveInt.tryCreate(1), testedProtocolVersion)

        val request = createSendRequest(
          p1,
          "unregistered-eligible-sender",
          Recipients.cc(p1),
          aggregationRule = Some(aggregationRule),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          // Since the envelope does not contain a signature, we don't need to specify a topology timestamp
          topologyTimestamp = None,
        )

        for {
          error <- sequencer.sendAsyncSigned(sign(request)).leftOrFailShutdown("Sent async")
        } yield {
          error shouldBe a[SendAsyncError.SenderUnknown]
          error.message should (
            include("The following senders in the aggregation rule are unknown") and
              include(p16.toString)
          )
        }
      }

      "require the threshold to be reachable" onlyRunWhen testAggregation in { env =>
        import env.*

        // TODO(i10412): See above
        val faultyThreshold = PositiveInt.tryCreate(2)
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p17, p17), faultyThreshold, testedProtocolVersion)

        val messageId = MessageId.tryCreate("unreachable-threshold")
        val request = SubmissionRequest.tryCreate(
          p17,
          messageId,
          Batch.empty(testedProtocolVersion),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          topologyTimestamp = None,
          aggregationRule = Some(aggregationRule),
          Option.empty[SequencingSubmissionCost],
          testedProtocolVersion,
        )

        for {
          reads <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            for {
              _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFailShutdown("Sent async")
              reads <- readForMembers(Seq(p17), sequencer, timeout = 5.seconds)
            } yield reads,
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    SequencerErrors.SubmissionRequestMalformed,
                    _ shouldBe s"Send request [$messageId] is malformed. " +
                      s"Discarding request. Threshold $faultyThreshold cannot be reached",
                  ),
                  "p17's submission generates an alarm",
                )
              )
            ),
          )
        } yield {
          // p17 gets nothing
          checkMessages(Seq(), reads)
        }
      }

      "require the sender to be eligible" onlyRunWhen testAggregation in { env =>
        import env.*

        // TODO(i10412): See above
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p17), PositiveInt.tryCreate(1), testedProtocolVersion)

        val messageId = MessageId.tryCreate("first-sender-not-eligible")
        val request = SubmissionRequest.tryCreate(
          p18,
          messageId,
          Batch.empty(testedProtocolVersion),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          topologyTimestamp = None,
          aggregationRule = Some(aggregationRule),
          Option.empty[SequencingSubmissionCost],
          testedProtocolVersion,
        )

        for {
          reads <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            for {
              _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFailShutdown("Sent async")
              reads <- readForMembers(Seq(p18), sequencer, timeout = 5.seconds)
            } yield reads,
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    SequencerErrors.SubmissionRequestMalformed,
                    _ shouldBe s"Send request [$messageId] is malformed. " +
                      s"Discarding request. Sender [$p18] is not eligible according to the aggregation rule",
                  ),
                  "p18's submission generates an alarm",
                )
              )
            ),
          )
        } yield {
          // p18 gets nothing
          checkMessages(Seq(), reads)
        }
      }

      "prevent non-eligible senders from contributing" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageContent = "aggregatable-message"
        val aggregationRule =
          AggregationRule(NonEmpty(Seq, p1, p2), PositiveInt.tryCreate(2), testedProtocolVersion)

        val requestFromP1 = createSendRequest(
          sender = p1,
          messageContent,
          Recipients.cc(p3),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          aggregationRule = Some(aggregationRule),
          topologyTimestamp = Some(CantonTimestamp.Epoch),
        )

        // Request with non-eligible sender
        val messageId = MessageId.tryCreate("further-sender-not-eligible")
        val requestFromP4 = requestFromP1.copy(sender = p4, messageId = messageId)

        val requestFromP2 =
          requestFromP1.copy(sender = p2, messageId = MessageId.fromUuid(new UUID(1, 2)))

        for {
          _ <- sequencer
            .sendAsyncSigned(sign(requestFromP1))
            .valueOrFailShutdown("Sent async for participant1")

          readsForP1 <- readForMembers(Seq(p1), sequencer)

          readsForP4 <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            for {
              _ <- sequencer
                .sendAsyncSigned(sign(requestFromP4))
                .valueOrFailShutdown("Sent async for non-eligible participant4")
              reads <- readForMembers(Seq(p4), sequencer, timeout = 5.seconds)
            } yield reads,
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    SequencerErrors.SubmissionRequestMalformed,
                    _ shouldBe s"Send request [$messageId] is malformed. " +
                      s"Discarding request. Sender [$p4] is not eligible according to the aggregation rule",
                  ),
                  "p4's submission generates an alarm",
                )
              )
            ),
          )

          _ <- sequencer
            .sendAsyncSigned(sign(requestFromP2))
            .valueOrFailShutdown("Sent async for participant2")

          readsForP2 <- readForMembers(Seq(p2), sequencer)
          readsForP3 <- readForMembers(Seq(p3), sequencer)
        } yield {
          // p1 gets the receipt immediately
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p1,
                Some(requestFromP1.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            readsForP1,
          )

          // p2 gets the receipt only
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p2,
                Some(requestFromP2.messageId),
                defaultExpectedTrafficReceipt,
              )
            ),
            readsForP2,
          )

          // p3 gets the message
          checkMessages(
            Seq(
              EventDetails(
                SequencerCounter.Genesis,
                p3,
                None,
                None,
                EnvelopeDetails(messageContent, Recipients.cc(p3)),
              )
            ),
            readsForP3,
          )

          // p4 gets nothing
          checkMessages(Seq(), readsForP4)
        }
      }

      "require the member to be enabled to send/read" in { env =>
        import env.*

        val messageContent = "message-from-disabled-member"
        val sender = p7.member
        val recipients = Recipients.cc(sender)

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        for {
          // Need to send first request and wait for it to be processed to get the member registered in BS
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFailShutdown("Send async failed")
          _ <- readForMembers(Seq(p7), sequencer)
          _ <- sequencer.disableMember(sender).valueOrFail("Disabling member failed")
          sendError <- sequencer
            .sendAsyncSigned(sign(request))
            .leftOrFailShutdown("Send successful, expected error")
          subscribeError <- sequencer
            .read(sender, SequencerCounter.Genesis)
            .leftOrFail("Read successful, expected error")
        } yield {
          sendError shouldBe a[SendAsyncError.RequestRefused]
          sendError.message should (
            include("is disabled at the sequencer") and
              include(p7.toString)
          )
          subscribeError should matchPattern {
            case CreateSubscriptionError.MemberDisabled(member) if member == sender =>
          }
        }

      }
    }
  }
}

trait SequencerApiTestUtils
    extends FixtureAsyncWordSpec
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {
  protected def readForMembers(
      members: Seq[Member],
      sequencer: CantonSequencer,
      // up to 60 seconds needed because Besu is very slow on CI
      timeout: FiniteDuration = 60.seconds,
      firstSequencerCounter: SequencerCounter = SequencerCounter.Genesis,
  )(implicit materializer: Materializer): Future[Seq[(Member, OrdinarySerializedEvent)]] =
    members
      .parTraverseFilter { member =>
        for {
          source <- valueOrFail(sequencer.read(member, firstSequencerCounter))(
            s"Read for $member"
          )
          events <- source
            // hard-coding that we only expect 1 event per member
            .take(1)
            .takeWithin(timeout)
            .runWith(Sink.seq)
            .map {
              case Seq(Right(e)) => Some((member, e))
              case Seq(Left(err)) => fail(s"Test does not expect tombstones: $err")
              case _ =>
                // We read no messages for a member when we expected some
                None
            }
        } yield events
      }

  case class EnvelopeDetails(
      content: String,
      recipients: Recipients,
      signatures: Seq[Signature] = Seq.empty,
  )

  case class EventDetails(
      counter: SequencerCounter,
      to: Member,
      messageId: Option[MessageId],
      trafficReceipt: Option[TrafficReceipt],
      envs: EnvelopeDetails*
  )

  protected def createSendRequest(
      sender: Member,
      messageContent: String,
      recipients: Recipients,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
      aggregationRule: Option[AggregationRule] = None,
      topologyTimestamp: Option[CantonTimestamp] = None,
      sequencingSubmissionCost: Batch[ClosedEnvelope] => Option[SequencingSubmissionCost] = _ =>
        None,
  ): SubmissionRequest = {
    val envelope1 = TestingEnvelope(messageContent, recipients)
    val batch = Batch(List(envelope1.closeEnvelope), testedProtocolVersion)
    val messageId = MessageId.tryCreate(s"thisisamessage: $messageContent")
    SubmissionRequest.tryCreate(
      sender,
      messageId,
      batch,
      maxSequencingTime,
      topologyTimestamp,
      aggregationRule,
      sequencingSubmissionCost(batch),
      testedProtocolVersion,
    )
  }

  protected def checkMessages(
      expectedMessages: Seq[EventDetails],
      receivedMessages: Seq[(Member, OrdinarySerializedEvent)],
  ): Assertion = {

    receivedMessages.length shouldBe expectedMessages.length

    val sortExpected = expectedMessages.sortBy(e => e.to)
    val sortReceived = receivedMessages.sortBy { case (member, _) => member }

    forAll(sortReceived.zip(sortExpected)) { case ((member, message), expectedMessage) =>
      withClue(s"Member mismatch")(member shouldBe expectedMessage.to)

      withClue(s"Sequencer counter is wrong") {
        message.counter shouldBe expectedMessage.counter
      }

      val event = message.signedEvent.content

      event match {
        case Deliver(_, _, _, messageIdO, batch, _, trafficReceipt) =>
          withClue(s"Received the wrong number of envelopes for recipient $member") {
            batch.envelopes.length shouldBe expectedMessage.envs.length
          }

          if (messageIdO.isDefined) {
            withClue(s"Received incorrect traffic receipt $member") {
              trafficReceipt shouldBe expectedMessage.trafficReceipt
            }
          } else {
            withClue(s"Received a traffic receipt for $member in an event without messageId") {
              trafficReceipt shouldBe empty
            }
          }

          forAll(batch.envelopes.zip(expectedMessage.envs)) { case (got, wanted) =>
            got.recipients shouldBe wanted.recipients
            got.bytes shouldBe ByteString.copyFromUtf8(wanted.content)
            got.signatures shouldBe wanted.signatures
          }

        case _ => fail(s"Event $event is not a deliver")
      }
    }
  }

  def checkRejection(
      got: Seq[(Member, OrdinarySerializedEvent)],
      sender: Member,
      expectedMessageId: MessageId,
      expectedTrafficReceipt: Option[TrafficReceipt],
  )(assertReason: PartialFunction[Status, Assertion]): Assertion =
    got match {
      case Seq((`sender`, event)) =>
        event.signedEvent.content match {
          case DeliverError(
                _counter,
                _timestamp,
                _domainId,
                messageId,
                reason,
                trafficReceipt,
              ) =>
            messageId shouldBe expectedMessageId
            assertReason(reason)
            trafficReceipt shouldBe expectedTrafficReceipt

          case _ => fail(s"Expected a deliver error, but got $event")
        }
      case _ => fail(s"Read wrong events for $sender: $got")
    }

  def signEnvelope(
      crypto: DomainSyncCryptoClient,
      envelope: ClosedEnvelope,
  ): Future[ClosedEnvelope] = {
    val hash = crypto.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, envelope.bytes)
    crypto.currentSnapshotApproximation
      .sign(hash)
      .valueOrFailShutdown(s"Failed to sign $envelope")
      .map(sig => envelope.copy(signatures = Seq(sig)))
  }

  case class TestingEnvelope(content: String, override val recipients: Recipients)
      extends Envelope[String] {

    /** Closes the envelope by serializing the contents */
    def closeEnvelope: ClosedEnvelope =
      ClosedEnvelope.create(
        ByteString.copyFromUtf8(content),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )

    override def forRecipient(
        member: Member,
        groupAddresses: Set[GroupRecipient],
    ): Option[Envelope[String]] =
      recipients
        .forMember(member, groupAddresses)
        .map(recipients => TestingEnvelope(content, recipients))

    override protected def pretty: Pretty[TestingEnvelope] = adHocPrettyInstance
  }

  /** Registers all the members present in the topology snapshot with the sequencer.
    * Used for unit testing sequencers. During the normal sequencer operation members are registered
    * via topology subscription or sequencer startup in SequencerRuntime.
    */
  def registerAllTopologyMembers(headSnapshot: TopologySnapshot, sequencer: Sequencer): Unit =
    (for {
      allMembers <- EitherT.right[Sequencer.RegisterError](headSnapshot.allMembers())
      _ <- allMembers.toSeq
        .parTraverse_ { member =>
          for {
            firstKnownAtO <- EitherT.right(headSnapshot.memberFirstKnownAt(member))
            res <- firstKnownAtO match {
              case Some((_, firstKnownAtEffectiveTime)) =>
                sequencer.registerMemberInternal(member, firstKnownAtEffectiveTime.value)
              case None =>
                ErrorUtil.invalidState(
                  s"Member $member has no first known at time, despite being in the topology"
                )
            }
          } yield res
        }
    } yield ()).futureValue
}
