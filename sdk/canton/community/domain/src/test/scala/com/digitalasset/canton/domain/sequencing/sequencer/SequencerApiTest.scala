// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.ExceededMaxSequencingTime
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLogging, SuppressionRule}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PekkoUtil
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class SequencerApiTest
    extends SequencerApiTestUtils
    with ProtocolVersionChecksFixtureAsyncWordSpec {

  import RecipientsTest.*

  trait Env extends AutoCloseable with NamedLogging {

    private[SequencerApiTest] implicit lazy val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    private[SequencerApiTest] lazy val sequencer: CantonSequencer =
      SequencerApiTest.this.createSequencer(
        topologyFactory.forOwnerAndDomain(owner = mediatorId, domainId)
      )

    def topologyFactory: TestingIdentityFactoryBase

    def close(): Unit = {
      sequencer.close()
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    }
  }

  override protected type FixtureParam <: Env

  protected def createEnv(): FixtureParam

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = createEnv()
    complete {
      super.withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  var clock: Clock = _
  var driverClock: Clock = _

  def createClock(): Clock = new SimClock(loggerFactory = loggerFactory)

  def domainId: DomainId = DefaultTestIdentities.domainId
  def mediatorId: MediatorId = DefaultTestIdentities.mediator
  def topologyClientMember: Member = DefaultTestIdentities.sequencerId

  def createSequencer(crypto: DomainSyncCryptoClient)(implicit
      materializer: Materializer
  ): CantonSequencer

  protected def supportAggregation: Boolean

  protected def runSequencerApiTests(): Unit = {
    "The sequencers" should {
      "send a batch to one recipient" in { env =>
        import env.*
        val messageContent = "hello"
        val sender: MediatorId = mediatorId
        val recipients = Recipients.cc(sender)

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        for {
          _ <- valueOrFail(sequencer.registerMember(topologyClientMember))(
            "Register topology client"
          )
          _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
          _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
          messages <- readForMembers(List(sender), sequencer)
        } yield {
          val details = EventDetails(
            SequencerCounter(0),
            sender,
            Some(request.messageId),
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
          _ <- valueOrFail(sequencer.registerMember(topologyClientMember))(
            "Register topology client"
          )
          _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
          messages <- loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
            valueOrFail(sequencer.sendAsync(request))("Sent async")
              .flatMap(_ =>
                readForMembers(
                  List(sender),
                  sequencer,
                  timeout = 5.seconds, // We don't need the full timeout here
                )
              ),
            forAll(_) { entry =>
              entry.message should include(
                suppressedMessageContent
              ) // block update generator will log every send
              entry.message should (include(ExceededMaxSequencingTime.id) or include(
                "Observed Send"
              ))
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
          _ <- valueOrFail(sequencer.registerMember(topologyClientMember))(
            "Register topology client"
          )
          _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
          _ <- valueOrFail(sequencer.sendAsync(request1))("Sent async #1")
          messages <- loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
            valueOrFail(sequencer.sendAsync(request2))("Sent async #2")
              .flatMap(_ => readForMembers(List(sender), sequencer)),
            forAll(_) { entry =>
              // block update generator will log every send
              entry.message should ((include(ExceededMaxSequencingTime.id) or include(
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
        val readFor: List[Member] = recipients.allRecipients.map(_.member).toList

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        val expectedDetailsForMembers = readFor.map { member =>
          EventDetails(
            SequencerCounter.Genesis,
            member,
            Option.when(member == sender)(request.messageId),
            EnvelopeDetails(messageContent, recipients.forMember(member).value),
          )
        }

        for {
          _ <- registerMembers(
            recipients.allRecipients.map(_.member) + sender + topologyClientMember,
            sequencer,
          )
          _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
          reads <- readForMembers(readFor, sequencer)
        } yield {
          checkMessages(expectedDetailsForMembers, reads)
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
  )(implicit materializer: Materializer): Future[Seq[(Member, OrdinarySerializedEvent)]] = {
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
      envs: EnvelopeDetails*
  )

  protected def registerMembers(members: Set[Member], sequencer: CantonSequencer): Future[Unit] =
    members.toList.parTraverse_ { member =>
      val registerE = sequencer.registerMember(member)
      valueOrFail(registerE)(s"Register member $member")
    }

  protected def createSendRequest(
      sender: Member,
      messageContent: String,
      recipients: Recipients,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  ): SubmissionRequest = {
    val envelope1 = TestingEnvelope(messageContent, recipients)
    val batch = Batch(List(envelope1.closeEnvelope), testedProtocolVersion)
    val messageId = MessageId.tryCreate(s"thisisamessage: $messageContent")
    SubmissionRequest.tryCreate(
      sender,
      messageId,
      isRequest = false,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
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
      withClue(s"Member mismatch") { member shouldBe expectedMessage.to }

      withClue(s"Sequencer counter is wrong") {
        message.counter shouldBe expectedMessage.counter
      }

      val event = message.signedEvent.content

      event match {
        case Deliver(_, _, _, _, batch) =>
          withClue(s"Received the wrong number of envelopes for recipient $member") {
            batch.envelopes.length shouldBe expectedMessage.envs.length
          }

          forAll(batch.envelopes.zip(expectedMessage.envs)) { case (got, wanted) =>
            got.recipients shouldBe wanted.recipients
            got.bytes shouldBe ByteString.copyFromUtf8(wanted.content)
          }

        case _ => fail(s"Event $event is not a deliver")
      }
    }
  }

  def checkRejection(
      got: Seq[(Member, OrdinarySerializedEvent)],
      sender: Member,
      expectedMessageId: MessageId,
  )(assertReason: PartialFunction[Status, Assertion]): Assertion = {
    got match {
      case Seq((`sender`, event)) =>
        event.signedEvent.content match {
          case DeliverError(_counter, _timestamp, _domainId, messageId, reason) =>
            messageId shouldBe expectedMessageId
            assertReason(reason)

          case _ => fail(s"Expected a deliver error, but got $event")
        }
      case _ => fail(s"Read wrong events for $sender: $got")
    }
  }

  case class TestingEnvelope(content: String, override val recipients: Recipients)
      extends Envelope[String] {

    /** Closes the envelope by serializing the contents */
    def closeEnvelope: ClosedEnvelope =
      ClosedEnvelope(
        ByteString.copyFromUtf8(content),
        recipients,
        testedProtocolVersion,
      )

    override def forRecipient(member: Member): Option[Envelope[String]] = {
      recipients
        .forMember(member)
        .map(recipients => TestingEnvelope(content, recipients))
    }

    override def pretty: Pretty[TestingEnvelope] = adHocPrettyInstance
  }
}
