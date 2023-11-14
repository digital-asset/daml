// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.syntax.parallel.*
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.protocol.messages.{
  EnvelopeContent,
  ProtocolMessage,
  ProtocolMessageV0,
  ProtocolMessageV1,
  ProtocolMessageV2,
  ProtocolMessageV3,
  UnsignedProtocolMessageV4,
}
import com.digitalasset.canton.protocol.{v0, v1, v2, v3, v4}
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import com.typesafe.config.ConfigFactory
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.*

class SequencerTest extends FixtureAsyncWordSpec with BaseTest with HasExecutionContext {

  private val domainId = DefaultTestIdentities.domainId
  private val alice: Member = ParticipantId("alice")
  private val bob: Member = ParticipantId("bob")
  private val carole: Member = ParticipantId("carole")
  private val topologyClientMember = SequencerId(domainId)

  // Config to turn on Akka logging
  private lazy val akkaConfig = {
    import scala.jdk.CollectionConverters.*
    ConfigFactory.parseMap(
      Map[String, Object](
        "akka.loglevel" -> "DEBUG",
        "akka.stdout-level" -> "OFF",
        "akka.loggers" -> List("akka.event.slf4j.Slf4jLogger").asJava,
      ).asJava
    )
  }

  class Env extends FlagCloseableAsync {
    override val timeouts = SequencerTest.this.timeouts
    protected val logger = SequencerTest.this.logger
    private implicit val actorSystem: ActorSystem = ActorSystem(
      classOf[SequencerTest].getSimpleName,
      Some(akkaConfig),
      None,
      Some(parallelExecutionContext),
    )
    private val materializer = implicitly[Materializer]
    val store = new InMemorySequencerStore(loggerFactory)
    val clock = new WallClock(timeouts, loggerFactory = loggerFactory)
    val crypto = valueOrFail(
      TestingTopology()
        .build(loggerFactory)
        .forOwner(SequencerId(domainId))
        .forDomain(domainId)
        .toRight("crypto error")
    )("building crypto")
    val metrics = SequencerMetrics.noop("sequencer-test")

    val sequencer =
      DatabaseSequencer.single(
        CommunitySequencerConfig.Database(),
        DefaultProcessingTimeouts.testing,
        new MemoryStorage(loggerFactory, timeouts),
        clock,
        domainId,
        topologyClientMember,
        testedProtocolVersion,
        crypto,
        metrics,
        loggerFactory,
      )(parallelExecutionContext, tracer, materializer)

    def readAsSeq(
        member: Member,
        limit: Int,
        sc: SequencerCounter = SequencerCounter(0),
    ): Future[Seq[OrdinarySerializedEvent]] =
      valueOrFail(sequencer.readInternal(member, sc))(
        s"read for $member"
      ) flatMap {
        _.take(limit.toLong)
          .map {
            case Right(event) => event
            case Left(err) => fail(s"The DatabaseSequencer does not emit tombstone-errors: $err")
          }
          .idleTimeout(30.seconds)
          .runWith(Sink.seq)
      }

    def asDeliverEvent(event: SequencedEvent[ClosedEnvelope]): Deliver[ClosedEnvelope] =
      event match {
        case deliver: Deliver[ClosedEnvelope] => deliver
        case other => fail(s"Expected deliver event but got $other")
      }

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("sequencer", sequencer.close()),
      AsyncCloseable("actorSystem", actorSystem.terminate(), 10.seconds),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  class TestProtocolMessage(text: String)
      extends ProtocolMessage
      with ProtocolMessageV0
      with ProtocolMessageV1
      with ProtocolMessageV2
      with ProtocolMessageV3
      with UnsignedProtocolMessageV4 {
    private val payload =
      v0.SignedProtocolMessage(
        None,
        v0.SignedProtocolMessage.SomeSignedProtocolMessage.Empty,
      )
    override def domainId: DomainId = ???

    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      ???

    override protected val companionObj: AnyRef = TestProtocolMessage

    override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
      v0.EnvelopeContent(
        v0.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload)
      )

    override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
      v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload))

    override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
      v2.EnvelopeContent(v2.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload))

    override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
      v3.EnvelopeContent(v3.EnvelopeContent.SomeEnvelopeContent.SignedMessage(payload))

    override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
      v4.EnvelopeContent.SomeEnvelopeContent.Empty

    override def productElement(n: Int): Any = ???
    override def productArity: Int = ???
    override def canEqual(that: Any): Boolean = ???
  }
  object TestProtocolMessage

  "send" should {
    "correctly deliver event to each recipient" in { env =>
      import env.*

      val messageId = MessageId.tryCreate("test-message")
      val message1 = new TestProtocolMessage("message1")
      val message2 = new TestProtocolMessage("message2")

      val submission = SubmissionRequest.tryCreate(
        alice,
        messageId,
        true,
        Batch.closeEnvelopes(
          Batch.of(
            testedProtocolVersion,
            (message1, Recipients.cc(bob)),
            (message2, Recipients.cc(carole)),
          )
        ),
        clock.now.plusSeconds(10),
        None,
        None,
        testedProtocolVersion,
      )

      for {
        _ <- valueOrFail(
          List(alice, bob, carole, topologyClientMember).parTraverse(sequencer.registerMember)
        )(
          "member registration"
        )
        _ <- valueOrFail(sequencer.sendAsync(submission))("send")
        aliceDeliverEvent <- readAsSeq(alice, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        bobDeliverEvent <- readAsSeq(bob, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        caroleDeliverEvent <- readAsSeq(carole, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
      } yield {
        aliceDeliverEvent.messageIdO.value shouldBe messageId // as alice is the sender
        aliceDeliverEvent.batch.envelopes should have size (0) // as we didn't send a message to ourself

        bobDeliverEvent.messageIdO shouldBe None
        bobDeliverEvent.batch.envelopes.map(_.bytes) should contain only
          EnvelopeContent.tryCreate(message1, testedProtocolVersion).toByteString

        caroleDeliverEvent.messageIdO shouldBe None
        caroleDeliverEvent.batch.envelopes.map(_.bytes) should contain only
          EnvelopeContent.tryCreate(message2, testedProtocolVersion).toByteString
      }
    }
  }
}
