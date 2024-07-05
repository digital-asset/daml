// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.{
  EnvelopeContent,
  ProtocolMessage,
  UnsignedProtocolMessage,
}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter, config}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.*

class SequencerTest extends FixtureAsyncWordSpec with BaseTest with HasExecutionContext {

  private val domainId = DefaultTestIdentities.domainId
  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val carole = ParticipantId("carole")
  private val topologyClientMember = SequencerId(domainId.uid)

  // Config to turn on Pekko logging
  private lazy val pekkoConfig = {
    import scala.jdk.CollectionConverters.*
    ConfigFactory.parseMap(
      Map[String, Object](
        "pekko.loglevel" -> "DEBUG",
        "pekko.stdout-level" -> "OFF",
        "pekko.loggers" -> List("org.apache.pekko.event.slf4j.Slf4jLogger").asJava,
      ).asJava
    )
  }

  class Env extends FlagCloseableAsync {
    override val timeouts: ProcessingTimeout = SequencerTest.this.timeouts
    protected val logger: TracedLogger = SequencerTest.this.logger
    private implicit val actorSystem: ActorSystem = ActorSystem(
      classOf[SequencerTest].getSimpleName,
      Some(pekkoConfig),
      None,
      Some(parallelExecutionContext),
    )
    private val materializer = implicitly[Materializer]
    val store = new InMemorySequencerStore(
      protocolVersion = testedProtocolVersion,
      unifiedSequencer = testedUseUnifiedSequencer,
      loggerFactory = loggerFactory,
    )
    val clock = new WallClock(timeouts, loggerFactory = loggerFactory)
    val crypto: DomainSyncCryptoClient = valueOrFail(
      TestingTopology(
        sequencerGroup = SequencerGroup(
          active = NonEmpty.mk(Seq, SequencerId(domainId.uid)),
          passive = Seq.empty,
          threshold = PositiveInt.one,
        ),
        participants = Seq(
          alice,
          bob,
          carole,
        ).map((_, ParticipantAttributes(ParticipantPermission.Confirmation))).toMap,
      )
        .build(loggerFactory)
        .forOwner(SequencerId(domainId.uid))
        .forDomain(domainId, defaultStaticDomainParameters)
        .toRight("crypto error")
    )("building crypto")
    val metrics: SequencerMetrics = SequencerMetrics.noop("sequencer-test")

    val sequencer: DatabaseSequencer =
      DatabaseSequencer.single(
        CommunitySequencerConfig.Database(),
        None,
        DefaultProcessingTimeouts.testing,
        new MemoryStorage(loggerFactory, timeouts),
        clock,
        domainId,
        topologyClientMember,
        testedProtocolVersion,
        crypto,
        metrics,
        loggerFactory,
        unifiedSequencer = testedUseUnifiedSequencer,
        runtimeReady = FutureUnlessShutdown.unit,
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
      AsyncCloseable(
        "actorSystem",
        actorSystem.terminate(),
        config.NonNegativeFiniteDuration(10.seconds),
      ),
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

  class TestProtocolMessage(_text: String) extends ProtocolMessage with UnsignedProtocolMessage {
    override def domainId: DomainId = fail("shouldn't be used")

    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      fail("shouldn't be used")

    override protected val companionObj: AnyRef = TestProtocolMessage

    override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
      v30.EnvelopeContent.SomeEnvelopeContent.Empty

    override def productElement(n: Int): Any = fail("shouldn't be used")
    override def productArity: Int = fail("shouldn't be used")
    override def canEqual(that: Any): Boolean = fail("shouldn't be used")
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
        None,
        testedProtocolVersion,
      )

      for {
        _ <- valueOrFail(
          List(alice, bob, carole, topologyClientMember).parTraverse(
            TestDatabaseSequencerWrapper(sequencer)
              .registerMemberInternal(_, CantonTimestamp.Epoch)
          )
        )(
          "member registration"
        )
        _ <- sequencer.sendAsync(submission).valueOrFailShutdown("send")
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
        aliceDeliverEvent.batch.envelopes shouldBe empty // as we didn't send a message to ourself

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
