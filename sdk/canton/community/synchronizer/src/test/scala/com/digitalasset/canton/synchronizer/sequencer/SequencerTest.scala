// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.{
  EnvelopeContent,
  ProtocolMessage,
  UnsignedProtocolMessage,
}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.client.RequestSigner
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.{InMemorySequencerStore, SequencerStore}
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, config}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.duration.*

class SequencerTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val carole = ParticipantId("carole")
  private val topologyClientMember = SequencerId(synchronizerId.uid)

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
      sequencerMember = topologyClientMember,
      blockSequencerMode = true,
      loggerFactory = loggerFactory,
    )
    val clock = new WallClock(timeouts, loggerFactory = loggerFactory)
    private val testingTopology = TestingTopology(
      sequencerGroup = SequencerGroup(
        active = Seq(SequencerId(synchronizerId.uid)),
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

    val crypto: SynchronizerCryptoClient = valueOrFail(
      testingTopology
        .forOwner(SequencerId(synchronizerId.uid))
        .forSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
        .toRight("crypto error")
    )("building crypto")
    val aliceCrypto: SynchronizerCryptoClient = valueOrFail(
      testingTopology
        .forOwner(alice)
        .forSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
        .toRight("crypto error")
    )("building alice crypto")

    val metrics: SequencerMetrics = SequencerMetrics.noop("sequencer-test")

    val dbConfig = SequencerConfig.Database()
    val storage = new MemoryStorage(loggerFactory, timeouts)
    val sequencerStore = SequencerStore(
      storage,
      testedProtocolVersion,
      bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize = SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      sequencerMember = topologyClientMember,
      blockSequencerMode = false,
      cachingConfigs = CachingConfigs(),
    )

    val sequencer: DatabaseSequencer =
      DatabaseSequencer.single(
        dbConfig,
        None,
        DefaultProcessingTimeouts.testing,
        storage,
        sequencerStore,
        clock,
        synchronizerId,
        topologyClientMember,
        testedProtocolVersion,
        crypto,
        metrics,
        loggerFactory,
      )(parallelExecutionContext, tracer, materializer)

    def readAsSeq(
        member: Member,
        limit: Int,
        startingTimestamp: Option[CantonTimestamp] = None,
    ): FutureUnlessShutdown[Seq[SequencedSerializedEvent]] =
      FutureUnlessShutdown.outcomeF(
        valueOrFail(sequencer.readInternalV2(member, startingTimestamp).failOnShutdown)(
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
      )

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

  class TestProtocolMessage() extends ProtocolMessage with UnsignedProtocolMessage {
    override def synchronizerId: SynchronizerId = fail("shouldn't be used")

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
      val message1 = new TestProtocolMessage()
      val message2 = new TestProtocolMessage()

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
        signedSubmission <- RequestSigner(aliceCrypto, testedProtocolVersion, loggerFactory)
          .signRequest(submission, HashPurpose.SubmissionRequestSignature)
          .valueOrFail("sign request")
        _ <- sequencer.sendAsyncSigned(signedSubmission).valueOrFail("send")
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
