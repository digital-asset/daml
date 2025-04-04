// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Objective: Test sequencer channels with variously behaving test sequencer protocol processors.
  *
  * Setup:
  *   - 2 participants connecting to each other via sequencer channels
  *   - 1 sequencer
  */
@nowarn("msg=match may not be exhaustive")
sealed trait SequencerChannelProtocolIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SequencerChannelProtocolTestExecHelpers {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(ConfigTransforms.unsafeEnableOnlinePartyReplication*)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            synchronizerAlias = daName,
            synchronizerOwners = Seq[InstanceReference](mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        participants.local.foreach(_.start())
        participants.local.synchronizers.connect_local(sequencer1, daName)
      }

  // OnlinePartyReplication messages require dev protocol version for added safety
  // to ensure that they aren't accidentally used in production.
  "Participants able to perform empty conversation via sequencer channel" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val Seq(client1, client2) = Seq(participant1, participant2).map(
        _.testing.state_inspection
          .getSequencerChannelClient(daId)
          .getOrElse(fail("Cannot find client"))
      )

      clue("Both sequencer channel clients can ping the sequencer") {
        asyncExec("ping1")(client1.ping(sequencer1.id))
        asyncExec("ping2")(client2.ping(sequencer1.id))
      }

      val channelId = SequencerChannelId("empty channel")
      val testProcessor1 = new TestProcessor()
      val testProcessor2 = new TestProcessor()

      val recorder1 =
        new TestRecorder(participant1, daId, staticSynchronizerParameters1)
      val recorder2 =
        new TestRecorder(participant2, daId, staticSynchronizerParameters1)

      clue("Channel connected between P1 and P2") {
        exec("connectToSequencerChannel1")(
          client1.connectToSequencerChannel(
            sequencer1.id,
            channelId,
            participant2.id,
            testProcessor1,
            isSessionKeyOwner = true,
            recorder1.timestamp,
            Some(recorder1.recordSentMessage),
          )
        )

        exec("connectToSequencerChannel2")(
          client2.connectToSequencerChannel(
            sequencer1.id,
            channelId,
            participant1.id,
            testProcessor2,
            isSessionKeyOwner = false,
            recorder2.timestamp,
            Some(recorder2.recordSentMessage),
          )
        )

        eventually() {
          testProcessor1.hasChannelConnected shouldBe true
          testProcessor2.hasChannelConnected shouldBe true
        }
      }

      clue("Assert asymmetrically encrypted session key sent from P1 to P2") {
        val sessionKeyMessages = recorder1.fetchSessionKeyMessages
        sessionKeyMessages should have size 1
        sessionKeyMessages.head should matchPattern {
          case SequencerChannelSessionKey(enc) if enc.ciphertext.size() > 0 =>
        }
      }

      clue("Assert session key acknowledgement sent from P2 to P1") {
        val sessionKeyAcks = recorder2.fetchSessionKeyAcks
        sessionKeyAcks should have size 1
        sessionKeyAcks.head should matchPattern { case SequencerChannelSessionKeyAck() =>
        }
      }

      clue("Channel completed on P1 and P2") {
        asyncExec("complete channel")(testProcessor1.sendTestCompleted("complete channel"))

        eventually() {
          testProcessor1.hasChannelCompleted shouldBe true
          testProcessor2.hasChannelCompleted shouldBe true
        }
      }
  }

  "Participants receive each other's messages via sequencer channel" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val Seq(client1, client2) = Seq(participant1, participant2).map(
        _.testing.state_inspection
          .getSequencerChannelClient(daId)
          .getOrElse(fail("Cannot find client"))
      )

      val messagesReceivedP1 = mutable.Buffer[String]()
      val responsesReceivedP2 = mutable.Buffer[String]()

      val channelId = SequencerChannelId("exchange messages channel")
      val testProcessor1 = new TestProcessor {
        override def handlePayload(payload: ByteString)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] = {
          val str = payload.toString("UTF-8")
          messagesReceivedP1 += str
          val response = ByteString.copyFromUtf8(s"Response to \"$str\"")
          sendTestPayload(s"responding to \"$str\"", response)
        }
      }
      val testProcessor2 = new TestProcessor {
        override def handlePayload(payload: ByteString)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] = {
          val str = payload.toString("UTF-8")
          responsesReceivedP2 += str
          EitherTUtil.unitUS
        }
      }

      val stringMessageFromP2ToP1 = 1 to 3 map (i => s"message $i")
      val messagesFromP2ToP1 = stringMessageFromP2ToP1 map ByteString.copyFromUtf8

      val recorder1 = new TestRecorder(participant1, daId, staticSynchronizerParameters1)
      val recorder2 = new TestRecorder(participant2, daId, staticSynchronizerParameters1)

      clue("Channel connected between P1 and P2") {
        exec("connectToSequencerChannel1")(
          client1.connectToSequencerChannel(
            sequencer1.id,
            channelId,
            participant2.id,
            testProcessor1,
            isSessionKeyOwner = true,
            recorder1.timestamp,
            Some(recorder1.recordSentMessage),
          )
        )

        exec("connectToSequencerChannel2")(
          client2.connectToSequencerChannel(
            sequencer1.id,
            channelId,
            participant1.id,
            testProcessor2,
            isSessionKeyOwner = false,
            recorder2.timestamp,
            Some(recorder2.recordSentMessage),
          )
        )

        eventually() {
          testProcessor1.hasChannelConnected shouldBe true
          testProcessor2.hasChannelConnected shouldBe true
        }
      }

      clue("Payload messages sent from P2 to P1") {
        messagesFromP2ToP1.foreach { message =>
          asyncExec("send payload")(testProcessor2.sendTestPayload("send payload", message))
        }
      }

      clue("Assert encrypted payload messages") {
        val sessionKeyMessage = recorder1.fetchSessionKeyMessages.head
        val eventualSessionKey = recorder2.extractSessionKey(sessionKeyMessage)
        val sessionKey = eventualSessionKey.futureValueUS.value

        val payloadStringMessages = recorder2.convertPayloadMessagesToString(sessionKey)
        payloadStringMessages should have size 3
        payloadStringMessages shouldEqual stringMessageFromP2ToP1
      }

      clue("Received all messages on P1")(
        eventually() {
          messagesReceivedP1.toSeq shouldBe stringMessageFromP2ToP1
          responsesReceivedP2.toSeq shouldBe stringMessageFromP2ToP1.map(str =>
            s"Response to \"$str\""
          )
        }
      )

      clue("Channel completed on P1 and P2") {
        asyncExec("complete channel")(testProcessor1.sendTestCompleted("complete channel"))

        eventually() {
          testProcessor1.hasChannelCompleted shouldBe true
          testProcessor2.hasChannelCompleted shouldBe true
        }
      }
  }

  "Participant can cancel channel when connection takes too long" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val channelClient = participant1.testing.state_inspection
        .getSequencerChannelClient(daId)
        .getOrElse(fail("Cannot find client"))

      val channelId = SequencerChannelId("canceled channel")
      val testProcessor1 = new TestProcessor()
      val recorder1 = new TestRecorder(participant1, daId, staticSynchronizerParameters1)

      channelClient.connectToSequencerChannel(
        sequencer1.id,
        channelId,
        participant2.id,
        testProcessor1,
        isSessionKeyOwner = true,
        recorder1.timestamp,
      )

      val serverSideCancelMessage = "CANCELLED: client cancelled"
      loggerFactory.assertLogsUnorderedOptional(
        asyncExec("Cancel channel")(
          testProcessor1.sendTestError("Channel took too long to connect")
        ),
        (
          LogEntryOptionality.Required,
          entry => {
            entry.loggerName should include("SequencerChannelClientEndpoint")
            entry.warningMessage should include("Channel took too long to connect")
          },
        ),
        // Since the test does not wait for the sequencer channel to be initialized on the sequencer service,
        // the cancellation by the client might flakily arrive at the sequencer server before the channel is initialized.
        (
          LogEntryOptionality.Optional,
          entry => {
            entry.loggerName should include("GrpcSequencerChannelPool")
            entry.warningMessage should include(
              s"Request stream error $serverSideCancelMessage before initialization."
            )
          },
        ),
        // But normally the channel message handler warns that the client cancelled the request.
        (
          LogEntryOptionality.Optional,
          entry => {
            entry.loggerName should include("GrpcSequencerChannelMemberMessageHandler")
            entry.warningMessage should include(
              s"Member message handler received error $serverSideCancelMessage."
            )
          },
        ),
      )
  }

  "Participant cannot reuse channel processor for multiple connections" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val channelClient = participant1.testing.state_inspection
        .getSequencerChannelClient(daId)
        .getOrElse(fail("Cannot find client"))

      val testProcessor = new TestProcessor()
      val timestamp =
        (new TestRecorder(participant1, daId, staticSynchronizerParameters1)).timestamp

      exec("connect processor the first time")(
        channelClient.connectToSequencerChannel(
          sequencer1.id,
          SequencerChannelId("reuse processor channel1"),
          participant2.id,
          testProcessor,
          isSessionKeyOwner = true,
          timestamp,
        )
      )

      asyncExec("Complete channel")(
        testProcessor.sendTestCompleted("Close channel before fully connected")
      )
      clue("Channel completed on P1 and P2") {
        eventually() {
          testProcessor.hasChannelCompleted shouldBe true
        }
      }

      val error = execWithError("connect processor the second time")(
        channelClient.connectToSequencerChannel(
          sequencer1.id,
          SequencerChannelId("reuse processor channel2"),
          participant2.id,
          testProcessor,
          isSessionKeyOwner = true,
          timestamp,
        )
      )
      error shouldBe "Channel protocol processor previously connected to a different channel endpoint"
  }

  private class TestProcessor(
      initialPayloads: Seq[ByteString] = Seq.empty,
      protected val protocolVersion: ProtocolVersion = testedProtocolVersion,
  )(implicit val executionContext: ExecutionContext)
      extends SequencerChannelProtocolProcessor {
    override def onConnected()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      MonadUtil.sequentialTraverse_(initialPayloads)(sendPayload("initial payload", _))

    override def handlePayload(payload: ByteString)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      logger.info(s"Received payload of size ${payload.size}")
      EitherTUtil.unitUS
    }

    def sendTestPayload(operation: String, payload: ByteString)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = sendPayload(operation, payload)

    def sendTestCompleted(status: String)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = sendCompleted(status)

    def sendTestError(error: String)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = sendError(error)

    override def onDisconnected(status: Either[String, Unit])(implicit
        traceContext: TraceContext
    ): Unit =
      status.swap.foreach(err => logger.warn(s"TestProcessor disconnected with error: $err"))

    override protected def timeouts: ProcessingTimeout =
      SequencerChannelProtocolIntegrationTest.this.timeouts

    override protected def loggerFactory: NamedLoggerFactory =
      SequencerChannelProtocolIntegrationTest.this.loggerFactory
  }
}

trait SequencerChannelProtocolTestExecHelpers {
  this: BaseTest =>

  protected def asyncExec[A](
      operation: String
  )(code: => EitherT[FutureUnlessShutdown, String, A]): A =
    clue(operation)(code.futureValueUS.value)

  protected def exec[A](operation: String)(code: => EitherT[UnlessShutdown, String, A]): A =
    code.value
      .onShutdown(fail(s"Shutdown during $operation"))
      .valueOr(err => fail(s"Error during $operation: $err"))

  protected def execWithError[A](operation: String)(
      code: => EitherT[UnlessShutdown, String, A]
  ): String =
    code.value
      .onShutdown(fail(s"Shutdown during $operation"))
      .swap
      .getOrElse(fail(s"Operation $operation expected to fail, but succeeded"))
}

class SequencerChannelProtocolIntegrationTestPostgres
    extends SequencerChannelProtocolIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
