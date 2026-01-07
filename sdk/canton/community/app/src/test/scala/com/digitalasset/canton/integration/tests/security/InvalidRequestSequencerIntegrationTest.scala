// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Availability
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SequencerTestHelper,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.google.protobuf.ByteString
import io.grpc.{ConnectivityState, ManagedChannel, StatusRuntimeException}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

trait InvalidRequestSequencerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private var daChannel: ManagedChannel = _
  private var sequencerServiceStub: SequencerServiceStub = _
  private val maxRequestSizeDefault = MaxRequestSize(NonNegativeInt.tryCreate(30000))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)

        daChannel = SequencerTestHelper.createChannel(
          sequencer1,
          loggerFactory,
          executionContext,
        )

        requestNewToken()
      }

  private def requestNewToken()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val token =
      SequencerTestHelper
        .requestToken(
          daChannel,
          daId,
          participant1.id,
          SynchronizerCrypto(participant1.crypto, staticSynchronizerParameters1),
          testedProtocolVersion,
          loggerFactory,
        )
        .value
        .futureValueUS
        .value

    sequencerServiceStub = new SequencerServiceStub(daChannel).withCallCredentials(
      SequencerTestHelper.mkCallCredentials(daId, participant1.id, token)
    )
  }

  private def setMaxRequestSizeAndRestart(
      maxRequestSize: MaxRequestSize
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    synchronizerOwners1.foreach {
      _.topology.synchronizer_parameters
        .propose_update(synchronizerId = daId, _.update(maxRequestSize = maxRequestSize.unwrap))
    }

    // `maxRequestSize` configures both the gRPC channel size on the sequencer node and
    // the maximum size that a sequencer client is allowed to transfer.
    // When changing its value, the sequencer nodes need to be restarted for the new value
    // to be taken into account.
    participants.all.synchronizers.disconnect_all()
    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        sequencer1.stop()
        sequencer1.start()
        sequencer1.health.wait_for_running()
        utils.retry_until_true(daChannel.getState(true) == ConnectivityState.READY)

        // Request a new token (the sequencer was restarted and tokens are not persistent)
        requestNewToken()

        participants.all.synchronizers.reconnect_all()
        eventually() {
          participant1.health.ping(participant1)
        }
      },
      LogEntry.assertLogSeq(
        Seq(),
        Seq(
          // mediator might squeak
          _.shouldBeCantonErrorCode(LostSequencerSubscription),
          // in rare cases SequencerClient#acknowledgeSigned runs into an already shut down
          // sequencer, resulting in a `Connection refused` error
          _.message should include("Connection refused"),
        ),
      ),
    )

  }

  override def afterAll(): Unit = {
    SequencerTestHelper.closeChannel(daChannel, logger, getClass.getSimpleName)
    super.afterAll()
  }

  private lazy val availability = SecurityTest(
    property = Availability,
    asset = "sequencer server",
  )

  "A grpc sequencer server should decompress request that are smaller than maxRequestSize" taggedAs availability
    .setHappyCase("decompress correctly the submission request") in { implicit env =>
    import env.*

    setMaxRequestSizeAndRestart(maxRequestSizeDefault)

    val request = SequencerTestHelper.mkSubmissionRequest(participant1.id)
    val signature = SequencerTestHelper.signSubmissionRequest(request, participant1).futureValueUS

    SequencerTestHelper
      .sendSubmissionRequest(sequencerServiceStub, request, signature)
      .futureValue shouldBe ()
  }

  "A grpc sequencer server should not decompress more than the maxRequestSize configured" taggedAs availability
    .setAttack(
      attack = Attack(
        actor = "a malicious synchronizer member",
        threat = "crash the sequencer server with a zip bombing attack",
        mitigation = "reject the request",
      )
    ) in { implicit env =>
    import env.*

    setMaxRequestSizeAndRestart(maxRequestSizeDefault)

    val bigEnvelope =
      ClosedEnvelope.create(
        ByteString.copyFromUtf8("A" * 50000),
        Recipients.cc(participant2.id),
        Seq.empty,
        testedProtocolVersion,
      )

    val requestThatCanBeCompressed =
      SequencerTestHelper
        .mkSubmissionRequest(participant1.id)
        .focus(_.batch.envelopes)
        .replace(List(bigEnvelope))

    val alarmMsg =
      s"Max bytes to decompress is exceeded. The limit is ${maxRequestSizeDefault.unwrap} bytes."
    val responseF = loggerFactory.assertLogs(
      SequencerTestHelper.sendSubmissionRequest(sequencerServiceStub, requestThatCanBeCompressed),
      _.shouldBeCantonError(
        SequencerError.MaxRequestSizeExceeded,
        _ shouldBe alarmMsg,
      ),
    )

    val expectedReason =
      ProtoDeserializationError
        .MaxBytesToDecompressExceeded(alarmMsg)
        .message

    // test the error message received by the client
    inside(responseF.failed.futureValue) { case ex: StatusRuntimeException =>
      ex.getMessage should include(expectedReason)
    }
    // make sure that the sequencer is still responsive
    assertPingSucceeds(participant1, participant1)
  }

  "A grpc sequencer server can process requests exceeding the maxRequestSize after modifying this value" taggedAs availability
    .setHappyCase(
      "decompress correctly the submission request"
    ) in { implicit env =>
    import env.*

    setMaxRequestSizeAndRestart(maxRequestSizeDefault)

    val bigEnvelope =
      ClosedEnvelope.create(
        ByteString.copyFromUtf8(scala.util.Random.nextString(50000)), // cannot be compressed
        Recipients.cc(participant2.id),
        Seq.empty,
        testedProtocolVersion,
      )

    val request =
      SequencerTestHelper
        .mkSubmissionRequest(participant1.id)
        .focus(_.batch.envelopes)
        .replace(List(bigEnvelope))
    val signature = SequencerTestHelper.signSubmissionRequest(request, participant1).futureValueUS

    SequencerTestHelper
      .sendSubmissionRequest(sequencerServiceStub, request, signature)
      .failed
      .futureValue
      .getMessage should include("RESOURCE_EXHAUSTED: gRPC message exceeds maximum size 30000")

    // modify the value
    val newMaxRequestSize = MaxRequestSize(500000)
    setMaxRequestSizeAndRestart(newMaxRequestSize)

    // the recipient not expecting to receive a response, we stop it to avoid crashing it
    participant2.stop()
    val responseF2 =
      SequencerTestHelper.sendSubmissionRequest(sequencerServiceStub, request, signature)
    responseF2.futureValue shouldBe (())
  }
}

class InvalidRequestSequencerBftOrderingIntegrationTestPostgres
    extends InvalidRequestSequencerIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
