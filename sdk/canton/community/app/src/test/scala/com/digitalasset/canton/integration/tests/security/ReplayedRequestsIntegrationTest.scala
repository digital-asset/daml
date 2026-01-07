// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.javaapi.data.Command
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Availability, Integrity}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SequencerTestHelper,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.{
  CreatesExistingContracts,
  MalformedRequest,
}
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.LedgerTime
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  MemberRecipient,
  Recipients,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import io.grpc.ManagedChannel
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*

trait ReplayedRequestsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with SecurityTestSuite {

  var sequencer: ProgrammableSequencer = _

  var sequencerChannel: ManagedChannel = _
  var p2SequencerServiceStub: SequencerServiceStub = _

  // Using AtomicRef, because this gets read from various threads.
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        // Use a sim clock so that we don't have to worry about timeouts
        ConfigTransforms.useStaticTime
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonExamplesPath)

        sequencer = getProgrammableSequencer(sequencer1.name)

        sequencerChannel =
          SequencerTestHelper.createChannel(sequencer1, loggerFactory, executionContext)

        val token =
          SequencerTestHelper
            .requestToken(
              sequencerChannel,
              daId,
              participant2.id,
              SynchronizerCrypto(participant2.crypto, staticSynchronizerParameters1),
              testedProtocolVersion,
              loggerFactory,
            )
            .futureValueUS

        p2SequencerServiceStub = new SequencerServiceStub(sequencerChannel).withCallCredentials(
          SequencerTestHelper.mkCallCredentials(daId, participant2.id, token.value)
        )

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
      }

  override def afterAll(): Unit = {
    SequencerTestHelper.closeChannel(sequencerChannel, logger, getClass.getSimpleName)
    super.afterAll()
  }

  override val defaultParticipant: String = "participant1"

  // Restricting this test to protocolVersion >= v4, as the error message is different at lower versions
  // and we do not officially support the tested scenario for lower protocol versions.

  "Participants can recover from a malformed mediator confirmation request" taggedAs SecurityTest(
    property = Availability,
    asset = "participant node",
    attack = Attack(
      actor = "a Canton user",
      threat = "crash a participant with a malformed mediator message",
      mitigation = "reject the request",
    ),
  ) in { implicit env =>
    import env.*

    val party1 = participant1.id.adminParty

    val commandQueued = new AtomicBoolean()

    sequencer.setPolicy_("delay participant1's submission") { submissionRequest =>
      if (
        !commandQueued.getAndSet(true) &&
        submissionRequest.isConfirmationRequest &&
        submissionRequest.sender == participant1.id
      ) {

        val modifiedRequest = submissionRequest
          .focus(_.batch.envelopes)
          .modify(_.map { envelope =>
            val allRecipients = envelope.recipients.allRecipients.forgetNE
            val mediatorGroup = MediatorGroupRecipient(MediatorGroupIndex.zero)

            if (allRecipients == Set(MemberRecipient(participant1.id), mediatorGroup)) {
              // This is the envelope corresponding to the root hash message.
              // Remove the mediator from the recipients.
              envelope
                .focus(_.recipients)
                .replace(Recipients.cc((allRecipients - mediatorGroup).loneElement))
            } else {
              envelope
            }
          })
        val signedModifiedRequest = signModifiedSubmissionRequest(
          modifiedRequest,
          participant1.underlying.value.sync.syncCrypto
            .tryForSynchronizer(daId, staticSynchronizerParameters1),
          Some(environment.now),
        )

        SendDecision.Replace(signedModifiedRequest)
      } else SendDecision.Process
    }

    val iouCommand =
      new Iou(
        party1.toProtoPrimitive,
        party1.toProtoPrimitive,
        new Amount(1.toBigDecimal, "snack"),
        List.empty.asJava,
      ).create.commands.loneElement

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      trackingLedgerEvents(Seq(participant1), Seq.empty) {
        participant1.ledger_api.javaapi.commands
          .submit_async(
            Seq(party1),
            Seq(iouCommand),
            commandId = "MalformedMessageMissingRootHash",
          )

        // Wait until the request has been sequenced.
        eventually() {
          loggerFactory.numberOfRecordedEntries should be > 0
        }
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            _.shouldBeCantonError(
              MediatorError.MalformedMessage,
              _ should fullyMatch regex raw"Received a mediator confirmation request with id \S+ with invalid root hash messages\. Rejecting\.\.\. Reason: Missing root hash message for informee participants: \S+",
            ),
            "mediator",
          ),
          (
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should startWith regex raw"\(sequencer counter: \S+, timestamp: \S+\): Received root hash messages that were not sent to a mediator: OpenEnvelope\(",
            ),
            "participant missing mediator",
          ),
          (
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should fullyMatch regex raw"\(sequencer counter: \S+, timestamp: \S+\): No valid root hash message in batch",
            ),
            "participant invalid root hash",
          ),
        )
      ),
    )

    sequencer.resetPolicy()

    // Make sure that both participants are still alive
    participant1.health.ping(participant2)
  }

  lazy val testReplayAttack: SecurityTest = SecurityTest(
    property = Integrity,
    asset = "ledger",
    attack = Attack(
      actor = "a malicious network participant",
      threat = "submits a request with a non-unique uuid",
      mitigation = "the mediator rejects the request",
    ),
  )

  "A mediator rejects a replayed approved request" taggedAs testReplayAttack in { implicit env =>
    import env.*

    val offsetAtBeginning = participant1.ledger_api.state.end()

    val replayRequest = captureRequest {
      val replayCommandId = "maultaschen"
      participant1.ledger_api.javaapi.commands.submit(
        Seq(participant1.adminParty),
        Seq(createIou),
        commandId = replayCommandId,
      )
    }
    val signature =
      SequencerTestHelper.signSubmissionRequest(replayRequest, participant2).futureValueUS

    logger.info("Now replaying request...")

    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      {
        SequencerTestHelper
          .sendSubmissionRequest(
            p2SequencerServiceStub,
            replayRequest,
            signature,
          )
          .futureValue
        assertPingSucceeds(participant1, participant2)
      },
      LogEntry.assertLogSeq(
        Seq(
          (assertUuidInUseError, "non unique request uuid"),
          (_.shouldBeCantonErrorCode(CreatesExistingContracts), "creates existing contracts"),
          (
            _.shouldBeCantonError(
              MalformedRequest,
              _ should include("belongs to a replayed transaction"),
            ),
            "participant detects replayed transaction",
          ),
        ),
        Seq(
          // Accept other info level messages
          _.level == Level.INFO shouldBe true
        ),
      ),
    )

    assertNoExtraCompletion(offsetAtBeginning)
  }

  private def captureRequest(
      body: => Unit,
      releaseResponseF: Future[Unit] = Future.unit,
  )(implicit env: TestConsoleEnvironment): SubmissionRequest = {
    import env.*

    val submissionRequestP = Promise[SubmissionRequest]()
    val responseArrived = new AtomicBoolean()

    sequencer.setPolicy_("capture next submission request and hold back first response") {
      submissionRequest =>
        if (isConfirmationResponse(submissionRequest) && !responseArrived.getAndSet(true)) {
          SendDecision.HoldBack(releaseResponseF)
        } else {
          submissionRequestP.trySuccess(submissionRequest)
          SendDecision.Process
        }
    }

    body

    val submissionRequest = submissionRequestP.future.futureValue
    submissionRequest.copy(sender = participant2.id)
  }

  private def createIou(implicit env: TestConsoleEnvironment): Command = {
    import env.*
    new Iou(
      participant1.adminParty.toProtoPrimitive,
      participant2.adminParty.toProtoPrimitive,
      new Amount(10.toBigDecimal, "pears"),
      List.empty.asJava,
    ).create.commands.loneElement
  }

  private def assertUuidInUseError(entry: LogEntry): Assertion = {
    entry.shouldBeCantonErrorCode(MediatorError.DuplicateConfirmationRequest)
    entry.infoMessage should include regex "The request UUID \\(.*\\) is a duplicate of a previous request with an identical UUID. It cannot be re-used until"
  }

  private def assertNoExtraCompletion(
      offsetAtBeginning: Long,
      timeoutMillis: Long = 10000,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*

    val completionsAfterReplay =
      participant1.ledger_api.completions.list(
        participant1.adminParty,
        2,
        offsetAtBeginning,
        timeout = NonNegativeDuration.ofMillis(timeoutMillis),
      )

    completionsAfterReplay should have size 1
  }

  "A mediator rejects a replayed pending request" taggedAs testReplayAttack in { implicit env =>
    import env.*

    val offsetAtBeginning = participant1.ledger_api.state.end()

    val releaseResponseP = Promise[Unit]()

    val replayRequest = captureRequest(
      {
        val replayCommandId = "nudelsuppe"
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(participant1.adminParty),
          Seq(createIou),
          commandId = replayCommandId,
        )
      },
      releaseResponseP.future,
    )
    val signature =
      SequencerTestHelper.signSubmissionRequest(replayRequest, participant2).futureValueUS

    logger.info("Now replaying request...")

    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      {
        SequencerTestHelper
          .sendSubmissionRequest(p2SequencerServiceStub, replayRequest, signature)
          .futureValue
        eventually()(loggerFactory.numberOfRecordedEntries should be > 0)
        releaseResponseP.success(())
        assertPingSucceeds(participant1, participant2)
      },
      LogEntry.assertLogSeq(
        Seq(
          (assertUuidInUseError, "non unique request uuid"),
          (
            _.shouldBeCantonError(
              MalformedRequest,
              _ should include("belongs to a replayed transaction"),
            ),
            "participant detects replayed transaction",
          ),
        ),
        Seq(
          // Accept other info level messages
          _.level == Level.INFO shouldBe true
        ),
      ),
    )

    // Wait a bit longer than usual, because the first request still needs to be processed.
    assertNoExtraCompletion(offsetAtBeginning, 1000)
  }

  "A mediator rejects a replayed rejected request" taggedAs testReplayAttack in { implicit env =>
    import env.*

    val offsetAtBeginning = participant1.ledger_api.state.end()

    val replayRequest = captureRequest {
      // Send a transaction with ledger time far in the future.
      // It is important that the uuid gets deduplicated as well,
      // because otherwise a malicious participant could replay the transaction at a later point in time.
      val replayCommandId = "schnitzel"
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant1.ledger_api.javaapi.commands.submit(
          Seq(participant1.adminParty),
          Seq(createIou),
          commandId = replayCommandId,
          minLedgerTimeAbs = Some(Instant.ofEpochSecond(10000)),
        ),
        entries =>
          forAtLeast(1, entries) {
            _.shouldBeCantonErrorCode(LedgerTime)
          },
      )
    }
    val signature =
      SequencerTestHelper.signSubmissionRequest(replayRequest, participant2).futureValueUS

    logger.info("Now replaying request...")

    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      {
        SequencerTestHelper
          .sendSubmissionRequest(p2SequencerServiceStub, replayRequest, signature)
          .futureValue
        assertPingSucceeds(participant1, participant2)
      },
      LogEntry.assertLogSeq(
        Seq(
          (assertUuidInUseError, "non unique request uuid"),
          (_.warningMessage should include("Time validation has failed"), "time validation"),
          (
            _.shouldBeCantonError(
              MalformedRequest,
              _ should include("belongs to a replayed transaction"),
            ),
            "participant detects replayed transaction",
          ),
        ),
        Seq(
          // Accept other info level messages
          _.level == Level.INFO shouldBe true
        ),
      ),
    )

    assertNoExtraCompletion(offsetAtBeginning)
  }

  "A dropped but replayed request does not generate a duplicate completion event" taggedAs SecurityTest(
    property = Integrity,
    asset = "ledger",
    attack = Attack(
      actor = "a malicious network participant",
      threat = "replays a request previously submitted by another participant",
      mitigation = "honest participants do not emit multiple completion events",
    ),
  ) in { implicit env =>
    import env.*

    // This test exercises the following replay attack scenario:
    //
    // 1. Participant p submits a request to the sequencer.
    // 2. Instead of sequencing the submission request, the sequencer hands it to a malicious participant.
    //    This participant changes the message ID and sender fields of the submission request and resubmits it in its own name.
    // 3. The sequencer sequences the resubmission and delivers it to p. The message ID is not set because p was not the sender.
    // 4. The original submission request of p never gets sequenced.
    //
    // We want to make sure that the original submission request, which was dropped, does not generate a duplicate
    // completion event due to it timing out.

    val offsetAtBeginning = participant1.ledger_api.state.end()

    val replayRequest = captureAndDropRequest(participant1) {
      val replayCommandId = "carac"
      participant1.ledger_api.javaapi.commands.submit_async(
        Seq(participant1.adminParty),
        Seq(createIou),
        commandId = replayCommandId,
      )
    }.copy(sender = participant2.id)
    val signature =
      SequencerTestHelper.signSubmissionRequest(replayRequest, participant2).futureValueUS

    logger.info("Now replaying request...")
    val clock = environment.simClock.value

    SequencerTestHelper
      .sendSubmissionRequest(
        p2SequencerServiceStub,
        replayRequest,
        signature,
      )
      .futureValue

    assertPingSucceeds(participant1, participant2)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        // Move past the expiration of the `maxSequencingTime` for the request submitted by participant1
        clock.advanceTo(replayRequest.maxSequencingTime.plusSeconds(1))
        assertPingSucceeds(participant1, participant2)
      },
      LogEntry.assertLogSeq(
        Seq(
          (_.warningMessage should include("Submission timed out"), "request times out")
        ),
        Seq.empty,
      ),
    )

    assertNoExtraCompletion(offsetAtBeginning)
  }

  private def captureAndDropRequest(
      participant: ParticipantReference
  )(body: => Unit): SubmissionRequest = {
    val submissionRequestP = Promise[SubmissionRequest]()
    val firstConfirmationRequestFromParticipant = new AtomicBoolean(true)

    sequencer.setPolicy_(s"capture next submission request from $participant and drop it") {
      submissionRequest =>
        if (
          submissionRequest.isConfirmationRequest && submissionRequest.sender == participant.id && firstConfirmationRequestFromParticipant
            .getAndSet(false)
        ) {
          submissionRequestP.trySuccess(submissionRequest)
          SendDecision.Drop
        } else {
          SendDecision.Process
        }
    }

    body

    submissionRequestP.future.futureValue
  }
}

class ReplayedRequestsIntegrationTestPostgres extends ReplayedRequestsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
