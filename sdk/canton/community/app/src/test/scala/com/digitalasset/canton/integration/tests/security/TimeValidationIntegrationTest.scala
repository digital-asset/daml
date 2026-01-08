// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.option.*
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.TimeoutError
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.MalformedRequest
import com.digitalasset.canton.sequencing.protocol.MemberRecipient
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.MaliciousParticipantNode

import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
trait TimeValidationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with SecurityTestSuite
    with HasCycleUtils
    with SecurityTestHelpers {

  private val ledgerIntegrity: SecurityTest = SecurityTest(property = Integrity, asset = "ledger")

  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference[CryptoPureApi]()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        synchronizerOwners1.foreach {
          _.topology.synchronizer_parameters.set_ledger_time_record_time_tolerance(
            synchronizerId = daId,
            config.NonNegativeFiniteDuration(tolerance),
          )
        }
        synchronizerOwners1.foreach {
          _.topology.synchronizer_parameters.set_preparation_time_record_time_tolerance(
            synchronizerId = daId,
            config.NonNegativeFiniteDuration(tolerance),
            // Bypass a validation that can interfere with the default tolerance
            force = true,
          )
        }

        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participants.local.dars.upload(CantonExamplesPath)
        participants.local.dars.upload(CantonTestsPath)

        sequencer = getProgrammableSequencer(sequencer1.name)

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
      }

  private val tolerance: Duration = Duration.ofSeconds(60)
  private var sequencer: ProgrammableSequencer = _

  private def pingCommand(implicit env: TestConsoleEnvironment): javaapi.data.Command = {
    import env.*
    new Ping(
      UUID.randomUUID().toString,
      participant1.id.adminParty.toProtoPrimitive,
      participant2.id.adminParty.toProtoPrimitive,
    ).create.commands.loneElement
  }

  "A command gets rejected, if ledger time << record time" taggedAs ledgerIntegrity
    .setAttack(
      attack = Attack(
        actor = "ledger api user",
        threat = "submits a command with a ledger time far in the past",
        mitigation = "reject the command",
      )
    ) in { implicit env =>
    import env.*

    sequencer.setPolicy_("advance sim clock to after tolerance") { submissionRequest =>
      val allRecipients = submissionRequest.batch.allRecipients
      submissionRequest.sender match {
        case _: ParticipantId
            if allRecipients.contains(MemberRecipient(participant1)) &&
              submissionRequest.isConfirmationRequest =>
          // Move the clock to after the ledgerTimeRecordTimeTolerance
          // and pretend that the submission request has not been sequenced before by simply dropping it
          env.environment.simClock.value.advance(tolerance.multipliedBy(2))
          SendDecision.Drop
        case _ => SendDecision.Process
      }
    }

    loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
      participant1.ledger_api.javaapi.commands
        .submit(Seq(participant1.id.adminParty), Seq(pingCommand)),
      _.warningMessage should include("Submission timed out at"),
      _.shouldBeCantonErrorCode(TimeoutError),
    )
  }

  "A command gets rejected, if ledger time >> record time" taggedAs ledgerIntegrity
    .setAttack(
      attack = Attack(
        actor = "ledger api user",
        threat = "submits a command with a ledger time far in the future",
        mitigation = "reject the command",
      )
    ) in { implicit env =>
    import env.*

    sequencer.resetPolicy()

    val minLedgerTime = env.environment.clock.now.add(tolerance.plusSeconds(1L))

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.javaapi.commands
        .submit(
          Seq(participant1.id.adminParty),
          Seq(pingCommand),
          minLedgerTimeAbs = minLedgerTime.toInstant.some,
        ),
      _.warningMessage should include regex "Time validation has failed: The delta of the ledger time .* and the record time .* exceeds the max of",
      _.commandFailureMessage should include(LocalRejectError.TimeRejects.LedgerTime.id),
    )
  }

  // The validation of preparation time has been introduced in Canton for uniformity with other ledger implementations.
  // It is not security relevant in Canton (and thus also not annotated as a SecurityTest).
  "A command gets rejected, if preparation time << record time" in { implicit env =>
    import env.*

    // Relevant timestamps:
    //
    // command | preparation time | ledger time | record time
    // --------|------------------|-------------|---------------
    // cmd 1   | tolerance        | tolerance   | 0 + eps
    // --------|------------------|-------------|---------------
    // cmd 2   | 0                | tolerance   | tolerance + 1
    // --------|------------------|-------------|---------------
    //
    // cmd 1 gets accepted.
    // cmd 2 gets rejected, because preparation time << record time

    val count = new AtomicInteger(0)

    sequencer.setPolicy_("advance sim clock by tolerance + 1 before second command") {
      submissionRequest =>
        val allRecipients = submissionRequest.batch.allRecipients
        submissionRequest.sender match {
          case _: ParticipantId
              if allRecipients.contains(MemberRecipient(participant1)) &&
                submissionRequest.isConfirmationRequest && count.incrementAndGet() == 2 =>
            // Delay the sequencing timestamp of the second request
            env.environment.simClock.value.advance(tolerance.plus(Duration.ofSeconds(1)))
          case _ =>
        }

        SendDecision.Process
    }

    // Set minLedgerTime to bump ledger time for first command.
    val minLedgerTime = environment.simClock.value.now.add(tolerance)

    val iouCommand =
      new Iou(
        participant1.adminParty.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
        new Amount(1.toBigDecimal, "snack"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq

    val iouContract = JavaDecodeUtil
      .decodeAllCreated(Iou.COMPANION)(
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(participant1.id.adminParty),
            iouCommand,
            minLedgerTimeAbs = Some(minLedgerTime.toInstant),
          )
      )
      .headOption
      .value

    // Wait until participant2 sees the iou creation
    eventually() {
      val acs =
        participant2.ledger_api.state.acs.of_party(participant2.adminParty, verbose = false)
      val createdIds = acs.map(_.contractId)
      createdIds should contain(iouContract.id.contractId)
    }

    // Command 2 is submitted at clock.now,
    // but its ledger time is tolerance due to the causal dependency on command 1.
    val shareIouCommand = iouContract.id
      .exerciseShare(participant1.adminParty.toProtoPrimitive)
      .commands
      .asScala
      .toSeq

    loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
      participant2.ledger_api.javaapi.commands
        .submit(Seq(participant2.id.adminParty), shareIouCommand),
      _.warningMessage should include regex "Time validation has failed: The delta of the preparation time .* and the record time .* exceeds the max of",
      _.commandFailureMessage should include(LocalRejectError.TimeRejects.PreparationTime.id),
    )
  }

  "A request gets rejected if it uses a future contract" taggedAs ledgerIntegrity
    .setAttack(
      attack = Attack(
        actor = "a malicious participant",
        threat = "submits a command that uses a contract created at a future ledger time",
        mitigation = "alarm and reject the command",
      )
    ) in { implicit env =>
    import env.*

    val maliciousP1 =
      MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
        testSubmissionServiceOverrideO = Some(
          TestSubmissionService(
            participant = participant1
          )
        ),
      )

    val createCmd = new UniversalContract(
      List(participant1.adminParty.toProtoPrimitive).asJava,
      List.empty.asJava,
      List.empty.asJava,
      List(participant1.adminParty.toProtoPrimitive).asJava,
    ).create.commands.asScala.toSeq
    val transaction =
      participant1.ledger_api.javaapi.commands
        .submit(Seq(participant1.adminParty), createCmd)
    val creationTime =
      CantonTimestamp.assertFromInstant(transaction.getEffectiveAt)
    val cid = JavaDecodeUtil
      .decodeAllCreated(UniversalContract.COMPANION)(
        transaction
      )
      .loneElement

    val exerciseCmdRaw = cid.id
      .exerciseReplace(
        List(participant1.adminParty.toProtoPrimitive).asJava,
        List.empty.asJava,
        List.empty.asJava,
        List(participant1.adminParty.toProtoPrimitive).asJava,
        List.empty.asJava,
      )
      .commands
      .asScala
      .toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))

    val exerciseTime = creationTime.immediatePredecessor.toLf
    val exerciseCmd = CommandsWithMetadata(
      exerciseCmdRaw,
      Seq(participant1.adminParty),
      ledgerTime = exerciseTime,
    )
    val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      trackingLedgerEvents(Seq(participant1), Seq.empty) {
        maliciousP1.submitCommand(exerciseCmd).futureValueUS
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            _.shouldBeCantonError(
              MalformedRequest,
              _ should include regex raw"A request with ledger time \S+ uses a future contract \(created at \S+, id = \S+\)",
            ),
            "error",
          )
        )
      ),
    )
    events.assertNoTransactions()
  }
}

class TimeValidationReferenceIntegrationTestInMemory extends TimeValidationIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
