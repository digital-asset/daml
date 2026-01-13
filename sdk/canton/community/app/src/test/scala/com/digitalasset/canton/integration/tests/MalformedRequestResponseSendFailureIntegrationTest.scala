// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.util.{EntitySyntax, TestSubmissionService}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{MemberRecipient, Recipient}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

trait MalformedRequestResponseSendFailureIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with EntitySyntax {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "successfully mark requests as clean even if sending responses fail for a malformed request" in {
    implicit env =>
      import env.*

      participants.local.synchronizers.connect_local(sequencer1, daName)
      participants.all.dars.upload(CantonTestsPath)

      val alice = participant1.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participant2),
      )
      val bob = participant2.parties.enable(
        "Bob",
        synchronizeParticipants = Seq(participant1),
      )

      val maliciousP2 = MaliciousParticipantNode(
        participant2,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
        testSubmissionServiceOverrideO = Some(
          TestSubmissionService(
            participant = participant2
          )
        ),
      )
      val sequencer = getProgrammableSequencer(sequencer1.name)

      val failureCounter = new AtomicInteger(0)

      sequencer.setPolicy_("Fail confirmation response send") {
        // Only fail the first response from p1, which will be the malformed confirmation request response
        case sr
            if sr.sender == participant1.id && ProgrammableSequencerPolicies.isConfirmationResponse(
              sr
            ) && failureCounter
              .incrementAndGet() == 1 =>
          SendDecision.Reject
        case _ => SendDecision.Process
      }

      def modifyP1RootHashMessageRecipients(
          envelope: DefaultOpenEnvelope
      ): DefaultOpenEnvelope = {
        val attackedParticipant = participant1.id
        val attackedMember: Recipient = MemberRecipient(attackedParticipant)
        envelope.protocolMessage match {
          case rhm: RootHashMessage[?] =>
            val allRecipients = envelope.recipients.allRecipients
            if (allRecipients.contains(attackedMember)) {
              // Mess up the root hash of the first message addressed to p1
              envelope.copy(protocolMessage = rhm.copy(rootHash = RootHash(TestHash.digest(10))))
            } else envelope
          case _ => envelope
        }
      }

      val rawCmd = mkUniversal(
        maintainers = List(bob),
        observers = List(alice),
      ).create.commands.asScala.toSeq.map(c => Command.fromJavaProto(c.toProtoCommand))

      val cmd = CommandsWithMetadata(rawCmd, Seq(bob), ledgerTime = environment.now.toLf)

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          maliciousP2
            .submitCommand(
              cmd,
              envelopeInterceptor = modifyP1RootHashMessageRecipients,
            )
            .futureValueUS

          eventually() {
            // Wait until we observe p1 has tried to send the response to the mediator
            failureCounter.get() shouldBe 1
          }

          eventually() {
            // And then observe that the request does get cleaned up even with the failure
            participant1.underlying.value.sync
              .readyConnectedSynchronizerById(daId)
              .value
              .numberOfDirtyRequests() shouldBe 0
          }
        },
        entries => {
          val assertionResult = entries.count(_.message.contains("Failed to send responses")) >= 1
          if (assertionResult) {
            // Stop the mediator to make sure we capture all warnings they emit,
            // otherwise the test might fail
            mediator1.stop()
            participant1.stop()
            participant2.stop()
          }
          assertionResult shouldBe true
        },
      )

  }

  def mkUniversal(
      maintainers: List[PartyId] = List.empty,
      signatories: List[PartyId] = List.empty,
      observers: List[PartyId] = List.empty,
      actors: List[PartyId] = List.empty,
  ): UniversalContract = new UniversalContract(
    maintainers.map(_.toProtoPrimitive).asJava,
    signatories.map(_.toProtoPrimitive).asJava,
    observers.map(_.toProtoPrimitive).asJava,
    actors.map(_.toProtoPrimitive).asJava,
  )
}

class MalformedRequestResponseSendFailureIntegrationTestH2
    extends MalformedRequestResponseSendFailureIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
