// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.slf4j.event.Level

import java.time.Instant
import java.util.UUID

class ReassignmentTargetTimestampIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasProgrammableSequencer
    with EntitySyntax {
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1_S1M1
    .withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)
      participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = acmeId)

      val allParticipants = participants.all.toSet
      PartiesAllocator(allParticipants)(
        newParties = Seq("alice" -> participant1),
        targetTopology = Map(
          "alice" -> Map(
            daId -> (PositiveInt.one, allParticipants.map(_.id -> Submission)),
            acmeId -> (PositiveInt.one, allParticipants.map(_.id -> Submission)),
          )
        ),
      )
      alice = "alice".toPartyId(participant1)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    }

  private var alice: PartyId = _
  private var maliciousP1: MaliciousParticipantNode = _

  "unassignment request with target timestamp too far in the future" should {
    "lead to LocalAbstain" in { implicit env =>
      import env.*

      val contract = createContract()
      val doomsday = CantonTimestamp.fromInstant(Instant.parse("2060-12-31T23:59:59Z")).value
      val unvalidatableUnassignReq = createFullUnassignmentTree(contract, Target(doomsday))

      val recorder = new RecordSequencerMessages()
      getProgrammableSequencer(sequencer1.name)
        .setPolicy_("record sequencer messages")(recorder.onSequencerMessage(_))

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.INFO))(
        within = maliciousP1
          .submitUnassignmentRequest(unvalidatableUnassignReq, Some(environment.now))
          .futureValueUS
          .value,
        logs =>
          forAtLeast(2, logs)(
            _.message should include regex "Sending an abstain verdict for .* because target timestamp is not validatable"
          ),
      )

      eventually() {
        ProgrammableSequencer.confirmationResponsesKind(recorder.seen) shouldBe Map(
          participant1.id -> Seq("LocalAbstain"),
          participant2.id -> Seq("LocalAbstain"),
        )
      }
    }
  }

  private def createContract()(implicit env: TestConsoleEnvironment): ContractInstance = {
    import env.*
    val iou = IouSyntax.createIou(participant1, Some(daId))(alice, alice, 1.0)
    participant1.testing
      .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
      .loneElement
  }

  private def createFullUnassignmentTree(
      contract: ContractInstance,
      targetTs: Target[CantonTimestamp],
  )(implicit
      env: TestConsoleEnvironment
  ): FullUnassignmentTree = {
    import env.*
    import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
    import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient

    val pureCrypto = participant1.underlying.value.sync.syncCrypto
      .forSynchronizer(daId, staticSynchronizerParameters1)
      .value
      .pureCrypto

    val helpers = ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = targetTs,
    )

    val uuid = new UUID(42L, 0)
    val seed = new SeedGenerator(pureCrypto).generateSaltSeed()

    helpers
      .unassignmentRequest(
        alice.toLf,
        participant1,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(Set(participant1, participant2))
      .toFullUnassignmentTree(pureCrypto, pureCrypto, seed, uuid)
  }

  private class RecordSequencerMessages {
    import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
    import com.digitalasset.canton.topology.Member
    import scala.collection.concurrent.TrieMap

    private val bySender = TrieMap.empty[Member, Seq[SubmissionRequest]]

    def onSequencerMessage(req: SubmissionRequest): SendDecision = {
      bySender.updateWith(req.sender) {
        case None => Some(Seq(req))
        case Some(msgs) => Some(msgs ++ Seq(req))
      }
      SendDecision.Process
    }

    def seen: Map[Member, Seq[SubmissionRequest]] = bySender.toMap
  }
}
