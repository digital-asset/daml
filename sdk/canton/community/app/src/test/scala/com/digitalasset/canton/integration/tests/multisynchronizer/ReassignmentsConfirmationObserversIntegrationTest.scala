// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, LocalSequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  EntitySyntax,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
  PartiesAllocator,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentCompleted
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{BaseTest, SynchronizerAlias, config}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/*
This test checks that confirmation of reassignments is not done/required by
participants hosting only observers. They should however populate the reassignment store.
The same behavior is expected by participants hosting signatories with observation permission.

Topology:
- Synchronizer da (source synchronizer)
  signatory -> participant1 (confirmation rights)
  signatory -> participant3 (observation rights)
  observer1 -> participant1 (confirmation rights)
  observer2 -> participant2 (confirmation rights)

- Synchronizer acme (target synchronizer)
  signatory -> participant1 (confirmation rights)
  signatory -> participant3 (observation rights)
  observer1 -> participant1 (observation rights)
  observer2 -> participant2 (observation rights)
 */
sealed trait ReassignmentsConfirmationObserversIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with EntitySyntax {

  private var signatory: PartyId = _
  private var observer1: PartyId = _
  private var observer2: PartyId = _

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      // We want to trigger time out
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        // Disable automatic assignment so that we really control it
        def disableAutomaticAssignment(
            sequencer: LocalSequencerReference
        ): Unit =
          sequencer.topology.synchronizer_parameters
            .propose_update(
              sequencer.synchronizer_id,
              _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
            )

        disableAutomaticAssignment(sequencer1)
        disableAutomaticAssignment(sequencer2)

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        PartiesAllocator(Set(participant1, participant2, participant3))(
          newParties = Seq(
            "signatory" -> participant1,
            "observer1" -> participant1,
            "observer2" -> participant2,
          ),
          targetTopology = Map(
            "signatory" -> Map(
              daId -> (PositiveInt.one, Set(
                participant1.id -> ParticipantPermission.Submission,
                participant3.id -> ParticipantPermission.Observation,
              )),
              acmeId -> (PositiveInt.one, Set(
                participant1.id -> ParticipantPermission.Submission,
                participant3.id -> ParticipantPermission.Observation,
              )),
            ),
            "observer1" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(
                participant1.id -> ParticipantPermission.Observation
              )),
            ),
            "observer2" -> Map(
              daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(
                participant2.id -> ParticipantPermission.Observation
              )),
            ),
          ),
        )
        signatory = "signatory".toPartyId(participant1)
        observer1 = "observer1".toPartyId(participant1)
        observer2 = "observer2".toPartyId(participant2)

        eventually() {
          participant1.topology.party_to_participant_mappings.is_known(
            acmeId,
            observer1,
            List(participant1),
            Some(ParticipantPermission.Observation),
          ) shouldBe true

          participant2.topology.party_to_participant_mappings.is_known(
            acmeId,
            observer2,
            List(participant2),
            Some(ParticipantPermission.Observation),
          ) shouldBe true

          participant3.topology.party_to_participant_mappings.is_known(
            acmeId,
            signatory,
            List(participant3),
          ) shouldBe true
        }

        programmableSequencers.put(
          daName,
          getProgrammableSequencer(sequencer1.name),
        )
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))
      }

  "Observers on a contract" should {
    def lookupReassignment(participant: LocalParticipantReference, reassignmentId: ReassignmentId)(
        implicit env: TestConsoleEnvironment
    ): Either[ReassignmentStore.ReassignmentLookupError, UnassignmentData] = {
      import env.*

      participant.underlying.value.sync.syncPersistentStateManager
        .get(acmeId)
        .value
        .reassignmentStore
        .lookup(reassignmentId)
        .value
        .failOnShutdown
        .futureValue
    }

    def getSynchronizerOfContract(
        participant: LocalParticipantReference,
        party: PartyId,
        cid: String,
    ): SynchronizerId = {
      val synchronizer = participant.ledger_api.state.acs
        .active_contracts_of_party(party)
        .filter(_.createdEvent.value.contractId == cid)
        .loneElement
        .synchronizerId

      SynchronizerId.tryFromString(synchronizer)
    }

    "not confirm reassignments requests" in { implicit env =>
      import env.*

      val daConfirmations = new TrieMap[ParticipantId, Int]()
      val acmeConfirmations = new TrieMap[ParticipantId, Int]()

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer2)
      val cid = iou.id.contractId
      getSynchronizerOfContract(participant1, signatory, cid) shouldBe daId
      getSynchronizerOfContract(participant2, observer2, cid) shouldBe daId

      programmableSequencers(daName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(daConfirmations)
      )
      programmableSequencers(acmeName).setPolicy_("confirmations count")(
        countConfirmationResponsesPolicy(acmeConfirmations)
      )

      // Unassignment
      val unassignId =
        participant1.ledger_api.commands
          .submit_unassign(signatory, Seq(iou.id.toLf), daId, acmeId)
          .unassignId
      val reassignmentId =
        ReassignmentId(Source(daId), CantonTimestamp.fromProtoPrimitive(unassignId.toLong).value)

      // Check that reassignment store is populated on both participants
      eventually() {
        lookupReassignment(participant1, reassignmentId).value.unassignmentResult.value shouldBe
          a[DeliveredUnassignmentResult]
        lookupReassignment(participant2, reassignmentId).value.unassignmentResult.value shouldBe
          a[DeliveredUnassignmentResult]
        lookupReassignment(participant3, reassignmentId).value.unassignmentResult.value shouldBe
          a[DeliveredUnassignmentResult]
      }

      participant1.ledger_api.commands.submit_assign(signatory, unassignId, daId, acmeId)

      // no confirmation sent by p2, hosting observer2
      // no confirmation sent by p3, hosting signatory with observing permissions
      daConfirmations.toMap shouldBe Map(participant1.id -> 1)
      acmeConfirmations.toMap shouldBe Map(participant1.id -> 1)

      // reassignment should be completely done
      getSynchronizerOfContract(participant1, signatory, cid) shouldBe acmeId
      eventually() { // p2 might need some more time
        getSynchronizerOfContract(participant2, observer2, cid) shouldBe acmeId
      }

      lookupReassignment(participant1, reassignmentId).left.value shouldBe a[ReassignmentCompleted]
      lookupReassignment(participant2, reassignmentId).left.value shouldBe a[ReassignmentCompleted]
      lookupReassignment(participant3, reassignmentId).left.value shouldBe a[ReassignmentCompleted]

      programmableSequencers(daName).resetPolicy()
      programmableSequencers(acmeName).resetPolicy()
    }

    "work if signatory and observer are hosted on the same participant" in { implicit env =>
      import env.*

      // signatory and observer1 are hosted on participant1
      participant1.topology.party_to_participant_mappings.are_known(
        daId,
        Seq(signatory, observer1),
        Seq(participant1),
      ) shouldBe true

      // observer1 is not hosted on participant1
      participant1.topology.party_to_participant_mappings.are_known(
        daId,
        Seq(observer1),
        Seq(participant2),
      ) shouldBe false

      val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer1)

      val unassignId =
        participant1.ledger_api.commands
          .submit_unassign(signatory, Seq(iou.id.toLf), daId, acmeId)
          .unassignId

      participant1.ledger_api.commands.submit_assign(signatory, unassignId, daId, acmeId)
    }
  }

  // Count the number of confirmation responses sent by each participant
  private def countConfirmationResponsesPolicy(
      confirmations: TrieMap[ParticipantId, Int]
  ): SendPolicyWithoutTraceContext = submissionRequest =>
    submissionRequest.sender match {
      case pid: ParticipantId
          if ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest) =>
        val newValue = confirmations.getOrElse(pid, 0) + 1
        confirmations.put(pid, newValue)

        SendDecision.Process

      case _ => SendDecision.Process
    }
}

class ReassignmentsConfirmationObserversIntegrationTestPostgres
    extends ReassignmentsConfirmationObserversIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
