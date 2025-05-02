// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.UnknownReassignmentId
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isMediatorResult
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.util.ReassignmentTag.Source

import scala.concurrent.Promise

sealed trait AssignmentBeforeUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {

  private var aliceId: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        val alice = "alice"

        // Enable alice on other participants, on all synchronizers
        new PartiesAllocator(participants.all.toSet)(
          Seq(alice -> participant1),
          Map(
            alice -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant2, Submission),
              )),
              acmeId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant2, Submission),
              )),
            )
          ),
        ).run()

        aliceId = alice.toPartyId(participant1)
      }

  "assignment is completed on participant2 before unassignment" in { implicit env =>
    import env.*

    val contract = IouSyntax.createIou(participant1, Some(daId))(aliceId, aliceId)

    // we disconnect participant2 from the synchronizer in order to no process the unassignment
    participant2.synchronizers.disconnect(daName)

    val unassignId = participant1.ledger_api.commands
      .submit_unassign(
        aliceId,
        Seq(contract.id.toLf),
        daId,
        acmeId,
        timeout = None, // not waiting for all the other participants to receive the unassignment
      )
      .unassignId

    participant1.ledger_api.commands.submit_assign(
      aliceId,
      unassignId,
      daId,
      acmeId,
      timeout = None, // not waiting for all the other participants to receive the unassignment
    )

    val contractReassigned = participant1.ledger_api.javaapi.state.acs
      .await(Iou.COMPANION)(aliceId, synchronizerFilter = Some(acmeId))

    contractReassigned.id shouldBe contract.id

    val begin = participant2.ledger_api.state.end()
    participant2.synchronizers.reconnect_local(daName)

    val updates = participant2.ledger_api.updates.flat(
      partyIds = Set(aliceId),
      completeAfter = 1,
      beginOffsetExclusive = begin,
      synchronizerFilter = Some(daId),
    )

    // unassignment succeeded on participant2
    updates.headOption.value match {
      case unassigned: UpdateService.UnassignedWrapper =>
        unassigned.unassignId shouldBe unassignId
      case other =>
        fail(s"Expected a reassignment event but got $other")
    }
  }

  "assignment starts while unassignment is still in progress" in { implicit env =>
    import env.*
    val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, aliceId)

    val until: Promise[Unit] = Promise[Unit]()
    val aboutToSendVerdict: Promise[Unit] = Promise[Unit]()
    val begin = participant1.ledger_api.state.end()

    getProgrammableSequencer(sequencer1.name).setPolicy_(
      "delay mediator result"
    )(
      delayMediatorResult(until, aboutToSendVerdict)
    )

    participant1.ledger_api.commands
      .submit_unassign_async(
        aliceId,
        Seq(LfContractId.assertFromString(iou.id.contractId)),
        daId,
        acmeId,
      )

    aboutToSendVerdict.future.futureValue
    // Don't allow participant 2 to receive the mediator result and thus to finish phase 7 of the unassignment
    getProgrammableSequencer(sequencer1.name).blockFutureMemberRead(participant2)

    participant2.synchronizers.disconnect(daName)
    participant2.synchronizers.reconnect_local(daName)

    until.trySuccess(())

    val updates = participant1.ledger_api.updates.flat(
      partyIds = Set(aliceId),
      completeAfter = 1,
      beginOffsetExclusive = begin,
      resultFilter = _.isUnassignment,
      synchronizerFilter = Some(daId),
    )

    val unassign1 = updates.headOption.value match {
      case unassigned: UpdateService.UnassignedWrapper =>
        unassigned.unassignId
      case other =>
        fail(s"Expected a reassignment event but got $other")
    }

    participant1.ledger_api.commands
      .submit_assign(
        aliceId,
        unassign1,
        daId,
        acmeId,
        timeout = None, // only participant 1 will see the assignment event
      )

    val reassignmentStoreP2 = participant2.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore

    val reassignmentId =
      ReassignmentId(Source(daId), CantonTimestamp.assertFromLong(unassign1.toLong))

    reassignmentStoreP2.findReassignmentEntry(reassignmentId).futureValueUS shouldBe Left(
      UnknownReassignmentId(reassignmentId)
    )

    // unblock the read of the mediator result to finish the unassignment and to unblock the assignment
    getProgrammableSequencer(sequencer1.name).unBlockMemberRead(participant2)

    eventually() {
      val reassignmentEntry = reassignmentStoreP2
        .findReassignmentEntry(reassignmentId)
        .futureValueUS
        .value

      reassignmentEntry.assignmentTs should not be empty
      reassignmentEntry.unassignmentRequest should not be empty
    }

  }

  private def delayMediatorResult(
      until: Promise[Unit],
      aboutToSendVerdict: Promise[Unit],
  ): SendPolicyWithoutTraceContext =
    submissionRequest =>
      if (isMediatorResult(submissionRequest)) {
        aboutToSendVerdict.trySuccess(())
        SendDecision.HoldBack(until.future)
      } else SendDecision.Process

}

class AssignmentBeforeUnassignmentIntegrationTestPostgres
    extends AssignmentBeforeUnassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
