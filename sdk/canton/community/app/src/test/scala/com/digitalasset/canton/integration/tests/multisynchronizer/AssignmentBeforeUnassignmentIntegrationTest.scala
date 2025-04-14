// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission

class AssignmentBeforeUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

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

        IouSyntax.createIou(participant1, Some(daId))(aliceId, aliceId)
      }

  "assignment is completed on participant2 before unassignment" in { implicit env =>
    import env.*

    val contract = participant1.ledger_api.javaapi.state.acs
      .await(Iou.COMPANION)(aliceId, synchronizerFilter = Some(daId))

    // we disconnect participant2 from the synchronizer in order to no process the unassignment
    participant2.synchronizers.disconnect(daName)

    val unassignId = participant1.ledger_api.commands
      .submit_unassign(aliceId, Seq(contract.id.toLf), daId, acmeId)
      .unassignId

    participant1.ledger_api.commands.submit_assign(
      aliceId,
      unassignId,
      daId,
      acmeId,
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

}

class AssignmentBeforeUnassignmentIntegrationTestPostgres
    extends AssignmentBeforeUnassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
