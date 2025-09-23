// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  UnassignedWrapper,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, FeatureFlag}
import com.digitalasset.canton.integration.ConfigTransforms.zeroReassignmentTimeProofFreshnessProportion
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentCompleted
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.ReassignmentId

sealed trait RollbackUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair),
        zeroReassignmentTimeProofFreshnessProportion,
      )
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
          p.synchronizers.connect_local(sequencer2, alias = acmeName)
          p.dars.upload(CantonExamplesPath)
        }

        Seq(daName, acmeName).foreach { alias =>
          participant1.parties.enable(
            aliceS,
            synchronizeParticipants = Seq(participant2),
            synchronizer = alias,
          )
          participant2.parties.enable(
            bobS,
            synchronizeParticipants = Seq(participant1),
            synchronizer = alias,
          )
        }
      }

  "Can rollback an unassignment" in { implicit env =>
    import env.*

    val alice = aliceS.toPartyId(participant1)
    val bob = bobS.toPartyId(participant2)

    val cids = Seq(
      createContract(participant1, alice, bob, synchronizerId = Some(acmeId)).toLf,
      createContract(participant1, alice, bob, synchronizerId = Some(acmeId)).toLf,
    )

    val ledgerEnd = participant1.ledger_api.state.end()

    val reassignmentId = participant1.ledger_api.commands
      .submit_unassign(
        alice,
        cids,
        source = acmeId,
        target = daId,
      )
      .reassignmentId

    participant2.ledger_api.updates
      .reassignments(
        partyIds = Set(bob),
        filterTemplates = Seq.empty,
        completeAfter = 1,
        resultFilter = _.isUnassignment,
      )
      .loneElement match {
      case unassigned: UnassignedWrapper =>
        unassigned.reassignmentId shouldBe reassignmentId
      case _ => fail("should not happen")
    }

    participant1.synchronizers.disconnect_all()
    participant2.synchronizers.disconnect_all()
    participant1.repair.rollback_unassignment(reassignmentId, acmeId, daId)
    participant2.repair.rollback_unassignment(reassignmentId, acmeId, daId)

    participant1.synchronizers.reconnect_all()
    participant2.synchronizers.reconnect_all()

    val contractEntry = participant1.ledger_api.state.acs
      .active_contracts_of_party(party = alice)

    contractEntry.map(_.reassignmentCounter) shouldBe Seq(2, 2) // increased by two

    val updates = participant1.ledger_api.updates.reassignments(
      partyIds = Set(alice),
      filterTemplates = Seq.empty,
      completeAfter = 4,
      beginOffsetExclusive = ledgerEnd,
    )

    val assignedEvents = updates.collect { case assigned: AssignedWrapper =>
      assigned
    }

    val (assigned1, assigned2) = assignedEvents match {
      case Seq(a1, a2) => (a1, a2)
      case other => fail(s"Expected 2 assigned events but got: $other")
    }

    val unassignedEvents = updates.collect { case unassigned: UnassignedWrapper =>
      unassigned
    }

    val (unassigned1, unassigned2) = unassignedEvents match {
      case Seq(u1, u2) => (u1, u2)
      case other => fail(s"Expected 2 unassigned events, but got: $other")
    }

    assigned1.target shouldBe daId.logical.toProtoPrimitive
    assigned1.source shouldBe acmeId.logical.toProtoPrimitive
    assigned1.target shouldBe assigned2.source
    unassigned1.target shouldBe unassigned2.source
    unassigned1.source shouldBe unassigned2.target

    // check that we have the correct reassignmentId when we publish the event.
    assigned1.reassignmentId shouldBe reassignmentId

    val (unassigned, _) = participant1.ledger_api.commands.submit_reassign(
      alice,
      cids,
      source = acmeId,
      target = daId,
    )
    unassigned.events.map(_.reassignmentCounter) shouldBe Seq(3, 3)

    val lookup = participant1.underlying.value.sync.syncPersistentStateManager
      .get(daId)
      .value
      .reassignmentStore
      .lookup(ReassignmentId.tryCreate(reassignmentId))
      .value
      .failOnShutdown
      .futureValue

    lookup.isLeft shouldBe true
    lookup.left.value shouldBe a[ReassignmentCompleted]
  }

  "cannot rollback an unknown reassignment" in { implicit env =>
    import env.*

    val reassignmentId = ReassignmentId.tryCreate("0042").toProtoPrimitive
    participant2.synchronizers.disconnect_all()

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant2.repair.rollback_unassignment(reassignmentId, acmeId, daId),
      _.commandFailureMessage should include("unknown reassignment id"),
    )
  }

  "cannot rollback a completed reassignment" in { implicit env =>
    import env.*
    val alice = aliceS.toPartyId(participant1)
    val cid = createContract(participant1, alice, alice, synchronizerId = Some(acmeId))

    val reassignmentId = participant1.ledger_api.commands
      .submit_reassign(
        alice,
        Seq(cid.toLf),
        source = acmeId,
        target = daId,
      )
      ._1
      .reassignmentId

    participant1.synchronizers.disconnect_all()

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.repair.rollback_unassignment(reassignmentId, acmeId, daId),
      _.commandFailureMessage should include("reassignment already completed"),
    )
  }

  "cannot rollback an unassignment twice" in { implicit env =>
    import env.*

    participant1.synchronizers.reconnect_all()

    val alice = aliceS.toPartyId(participant1)
    val cid = createContract(participant1, alice, alice, synchronizerId = Some(acmeId))

    val reassignmentId = participant1.ledger_api.commands
      .submit_unassign(
        alice,
        Seq(cid.toLf),
        source = acmeId,
        target = daId,
      )
      .reassignmentId

    participant1.synchronizers.disconnect_all()
    participant1.repair.rollback_unassignment(reassignmentId, acmeId, daId)
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.repair.rollback_unassignment(reassignmentId, acmeId, daId),
      _.commandFailureMessage should include(s"reassignment already completed"),
    )
  }
}

final class RollbackUnassignmentIntegrationTestPostgres
    extends RollbackUnassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}
