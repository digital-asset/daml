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
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
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
import com.digitalasset.canton.util.ReassignmentTag.Source

sealed trait RollbackUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransform(ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair))
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
          p.synchronizers.connect_local(sequencer2, alias = acmeName)
          p.dars.upload(CantonExamplesPath)
        }

        participant1.parties.enable(
          aliceS,
          synchronizeParticipants = Seq(participant2),
        )
        participant2.parties.enable(
          bobS,
          synchronizeParticipants = Seq(participant1),
        )
      }

  "Can rollback an unassignment" in { implicit env =>
    import env.*

    val alice = aliceS.toPartyId(participant1)
    val bob = bobS.toPartyId(participant2)

    val cid = createContract(participant1, alice, bob, synchronizerId = Some(acmeId))

    val ledgerEnd = participant1.ledger_api.state.end()

    val unassignmentId = participant1.ledger_api.commands
      .submit_unassign(
        alice,
        Seq(cid.toLf),
        source = acmeId,
        target = daId,
      )
      .unassignId

    participant2.ledger_api.updates
      .flat(
        partyIds = Set(bob),
        completeAfter = 1,
        resultFilter = _.isUnassignment,
      )
      .loneElement match {
      case unassigned: UnassignedWrapper =>
        unassigned.unassignId shouldBe unassignmentId
      case _ => fail("should not happen")
    }

    participant1.synchronizers.disconnect_all()
    participant2.synchronizers.disconnect_all()
    participant1.repair.rollback_unassignment(unassignmentId, acmeId, daId)
    participant2.repair.rollback_unassignment(unassignmentId, acmeId, daId)

    participant1.synchronizers.reconnect_all()
    participant2.synchronizers.reconnect_all()

    val contractEntry = participant1.ledger_api.state.acs
      .active_contracts_of_party(party = alice)

    contractEntry.map(_.reassignmentCounter).loneElement shouldBe 2 // increased by two

    val updates = participant1.ledger_api.updates.flat(
      partyIds = Set(alice),
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

    assigned1.target shouldBe daId.toProtoPrimitive
    assigned1.source shouldBe acmeId.toProtoPrimitive
    assigned1.target shouldBe assigned2.source
    unassigned1.target shouldBe unassigned2.source
    unassigned1.source shouldBe unassigned2.target

    // check that we have the correct unassignmentId when we publish the event.
    assigned1.unassignId shouldBe unassignmentId

    val (unassigned, _) = participant1.ledger_api.commands.submit_reassign(
      alice,
      Seq(cid.toLf),
      source = acmeId,
      target = daId,
    )
    unassigned.events.loneElement.reassignmentCounter shouldBe 3

    val reassignmentId =
      ReassignmentId(Source(acmeId), CantonTimestamp.assertFromLong(unassignmentId.toLong))

    val lookup = participant1.underlying.value.sync.syncPersistentStateManager
      .get(daId)
      .value
      .reassignmentStore
      .lookup(reassignmentId)
      .value
      .failOnShutdown
      .futureValue

    lookup.isLeft shouldBe true
    lookup.left.value shouldBe a[ReassignmentCompleted]
  }

  "cannot rollback an unknown reassignment" in { implicit env =>
    import env.*

    val now = CantonTimestamp.now()
    participant2.synchronizers.disconnect_all()

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant2.repair.rollback_unassignment(now.toProtoPrimitive.toString, acmeId, daId),
      _.commandFailureMessage should include("unknown reassignment id"),
    )
  }

  "cannot rollback a completed reassignment" in { implicit env =>
    import env.*
    val alice = aliceS.toPartyId(participant1)
    val cid = createContract(participant1, alice, alice, synchronizerId = Some(acmeId))

    val unassignmentId = participant1.ledger_api.commands
      .submit_reassign(
        alice,
        Seq(cid.toLf),
        source = acmeId,
        target = daId,
      )
      ._1
      .unassignId

    participant1.synchronizers.disconnect_all()

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.repair.rollback_unassignment(unassignmentId, acmeId, daId),
      _.commandFailureMessage should include("reassignment already completed"),
    )
  }

  "cannot rollback an unassignment twice" in { implicit env =>
    import env.*

    participant1.synchronizers.reconnect_all()

    val alice = aliceS.toPartyId(participant1)
    val cid = createContract(participant1, alice, alice, synchronizerId = Some(acmeId))

    val unassignmentId = participant1.ledger_api.commands
      .submit_unassign(
        alice,
        Seq(cid.toLf),
        source = acmeId,
        target = daId,
      )
      .unassignId

    participant1.synchronizers.disconnect_all()
    participant1.repair.rollback_unassignment(unassignmentId, acmeId, daId)
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.repair.rollback_unassignment(unassignmentId, acmeId, daId),
      _.commandFailureMessage should include(s"reassignment already completed"),
    )
  }
}

final class RollbackUnassignmentIntegrationTestPostgres
    extends RollbackUnassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
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
