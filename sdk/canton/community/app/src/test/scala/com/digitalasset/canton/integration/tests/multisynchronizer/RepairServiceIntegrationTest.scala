// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  UnassignedWrapper,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.{ReassignmentCounter, config}
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*

abstract class RepairServiceIntegrationTest
    extends CommunityIntegrationTest
    with AcsInspection
    with SharedEnvironment {

  private val payerName = "payer"
  private val ownerName = "owner"

  private var payer: PartyId = _
  private var owner: PartyId = _

  protected def plugin: EnvironmentSetupPlugin

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S2M1_S2M1
      .withSetup { implicit env =>
        import env.*

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

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer3, alias = acmeName)

        participant1.dars.upload(CantonExamplesPath)

        payer = participant1.parties.enable(payerName)
        owner = participant1.parties.enable(ownerName)
      }

  "repair.change_assignation" when {
    "executing successfully" should {
      "generate reassignment events" in { implicit env =>
        import env.*

        val iou = IouSyntax.createIou(participant1, Some(daId))(payer, owner).id.toLf
        val beforeRepair = participant1.ledger_api.state.end()

        // repair.change_assignation requires to disconnect
        participant1.synchronizers.disconnect_all()

        participant1.repair.change_assignation(Seq(iou), daName, acmeName)

        // reconnecting after repair operations are done
        participant1.synchronizers.reconnect_all()

        // verifying that the contract was successfully reassigned to the "acme" synchronizer
        assertNotInLedgerAcsSync(Seq(participant1), owner, daId, iou)
        assertInLedgerAcsSync(Seq(participant1), owner, acmeId, iou)

        eventually() {
          val afterRepair = participant1.ledger_api.state.end()
          val updates = participant1.ledger_api.updates.flat(
            partyIds = Set(payer),
            completeAfter = Int.MaxValue,
            beginOffsetExclusive = beforeRepair,
            endOffsetInclusive = Some(afterRepair),
          )

          val (assignedEventsO, unassignedEventsO) = updates.collect {
            case t: AssignedWrapper => (t.events, Nil)
            case t: UnassignedWrapper => (Nil, t.events)
          }.unzip

          val unassignedEvents = unassignedEventsO.flatten
          val assignedEvents = assignedEventsO.flatten

          val unassigned =
            unassignedEvents
              .find(_.contractId == iou.coid)
              .valueOrFail(
                s"Unable to find an unassigned event for contract ID ${iou.coid}"
              )

          unassigned.source shouldBe daId.toProtoPrimitive
          unassigned.target shouldBe acmeId.toProtoPrimitive
          unassigned.submitter shouldBe empty

          val assigned =
            assignedEvents
              .find(_.unassignId == unassigned.unassignId)
              .valueOrFail(
                s"Unable to find an assigned event for unassign ID ${unassigned.unassignId}"
              )

          assigned.source shouldBe daId.toProtoPrimitive
          assigned.target shouldBe acmeId.toProtoPrimitive
          assigned.submitter shouldBe empty

        }

      }

      "allow changing the reassignment counter" in { implicit env =>
        import env.*

        val cid = IouSyntax.createIou(participant1)(payer, payer).id.toLf

        participant1.ledger_api.commands.submit_reassign(payer, Seq(cid), daId, acmeId)
        participant1.ledger_api.commands.submit_reassign(payer, Seq(cid), acmeId, daId)
        participant1.ledger_api.commands.submit_reassign(payer, Seq(cid), daId, acmeId)
        participant1.ledger_api.commands.submit_reassign(payer, Seq(cid), acmeId, daId)

        val beforeAssignation = participant1.ledger_api.state.acs
          .active_contracts_of_party(payer)
          .filter(_.createdEvent.value.contractId == cid.coid)
          .loneElement

        beforeAssignation.synchronizerId shouldBe daId.toProtoPrimitive
        beforeAssignation.reassignmentCounter shouldBe 4

        participant1.synchronizers.disconnect_all()
        participant1.repair.change_assignation(
          Seq(cid),
          daName,
          acmeName,
          reassignmentCounterOverride = Map(cid -> ReassignmentCounter(2)),
        )

        // Because we changed the reassignment counter of a contract, the running commitments will not match the ACS
        // TODO(#23735) Add here the sequence of repair commands that bring the system into a consistent state
        //  and remove the suppression of ACS_COMMITMENT_INTERNAL_ERROR
        val expectedLogs: Seq[(LogEntryOptionality, LogEntry => Assertion)] = Seq(
          (
            LogEntryOptionality.OptionalMany,
            { entry =>
              entry.errorMessage should (include("ACS_COMMITMENT_INTERNAL_ERROR") and include(
                "Detected an inconsistency between the running commitment and the ACS"
              ))
              entry.loggerName should include regex s"AcsCommitmentProcessor.*participant1"
            },
          )
        )

        loggerFactory.assertLogsUnorderedOptional(
          {
            participant1.synchronizers.reconnect_all()

            val afterAssignation = participant1.ledger_api.state.acs
              .active_contracts_of_party(payer)
              .filter(_.createdEvent.value.contractId == cid.coid)
              .loneElement

            afterAssignation.synchronizerId shouldBe acmeId.toProtoPrimitive
            afterAssignation.reassignmentCounter shouldBe 2

            val archiveCmd = participant1.ledger_api.javaapi.state.acs
              .await(Iou.COMPANION)(payer, predicate = _.id.toLf == cid)
              .id
              .exerciseArchive()
              .commands()
              .asScala
              .toSeq
            participant1.ledger_api.javaapi.commands.submit(Seq(payer), archiveCmd)
          },
          expectedLogs *,
        )
      }
    }
  }
}

class ReferenceRepairServiceIntegrationTest extends RepairServiceIntegrationTest {
  override protected lazy val plugin =
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"), InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3"), InstanceName.tryCreate("sequencer4")),
        )
      ),
    )
}
