// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.examples.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.topology.{Party, PartyId}
import monocle.macros.syntax.lens.*
import org.scalatest.OptionValues.*

trait DivulgenceIntegrationTest extends OfflinePartyReplicationIntegrationTestBase {

  import DivulgenceIntegrationTest.*

  // Whether to use Assign/Unassign (multi-synchronizer) or Create/Archive for the ACS import
  def alphaMultiSynchronizerSupport: Boolean

  // Inject this setting into the implicit scope for the helper class
  implicit def alphaSupportImplicit: Boolean = alphaMultiSynchronizerSupport

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.alphaMultiSynchronizerSupport).replace(alphaMultiSynchronizerSupport)
      )
    )

  "Divulgence should work as expected" in { implicit env =>
    import env.*

    def contractStore(participant: LocalParticipantReference) =
      participant.testing.state_inspection.syncPersistentStateManager
        .acsInspection(daId)
        .map(_.contractStore)
        .value
    def contractFor(
        participant: LocalParticipantReference,
        contractId: String,
    ): Option[ContractInstance] =
      contractStore(participant)
        .lookup(LfContractId.assertFromString(contractId))
        .value
        .futureValueUS

    def checkCreatedEventFor(
        participant: LocalParticipantReference,
        contractId: String,
        party: Party,
    ) = participant.ledger_api.javaapi.event_query
      .by_contract_id(contractId, Seq(party))
      .hasCreated shouldBe true

    def checkArchivedEventFor(
        participant: LocalParticipantReference,
        contractId: String,
        party: Party,
    ) = {
      checkCreatedEventFor(participant, contractId, party) // ensure created event exists
      participant.ledger_api.javaapi.event_query
        .by_contract_id(contractId, Seq(party))
        .hasArchived shouldBe true
    }

    // the divulged contract should not be visible by the event query service
    def assertEventNotFound(
        participant: LocalParticipantReference,
        contractId: String,
        party: Party,
    ) = loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy {
        participant.ledger_api.event_query
          .by_contract_id(contractId, Seq(party))
      },
      _.commandFailureMessage should include("Contract events not found, or not visible."),
    )

    // With enabled multi-synchronizer support, imported contracts appear as Assignments (hidden from GetEvents).
    // Otherwise, they appear as "Created" events (visible).
    def checkImportedEvent(
        participant: LocalParticipantReference,
        contractId: String,
        party: Party,
    ) = if (alphaMultiSynchronizerSupport) {
      assertEventNotFound(participant, contractId, party)
    } else {
      checkCreatedEventFor(participant, contractId, party)
    }

    // baseline Iou-s to test views / stakeholders / projections on the two participants, and ensure correct party migration baseline
    val (aliceStakeholderCreatedP1, _) = participant1.createIou(alice, alice)
    val (bobStakeholderCreatedP2, _) = participant2.createIou(bob, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 1
      participant2.acsDeltas(bob) should have size 1
    }
    contractFor(participant1, aliceStakeholderCreatedP1.contractId) should not be empty
    contractFor(participant1, bobStakeholderCreatedP2.contractId) shouldBe empty
    contractFor(participant2, aliceStakeholderCreatedP1.contractId) shouldBe empty
    contractFor(participant2, bobStakeholderCreatedP2.contractId) should not be empty

    val (aliceBobStakeholderCreatedP1, _) = participant1.createIou(alice, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 2
      participant2.acsDeltas(bob) should have size 2
    }
    contractFor(participant1, aliceBobStakeholderCreatedP1.contractId) should not be empty
    contractFor(participant2, aliceBobStakeholderCreatedP1.contractId) should not be empty

    // divulgence proxy contract for divulgence operations: divulging to bob
    val (divulgeIouByExerciseP2, divulgeIouByExerciseContract) =
      participant2.createDivulgeIou(alice, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 3
      participant2.acsDeltas(bob) should have size 3
    }
    contractFor(participant1, divulgeIouByExerciseP2.contractId) should not be empty
    contractFor(participant2, divulgeIouByExerciseP2.contractId) should not be empty

    // creating two iou-s with alice, which will be divulged to bob
    val (immediateDivulged1P1, immediateDivulged1Contract) =
      participant1.immediateDivulgeIou(alice, divulgeIouByExerciseContract)
    val (immediateDivulged2P1, _immediateDivulged2Contract) =
      participant1.immediateDivulgeIou(alice, divulgeIouByExerciseContract)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 5
      participant2.ledgerEffects(alice) should have size 6
    }
    contractFor(participant1, immediateDivulged1P1.contractId) should not be empty
    // Immediately divulged contracts are stored in the ContractStore
    contractFor(participant2, immediateDivulged1P1.contractId) should not be empty
    contractFor(participant1, immediateDivulged2P1.contractId) should not be empty
    // Immediately divulged contracts are stored in the ContractStore
    contractFor(participant2, immediateDivulged2P1.contractId) should not be empty

    // archiving the first divulged Iou
    participant1.archiveIou(alice, immediateDivulged1Contract)

    // create and then retroactively divulge the archival of an iou visible exclusively to alice
    val (aliceStakeholderCreated2P1, aliceStakeholderCreated2Contract) =
      participant1.createIou(alice, alice)
    val aliceStakeholderCreated2P1Archived = participant1.retroactiveDivulgeAndArchiveIou(
      alice,
      divulgeIouByExerciseContract,
      aliceStakeholderCreated2Contract.id,
    )
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 8
      participant2.ledgerEffects(alice) should have size 8
    }
    contractFor(participant1, aliceStakeholderCreated2P1.contractId) should not be empty
    // Retroactively divulged contracts are not stored in the ContractStore
    contractFor(participant2, aliceStakeholderCreated2P1.contractId) shouldBe empty

    // participant1 alice
    val divulgeIouByExerciseP1 = participant1.acsDeltas(alice)(2)._1
    divulgeIouByExerciseP1.contractId shouldBe divulgeIouByExerciseP2.contractId
    val immediateDivulged1ArchiveP1 = participant1.acsDeltas(alice)(5)._1
    immediateDivulged1ArchiveP1.contractId shouldBe immediateDivulged1P1.contractId
    aliceStakeholderCreated2P1Archived.contractId shouldBe aliceStakeholderCreated2P1.contractId
    participant1.acsDeltas(alice) shouldBe List(
      aliceStakeholderCreatedP1 -> Created,
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
      immediateDivulged1P1 -> Created,
      immediateDivulged2P1 -> Created,
      immediateDivulged1ArchiveP1 -> Consumed,
      aliceStakeholderCreated2P1 -> Created,
      aliceStakeholderCreated2P1Archived -> Consumed,
    )
    participant1.acs(alice) shouldBe List(
      aliceStakeholderCreatedP1,
      aliceBobStakeholderCreatedP1,
      divulgeIouByExerciseP1,
      immediateDivulged2P1,
    )
    participant1.ledgerEffects(alice) shouldBe List(
      aliceStakeholderCreatedP1 -> Created,
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
      immediateDivulged1P1.copy(contractId = divulgeIouByExerciseP1.contractId) -> NonConsumed,
      immediateDivulged1P1 -> Created,
      immediateDivulged2P1.copy(contractId = divulgeIouByExerciseP1.contractId) -> NonConsumed,
      immediateDivulged2P1 -> Created,
      immediateDivulged1ArchiveP1 -> Consumed,
      aliceStakeholderCreated2P1 -> Created,
      aliceStakeholderCreated2P1Archived.copy(contractId =
        divulgeIouByExerciseP1.contractId
      ) -> NonConsumed,
      aliceStakeholderCreated2P1Archived -> Consumed,
    )
    // the number of events with acs_delta field set should match the number of ACS deltas
    participant1.eventsWithAcsDelta(Seq.empty).size shouldBe participant1.acsDeltas(Seq.empty).size
    participant1.acsDeltas(Seq.empty) shouldBe participant1.acsDeltas(Seq(alice, bob))
    participant1.acsDeltas(Seq.empty) shouldBe participant1.acsDeltas(Seq(alice))

    // participant2 alice
    val aliceBobStakeholderCreatedP2 = participant2.acsDeltas(alice).headOption.value._1
    aliceBobStakeholderCreatedP2.contractId shouldBe aliceBobStakeholderCreatedP1.contractId
    participant2.acsDeltas(alice) shouldBe List(
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
    )
    participant2.acs(alice) shouldBe List(
      aliceBobStakeholderCreatedP2,
      divulgeIouByExerciseP2,
    )
    val immediateDivulged1P2 = participant2.ledgerEffects(alice)(3)._1
    immediateDivulged1P2.contractId shouldBe immediateDivulged1P1.contractId
    val immediateDivulged2P2 = participant2.ledgerEffects(alice)(5)._1
    immediateDivulged2P2.contractId shouldBe immediateDivulged2P1.contractId
    val aliceStakeholder2DivulgedArchiveP2 = participant2.ledgerEffects(alice)(7)._1
    aliceStakeholder2DivulgedArchiveP2.contractId shouldBe aliceStakeholderCreated2P1Archived.contractId
    participant2.ledgerEffects(alice) shouldBe List(
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
      // two events for the immediate divulgence follows with the same offset: first the nonconsuming exercise, and then the immediately divulged create
      OffsetCid(immediateDivulged1P2.offset, divulgeIouByExerciseP2.contractId) -> NonConsumed,
      immediateDivulged1P2 -> Created,
      // two events for the immediate divulgence follows with the same offset: first the nonconsuming exercise, and then the immediately divulged create
      OffsetCid(immediateDivulged2P2.offset, divulgeIouByExerciseP2.contractId) -> NonConsumed,
      immediateDivulged2P2 -> Created,
      aliceStakeholder2DivulgedArchiveP2.copy(contractId =
        divulgeIouByExerciseP2.contractId
      ) -> NonConsumed,
      aliceStakeholder2DivulgedArchiveP2 -> Consumed,
    )

    // participant1 bob
    participant1.acsDeltas(bob) shouldBe List(
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
    )
    participant1.acs(bob) shouldBe List(
      aliceBobStakeholderCreatedP1,
      divulgeIouByExerciseP1,
    )
    participant1.ledgerEffects(bob) shouldBe List(
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
      // two events for the immediate divulgence follows with the same offset: first the nonconsuming exercise, and then the immediately divulged create
      OffsetCid(immediateDivulged1P1.offset, divulgeIouByExerciseP1.contractId) -> NonConsumed,
      immediateDivulged1P1 -> Created,
      // two events for the immediate divulgence follows with the same offset: first the nonconsuming exercise, and then the immediately divulged create
      OffsetCid(immediateDivulged2P1.offset, divulgeIouByExerciseP2.contractId) -> NonConsumed,
      immediateDivulged2P1 -> Created,
      aliceStakeholderCreated2P1Archived.copy(contractId =
        divulgeIouByExerciseP1.contractId
      ) -> NonConsumed,
      aliceStakeholderCreated2P1Archived -> Consumed,
    )

    // participant2 bob
    participant2.acsDeltas(bob) shouldBe List(
      bobStakeholderCreatedP2 -> Created,
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
    )
    participant2.acs(bob) shouldBe List(
      bobStakeholderCreatedP2,
      aliceBobStakeholderCreatedP2,
      divulgeIouByExerciseP2,
    )
    participant2.ledgerEffects(bob) shouldBe List(
      bobStakeholderCreatedP2 -> Created,
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
      immediateDivulged1P2.copy(contractId = divulgeIouByExerciseP2.contractId) -> NonConsumed,
      immediateDivulged1P2 -> Created,
      immediateDivulged2P2.copy(contractId = divulgeIouByExerciseP2.contractId) -> NonConsumed,
      immediateDivulged2P2 -> Created,
      aliceStakeholder2DivulgedArchiveP2.copy(contractId =
        divulgeIouByExerciseP2.contractId
      ) -> NonConsumed,
      aliceStakeholder2DivulgedArchiveP2 -> Consumed,
    )
    // the number of events with acs_delta field set should match the number of ACS deltas
    participant2.eventsWithAcsDelta(Seq.empty).size shouldBe participant2.acsDeltas(Seq.empty).size
    participant2.acsDeltas(Seq.empty) shouldBe participant2.acsDeltas(Seq(alice, bob))
    participant2.acsDeltas(Seq.empty) shouldBe participant2.acsDeltas(Seq(bob))

    val source = participant1
    val target = participant2
    val beforeActivationOffset = authorizeAliceWithTargetDisconnect(daId, source, target)

    // Replicate `alice` from `source` (`participant1`) to `target` (`participant2`)
    source.parties.export_party_acs(
      alice,
      daId,
      target,
      beforeActivationOffset,
      acsSnapshotPath,
    )
    target.parties.import_party_acsV2(acsSnapshotPath, daId)

    target.synchronizers.reconnect(daName)

    // participant1 alice
    participant1.acsDeltas(alice) shouldBe List(
      aliceStakeholderCreatedP1 -> Created,
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
      immediateDivulged1P1 -> Created,
      immediateDivulged2P1 -> Created,
      immediateDivulged1ArchiveP1 -> Consumed,
      aliceStakeholderCreated2P1 -> Created,
      aliceStakeholderCreated2P1Archived -> Consumed,
    )
    participant1.acs(alice) shouldBe List(
      aliceStakeholderCreatedP1,
      aliceBobStakeholderCreatedP1,
      divulgeIouByExerciseP1,
      immediateDivulged2P1,
    )
    // event query
    checkCreatedEventFor(participant1, aliceStakeholderCreatedP1.contractId, alice)
    checkCreatedEventFor(participant1, aliceBobStakeholderCreatedP1.contractId, alice)
    checkCreatedEventFor(participant1, divulgeIouByExerciseP1.contractId, alice)
    checkCreatedEventFor(participant1, immediateDivulged1P1.contractId, alice)
    checkCreatedEventFor(participant1, immediateDivulged2P1.contractId, alice)
    checkArchivedEventFor(participant1, immediateDivulged1ArchiveP1.contractId, alice)
    assertEventNotFound(participant1, bobStakeholderCreatedP2.contractId, alice)
    checkCreatedEventFor(participant1, aliceBobStakeholderCreatedP2.contractId, alice)
    checkCreatedEventFor(participant1, divulgeIouByExerciseP2.contractId, alice)
    checkCreatedEventFor(participant1, aliceStakeholderCreated2P1.contractId, alice)
    checkArchivedEventFor(participant1, aliceStakeholderCreated2P1Archived.contractId, alice)

    // participant2 alice
    val aliceStakeholderCreatedP2Import = participant2.acsDeltas(alice)(2)._1
    aliceStakeholderCreatedP2Import.contractId shouldBe aliceStakeholderCreatedP1.contractId
    val immediateDivulged2P2Import = participant2.acsDeltas(alice)(3)._1
    immediateDivulged2P2Import.contractId shouldBe immediateDivulged2P1.contractId
    participant2.acsDeltas(alice) shouldBe List(
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
      aliceStakeholderCreatedP2Import -> Created,
      immediateDivulged2P2Import -> Created,
    )
    participant2.acs(alice) shouldBe List(
      aliceBobStakeholderCreatedP2,
      divulgeIouByExerciseP2,
      aliceStakeholderCreatedP2Import,
      immediateDivulged2P2Import,
    )
    // event query
    // Pure import (Created on P1, P2 never saw it):
    // Multi-Sync: Hidden (Assignment). Legacy: Visible (Created).
    checkImportedEvent(participant2, aliceStakeholderCreatedP1.contractId, alice)

    // Shared import (Created on P1, Bob on P2 witnessed it):
    // Visible in both modes because P2 has the event history from Bob.
    checkCreatedEventFor(participant2, aliceBobStakeholderCreatedP1.contractId, alice)

    // Local events (Created on P2): Visible.
    checkCreatedEventFor(participant2, divulgeIouByExerciseP1.contractId, alice)
    assertEventNotFound(participant2, immediateDivulged1P1.contractId, alice)

    // Divulged import (Created on P1, divulged to P2 Bob):
    // Multi-Sync: Hidden (Divulgence != Stakeholder Create). Legacy: Visible.
    checkImportedEvent(participant2, immediateDivulged2P1.contractId, alice)

    assertEventNotFound(participant2, immediateDivulged1ArchiveP1.contractId, alice)
    assertEventNotFound(participant2, bobStakeholderCreatedP2.contractId, alice)
    checkCreatedEventFor(participant2, aliceBobStakeholderCreatedP2.contractId, alice)
    checkCreatedEventFor(participant2, divulgeIouByExerciseP2.contractId, alice)
    assertEventNotFound(participant2, aliceStakeholderCreated2P1.contractId, alice)
    assertEventNotFound(participant2, aliceStakeholderCreated2P1Archived.contractId, alice)

    // participant1 bob
    participant1.acsDeltas(bob) shouldBe List(
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
    )
    participant1.acs(bob) shouldBe List(
      aliceBobStakeholderCreatedP1,
      divulgeIouByExerciseP1,
    )
    // event query
    assertEventNotFound(participant1, aliceStakeholderCreatedP1.contractId, bob)
    checkCreatedEventFor(participant1, aliceBobStakeholderCreatedP1.contractId, bob)
    checkCreatedEventFor(participant1, divulgeIouByExerciseP1.contractId, bob)
    assertEventNotFound(participant1, immediateDivulged1P1.contractId, bob)
    assertEventNotFound(participant1, immediateDivulged2P1.contractId, bob)
    assertEventNotFound(participant1, immediateDivulged1ArchiveP1.contractId, bob)
    assertEventNotFound(participant1, bobStakeholderCreatedP2.contractId, bob)
    checkCreatedEventFor(participant1, aliceBobStakeholderCreatedP2.contractId, bob)
    checkCreatedEventFor(participant1, divulgeIouByExerciseP2.contractId, bob)
    assertEventNotFound(participant1, aliceStakeholderCreated2P1.contractId, bob)
    assertEventNotFound(participant1, aliceStakeholderCreated2P1Archived.contractId, bob)

    // participant2 bob
    participant2.acsDeltas(bob) shouldBe List(
      bobStakeholderCreatedP2 -> Created,
      aliceBobStakeholderCreatedP2 -> Created,
      divulgeIouByExerciseP2 -> Created,
    )
    participant2.acs(bob) shouldBe List(
      bobStakeholderCreatedP2,
      aliceBobStakeholderCreatedP2,
      divulgeIouByExerciseP2,
    )
    // event query
    assertEventNotFound(participant2, aliceStakeholderCreatedP1.contractId, bob)
    checkCreatedEventFor(participant2, aliceBobStakeholderCreatedP1.contractId, bob)
    checkCreatedEventFor(participant2, divulgeIouByExerciseP1.contractId, bob)
    assertEventNotFound(participant2, immediateDivulged1P1.contractId, bob)
    assertEventNotFound(participant2, immediateDivulged2P1.contractId, bob)
    assertEventNotFound(participant2, immediateDivulged1ArchiveP1.contractId, bob)
    checkCreatedEventFor(participant2, bobStakeholderCreatedP2.contractId, bob)
    checkCreatedEventFor(participant2, aliceBobStakeholderCreatedP2.contractId, bob)
    checkCreatedEventFor(participant2, divulgeIouByExerciseP2.contractId, bob)
    assertEventNotFound(participant2, aliceStakeholderCreated2P1.contractId, bob)
    assertEventNotFound(participant2, aliceStakeholderCreated2P1Archived.contractId, bob)
  }
}

object DivulgenceIntegrationTest {
  final case class OffsetCid(offset: Long, contractId: String)
  sealed trait EventType extends Serializable with Product
  case object Created extends EventType
  case object Consumed extends EventType
  case object NonConsumed extends EventType

  implicit class ParticipantSimpleStreamHelper(val participant: LocalParticipantReference)(implicit
      val alphaMultiSynchronizerSupport: Boolean = false
  ) {

    def acs(party: Party): Seq[OffsetCid] =
      participant.ledger_api.state.acs
        .active_contracts_of_party(party)
        .flatMap(_.createdEvent)
        .map(c => OffsetCid(c.offset, c.contractId))

    def acsDeltas(
        party: Party,
        beginOffsetExclusive: Long = 0L,
    ): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, Seq(party.partyId), beginOffsetExclusive)

    def acsDeltas(parties: Seq[Party]): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, parties.map(_.partyId), 0L)

    def ledgerEffects(
        party: Party,
        beginOffsetExclusive: Long = 0L,
    ): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_LEDGER_EFFECTS, Seq(party.partyId), beginOffsetExclusive)

    def eventsWithAcsDelta(parties: Seq[PartyId]): Seq[Event] =
      updatesEvents(TRANSACTION_SHAPE_LEDGER_EFFECTS, parties, 0L).collect {
        case TransactionWrapper(tx) =>
          tx.events.filter(_.event match {
            case Event.Event.Created(created) => created.acsDelta
            case Event.Event.Exercised(ex) => ex.acsDelta
            case _ => false
          })
        case assigned: UpdateService.AssignedWrapper =>
          assigned.events.flatMap(_.createdEvent).collect {
            case createdEvent if createdEvent.acsDelta =>
              Event(Event.Event.Created(createdEvent))
          }
      }.flatten

    def createIou(payer: Party, owner: Party): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) = IouSyntax.createIouComplete(participant)(payer, owner)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def createDivulgeIou(
        payer: Party,
        divulgee: Party,
    ): (OffsetCid, DivulgeIouByExercise.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.createDivulgeIouByExerciseComplete(participant)(payer, divulgee)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def immediateDivulgeIou(
        payer: Party,
        divulgeContract: DivulgeIouByExercise.Contract,
    ): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.immediateDivulgeIouComplete(participant)(payer, divulgeContract)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def retroactiveDivulgeAndArchiveIou(
        payer: Party,
        divulgeContract: DivulgeIouByExercise.Contract,
        iouContractId: Iou.ContractId,
    ): OffsetCid = {
      val (transaction, _) =
        IouSyntax.retroactiveDivulgeAndArchiveIouComplete(participant)(
          payer,
          divulgeContract,
          iouContractId,
        )
      OffsetCid(transaction.offset, iouContractId.contractId)
    }

    def archiveIou(party: Party, iou: Iou.Contract): Unit =
      IouSyntax.archive(participant)(iou, party)

    private def updatesEvents(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
        beginOffsetExclusive: Long,
    ): Seq[UpdateService.UpdateWrapper] = {
      val reassignmentsFilter = if (alphaMultiSynchronizerSupport) {
        Some(
          EventFormat(
            filtersByParty = parties.map(party => party.toLf -> Filters(Nil)).toMap,
            filtersForAnyParty = if (parties.isEmpty) Some(Filters(Nil)) else None,
            verbose = false,
          )
        )
      } else None

      participant.ledger_api.updates.updates(
        updateFormat = UpdateFormat(
          includeTransactions = Some(
            TransactionFormat(
              eventFormat = Some(
                EventFormat(
                  filtersByParty = parties.map(party => party.toLf -> Filters(Nil)).toMap,
                  filtersForAnyParty = if (parties.isEmpty) Some(Filters(Nil)) else None,
                  verbose = false,
                )
              ),
              transactionShape = transactionShape,
            )
          ),
          includeReassignments = reassignmentsFilter,
          includeTopologyEvents = None,
        ),
        completeAfter = PositiveInt.tryCreate(1000000),
        endOffsetInclusive = Some(participant.ledger_api.state.end()),
        beginOffsetExclusive = beginOffsetExclusive,
      )
    }

    private def updates(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
        beginOffsetExclusive: Long,
    ): Seq[(OffsetCid, EventType)] =
      updatesEvents(transactionShape, parties, beginOffsetExclusive).collect {
        case TransactionWrapper(tx) =>
          tx.events.map(_.event).collect {
            case Event.Event.Created(event) =>
              OffsetCid(event.offset, event.contractId) -> Created
            case Event.Event.Archived(event) =>
              OffsetCid(event.offset, event.contractId) -> Consumed
            case Event.Event.Exercised(event) if event.consuming =>
              OffsetCid(event.offset, event.contractId) -> Consumed
            case Event.Event.Exercised(event) if !event.consuming =>
              OffsetCid(event.offset, event.contractId) -> NonConsumed
          }
        case assigned: UpdateService.AssignedWrapper =>
          assigned.events.map { event =>
            OffsetCid(assigned.reassignment.offset, event.createdEvent.value.contractId) -> Created
          }
        case unassigned: UpdateService.UnassignedWrapper =>
          unassigned.events.map { event =>
            OffsetCid(unassigned.reassignment.offset, event.contractId) -> Consumed
          }
      }.flatten
  }
}

trait DivulgenceIntegrationTestWithoutCache extends DivulgenceIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(
      ConfigTransforms.updateAllParticipantConfigs { (_: String, c: ParticipantNodeConfig) =>
        c
          .focus(_.ledgerApi.userManagementService.enabled)
          .replace(true)
          .focus(_.ledgerApi.userManagementService.maxCacheSize)
          .replace(0)
          .focus(_.ledgerApi.indexService.maxContractKeyStateCacheSize)
          .replace(0)
          .focus(_.ledgerApi.indexService.maxContractStateCacheSize)
          .replace(0)
          .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
          .replace(0)
      }
    )
}

class DivulgenceIntegrationTestReassignmentWithCache extends DivulgenceIntegrationTest {
  override def alphaMultiSynchronizerSupport: Boolean = true
}

class DivulgenceIntegrationTestLegacyWithCache extends DivulgenceIntegrationTest {
  override def alphaMultiSynchronizerSupport: Boolean = false
}

class DivulgenceIntegrationTestReassignmentWithoutCache
    extends DivulgenceIntegrationTestWithoutCache {
  override def alphaMultiSynchronizerSupport: Boolean = true
}

class DivulgenceIntegrationTestLegacyWithoutCache extends DivulgenceIntegrationTestWithoutCache {
  override def alphaMultiSynchronizerSupport: Boolean = false
}
