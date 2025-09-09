// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.examples.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

final class DivulgenceIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  import DivulgenceIntegrationTest.*

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // A party gets activated on multiple participants without being replicated (= ACS mismatch),
  // and we want to minimize the risk of warnings related to acs commitment mismatches
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.local.synchronizers.connect_local(sequencer1, daName)
      participants.local.dars.upload(CantonExamplesPath)
      sequencer1.topology.synchronizer_parameters
        .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

      participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))
      participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))
    }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsSnapshotAtOffset: String =
    "offline_party_replication_test_acs_snapshot_at_offset.gz"

  override def afterAll(): Unit =
    try {
      val exportFile = File(acsSnapshotAtOffset)
      if (exportFile.exists) exportFile.delete()
    } finally super.afterAll()

  "Divulgence should work as expected" in { implicit env =>
    import env.*

    val alice = participant1.parties.find("Alice")
    val bob = participant2.parties.find("Bob")

    // baseline Iou-s to test views / stakeholders / projections on the two participants, and ensure correct party migration baseline
    val (aliceStakeholderCreatedP1, _) = participant1.createIou(alice, alice)
    val (bobStakeholderCreatedP2, _) = participant2.createIou(bob, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 1
      participant2.acsDeltas(bob) should have size 1
    }
    val (aliceBobStakeholderCreatedP1, _) = participant1.createIou(alice, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 2
      participant2.acsDeltas(bob) should have size 2
    }

    // divulgence proxy contract for divulgence operations: divulging to bob
    val (divulgeIouByExerciseP2, divulgeIouByExerciseContract) =
      participant2.createDivulgeIou(alice, bob)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 3
      participant2.acsDeltas(bob) should have size 3
    }

    // creating two iou-s with alice, which will be divulged to bob
    val (immediateDivulged1P1, immediateDivulged1Contract) =
      participant1.immediateDivulgeIou(alice, divulgeIouByExerciseContract)
    val (immediateDivulged2P1, immediateDivulged2Contract) =
      participant1.immediateDivulgeIou(alice, divulgeIouByExerciseContract)
    eventually() {
      //  ensuring that both participants see all events necessary after running the commands (these numbers are deduced from the assertions below)
      participant1.acsDeltas(alice) should have size 5
      participant2.ledgerEffects(alice) should have size 6
    }

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

    val ledgerEndP1 = participant1.ledger_api.state.end()

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2, PP.Submission),
      ),
    )
    participant2.synchronizers.disconnect_all()

    participant1.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = participant2.id,
      beginOffsetExclusive = ledgerEndP1,
      exportFilePath = acsSnapshotAtOffset,
    )

    participant2.repair.import_acs(acsSnapshotAtOffset, "", ContractIdImportMode.Accept)

    participant2.synchronizers.reconnect(daName)

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

    // participant1 bob
    participant1.acsDeltas(bob) shouldBe List(
      aliceBobStakeholderCreatedP1 -> Created,
      divulgeIouByExerciseP1 -> Created,
    )
    participant1.acs(bob) shouldBe List(
      aliceBobStakeholderCreatedP1,
      divulgeIouByExerciseP1,
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

    // the divulged contract should not be visible by the event query service
    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy {
        participant2.ledger_api.event_query
          .by_contract_id(immediateDivulged1P1.contractId, Seq(alice, bob))
      },
      _.commandFailureMessage should include("Contract events not found, or not visible."),
    )
  }
}

object DivulgenceIntegrationTest {
  final case class OffsetCid(offset: Long, contractId: String)
  sealed trait EventType extends Serializable with Product
  case object Created extends EventType
  case object Consumed extends EventType
  case object NonConsumed extends EventType

  implicit class ParticipantSimpleStreamHelper(val participant: LocalParticipantReference)
      extends AnyVal {
    def acs(party: PartyId): Seq[OffsetCid] =
      participant.ledger_api.state.acs
        .active_contracts_of_party(party)
        .flatMap(_.createdEvent)
        .map(c => OffsetCid(c.offset, c.contractId))

    def acsDeltas(partyId: PartyId): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, Seq(partyId))

    def acsDeltas(parties: Seq[PartyId]): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, parties)

    def ledgerEffects(partyId: PartyId): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_LEDGER_EFFECTS, Seq(partyId))

    def eventsWithAcsDelta(parties: Seq[PartyId]): Seq[Event] =
      updatesEvents(TRANSACTION_SHAPE_LEDGER_EFFECTS, parties).filter(_.event match {
        case Event.Event.Created(created) => created.acsDelta
        case Event.Event.Exercised(ex) => ex.acsDelta
        case _ => false
      })

    def createIou(payer: PartyId, owner: PartyId): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) = IouSyntax.createIouComplete(participant)(payer, owner)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def createDivulgeIou(
        payer: PartyId,
        divulgee: PartyId,
    ): (OffsetCid, DivulgeIouByExercise.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.createDivulgeIouByExerciseComplete(participant)(payer, divulgee)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def immediateDivulgeIou(
        payer: PartyId,
        divulgeContract: DivulgeIouByExercise.Contract,
    ): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.immediateDivulgeIouComplete(participant)(payer, divulgeContract)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def retroactiveDivulgeAndArchiveIou(
        payer: PartyId,
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

    def archiveIou(party: PartyId, iou: Iou.Contract): Unit =
      IouSyntax.archive(participant)(iou, party)

    private def updatesEvents(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
    ): Seq[Event] =
      participant.ledger_api.updates
        .updates(
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
            includeReassignments = None,
            includeTopologyEvents = None,
          ),
          completeAfter = PositiveInt.tryCreate(1000000),
          endOffsetInclusive = Some(participant.ledger_api.state.end()),
        )
        .collect { case TransactionWrapper(tx) =>
          tx.events
        }
        .flatten

    private def updates(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
    ): Seq[(OffsetCid, EventType)] =
      updatesEvents(transactionShape = transactionShape, parties = parties)
        .map(_.event)
        .collect {
          case Event.Event.Created(event) => OffsetCid(event.offset, event.contractId) -> Created
          case Event.Event.Archived(event) => OffsetCid(event.offset, event.contractId) -> Consumed
          case Event.Event.Exercised(event) if event.consuming =>
            OffsetCid(event.offset, event.contractId) -> Consumed
          case Event.Event.Exercised(event) if !event.consuming =>
            OffsetCid(event.offset, event.contractId) -> NonConsumed
        }
  }
}
