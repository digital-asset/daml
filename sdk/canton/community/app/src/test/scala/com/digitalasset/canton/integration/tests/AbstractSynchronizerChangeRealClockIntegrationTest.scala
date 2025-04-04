// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.reassignment.UnassignedEvent
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.examples.java.paint.OfferToPaintHouseByOwner
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import org.scalatest.Assertion

import scala.concurrent.duration.Duration

abstract class AbstractSynchronizerChangeRealClockIntegrationTest
    extends SynchronizerChangeIntegrationTest(
      SynchronizerChangeIntegrationTest.Config(
        simClock = false,
        assignmentExclusivityTimeout = NonNegativeFiniteDuration.Zero,
      )
    ) {

  protected def setupAndUnassign(alice: PartyId, bank: PartyId, painter: PartyId)(implicit
      env: TestConsoleEnvironment
  ): UnassignedEvent = {
    import env.*
    val iou = createIou(alice, bank, painter)
    val iouId = iou.id.toLf

    // paintOfferId <- submit alice do
    //   create $ OfferToPaintHouseByOwner with painter = painter; houseOwner = alice; bank = bank; iouId = iouId
    // This time, let P5 create it on the paint synchronizer
    val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
    P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))
    val paintOfferId =
      searchAcsSync(
        Seq(P4, P5),
        paintSynchronizerAlias.unwrap,
        PaintModule,
        OfferToPaintHouseByOwnerTemplate,
      )
    assertInLedgerAcsSync(Seq(P5), alice, paintOfferId)
    assertInLedgerAcsSync(Seq(P4), painter, paintOfferId)

    assertSizeOfAcs(Seq(P1), iouSynchronizerAlias, s"^$IouModule", 1, Some(bank))
    assertSizeOfAcs(
      Seq(P2, P5),
      iouSynchronizerAlias,
      s"^$IouModule",
      1,
      Some(alice),
    )
    assertSizeOfAcs(
      Seq(P3, P4),
      iouSynchronizerAlias,
      s"^$PaintModule",
      0,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P4),
      paintSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P5),
      paintSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(alice),
    )

    logger.info("Painter reassignments the paint offer to the IOU synchronizer.")

    val paintOfferReassignmentId =
      P4.ledger_api.commands.submit_unassign(
        painter,
        paintOfferId,
        paintSynchronizerId,
        iouSynchronizerId,
      )

    // Verify ACS
    // Do not verify ledger ACS, as the ledger API server is currently not informed about reassignments
    assertNotInAcsSync(
      Seq(P4, P5),
      paintSynchronizerAlias,
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
    )
    assertNotInAcsSync(
      Seq(P2, P3, P4, P5),
      iouSynchronizerAlias,
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
      Duration.Zero,
    )

    checkUnassignment(paintSynchronizerId, iouSynchronizerId, painter)

    paintOfferReassignmentId.unassignedEvent
  }

  protected def assignmentAndPaintOfferAcceptance(
      alice: PartyId,
      bank: PartyId,
      painter: PartyId,
      paintOfferUnassignedEvent: UnassignedEvent,
  )(implicit env: TestConsoleEnvironment): Assertion = {

    clue(s"running assignment on ${paintOfferUnassignedEvent.contractId}") {
      P4.ledger_api.commands.submit_assign(
        painter,
        paintOfferUnassignedEvent.unassignId,
        paintSynchronizerId,
        iouSynchronizerId,
      )
    }
    val paintOfferId = LfContractId.assertFromString(paintOfferUnassignedEvent.contractId)
    checkReassignmentStoreIsEmpty(paintSynchronizerId, Seq(P4, P5), painter)

    logger.info(s"Checking that the reassigned contract $paintOfferId is in the ACS")

    assertInAcsSync(Seq(P2, P3, P4, P5), iouSynchronizerAlias, paintOfferId)
    assertInLedgerAcsSync(Seq(P2, P5), alice, iouSynchronizerId, paintOfferId)
    assertInLedgerAcsSync(Seq(P3, P4), painter, iouSynchronizerId, paintOfferId)

    val (painterIouId, paintHouseId) = acceptPaintOffer(alice, bank, painter, paintOfferId)

    callIou(alice, bank, painter, painterIouId)

    logger.info("Alice reassigns the PaintAgreement to the Paint synchronizer")

    val paintHouseUnassigned =
      P5.ledger_api.commands.submit_unassign(
        alice,
        paintHouseId,
        iouSynchronizerId,
        paintSynchronizerId,
      )

    assertNotInAcsSync(
      Seq(P2, P5),
      iouSynchronizerAlias,
      PaintModule,
      PaintHouseTemplate,
      stakeholder = Some(alice),
    )
    assertNotInAcsSync(
      Seq(P3, P4),
      iouSynchronizerAlias,
      PaintModule,
      PaintHouseTemplate,
      stakeholder = Some(painter),
    )

    assertNotInAcsSync(
      Seq(P4, P5),
      paintSynchronizerAlias,
      PaintModule,
      PaintHouseTemplate,
      Duration.Zero,
      Some(alice),
    )

    checkUnassignment(iouSynchronizerId, paintSynchronizerId, painter)

    P4.ledger_api.commands.submit_assign(
      painter,
      paintHouseUnassigned.unassignedEvent.unassignId,
      iouSynchronizerId,
      paintSynchronizerId,
    )

    checkReassignmentStoreIsEmpty(paintSynchronizerId, Seq(P4, P5), painter)

    assertInAcsSync(Seq(P4, P5), paintSynchronizerAlias, paintHouseId)
    assertInLedgerAcsSync(Seq(P5), alice, paintSynchronizerId, paintHouseId)
    assertInLedgerAcsSync(Seq(P4), painter, paintSynchronizerId, paintHouseId)

    // Test that all four participants are still alive
    // This runs over the Iou synchronizer because we involve P2 and P3
    assertPingSucceeds(P2, P5)
    assertPingSucceeds(P3, P4)

    // Test P4 and P5 are alive on the Paint synchronizer
    assertPingSucceeds(P4, P5, synchronizerId = Some(paintSynchronizerId))
    assertPingSucceeds(P5, P4, synchronizerId = Some(paintSynchronizerId))
  }

  protected def checkUnassignment(source: SynchronizerId, target: SynchronizerId, party: PartyId)(
      implicit env: TestConsoleEnvironment
  ): Unit =
    for (reassignmentParticipantRef <- Seq(P4, P5)) {
      withClue(s"For participant ${reassignmentParticipantRef.name}") {
        eventually() {
          reassignmentParticipantRef.ledger_api.state.acs
            .incomplete_unassigned_of_party(party)
            .filter { event =>
              event.source == source && event.target == target
            }
            .loneElement
        }
      }
    }

  protected def acceptPaintOffer(
      alice: PartyId,
      bank: PartyId,
      painter: PartyId,
      paintOfferId: LfContractId,
  )(implicit
      env: TestConsoleEnvironment
  ): (LfContractId, LfContractId) = {
    import env.*
    logger.info("Painter accepts the paint offer")
    val paintPackageId = findPackageIdOf(PaintModule)

    // construct the Iou disclosed contract: get the Iou's contract ID in question from the offer, and get the Iou event blob from the ACS accordingly
    val paintOffer = P2.ledger_api.javaapi.state.acs
      .filter(OfferToPaintHouseByOwner.COMPANION)(alice)
      .find(_.id.toLf == paintOfferId)
    paintOffer should not be empty
    val iouTemplateId = TemplateId.templateIdsFromJava(Iou.TEMPLATE_ID_WITH_PACKAGE_ID).head
    val iouCreated = P1.ledger_api.state.acs
      .of_party(
        party = bank,
        filterTemplates = Seq(iouTemplateId),
        includeCreatedEventBlob = true,
      )
      .find(_.event.contractId == paintOffer.value.data.iouId.contractId)
    iouCreated should not be (empty)
    val iouCreatedEventBlob = iouCreated.value.event.createdEventBlob
    val iouDisclosed = DisclosedContract(
      templateId = Some(iouTemplateId.toIdentifier),
      contractId = iouCreated.value.contractId,
      createdEventBlob = iouCreatedEventBlob,
      synchronizerId = "",
    )

    // painterIouId <- submit painter do
    //   exercise offerId AcceptByPainter
    val acceptCmd = ledger_api_utils.exercise(
      paintPackageId,
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
      "AcceptByPainter",
      Map.empty,
      paintOfferId.coid,
    )
    clue("accept by painter") {
      P3.ledger_api.commands.submit(
        actAs = Seq(painter),
        commands = Seq(acceptCmd),
        disclosedContracts = Seq(iouDisclosed),
      )
    }
    val paintHouseId =
      searchAcsSync(
        Seq(P2, P3, P4, P5),
        iouSynchronizerAlias.unwrap,
        PaintModule,
        PaintHouseTemplate,
        stakeholder = Some(alice),
      )
    assertInLedgerAcsSync(Seq(P2, P5), alice, paintHouseId)
    assertInLedgerAcsSync(Seq(P3, P4), painter, paintHouseId)

    assertNotInAcsSync(
      Seq(P2, P3, P4, P5),
      iouSynchronizerAlias,
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
      Duration.Zero,
    )
    val painterIouId =
      searchAcsSync(
        Seq(P1, P3, P4),
        iouSynchronizerAlias.unwrap,
        IouModule,
        IouTemplate,
        Duration.Zero,
        Some(painter),
      )

    // Paint agreement
    assertSizeOfAcs(
      Seq(P2, P5),
      iouSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(alice),
    )
    assertSizeOfAcs(
      Seq(P3, P4),
      iouSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(painter),
    )

    // IOU
    assertSizeOfAcs(
      Seq(P1),
      iouSynchronizerAlias,
      s"^$IouModule",
      1,
      Some(bank),
    )
    assertSizeOfAcs(
      Seq(P1, P3, P4),
      iouSynchronizerAlias,
      s"^$IouModule",
      1,
      Some(painter),
    )

    assertSizeOfAcs(
      Seq(P4),
      paintSynchronizerAlias,
      s"^$PaintModule",
      0,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P5),
      paintSynchronizerAlias,
      s"^$PaintModule",
      0,
      Some(alice),
    )
    assertSizeOfAcs(
      Seq(P4),
      paintSynchronizerAlias,
      s"^$IouModule",
      0,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P5),
      paintSynchronizerAlias,
      s"^$IouModule",
      0,
      Some(alice),
    )

    (painterIouId, paintHouseId)
  }

  protected def callIou(
      alice: PartyId,
      bank: PartyId,
      painter: PartyId,
      iouId: LfContractId,
  )(implicit env: TestConsoleEnvironment): LfContractId = {
    import env.*
    logger.info("Painter calls his IOU")
    val iouPackageId = findPackageIdOf(IouModule)

    // submit painter do
    //   exercise painterIouId Call
    val callCmd =
      ledger_api_utils.exercise(iouPackageId, IouModule, IouTemplate, "Call", Map.empty, iouId.coid)
    clue("painter calls his IOU") {
      P3.ledger_api.commands.submit(Seq(painter), Seq(callCmd))
    }
    val getCashId = searchAcsSync(
      Seq(P1, P3, P4),
      iouSynchronizerAlias.unwrap,
      IouModule,
      GetCashTemplate,
      stakeholder = Some(painter),
    )
    assertInLedgerAcsSync(Seq(P1), bank, getCashId)
    assertInLedgerAcsSync(Seq(P3), painter, getCashId)

    assertNotInAcsSync(
      Seq(P1, P3),
      iouSynchronizerAlias,
      IouModule,
      IouTemplate,
      Duration.Zero,
    )

    // paint agreement
    assertSizeOfAcs(
      Seq(P2, P5),
      iouSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(alice),
    )
    assertSizeOfAcs(
      Seq(P3, P4),
      iouSynchronizerAlias,
      s"^$PaintModule",
      1,
      Some(painter),
    )

    // get cash
    assertSizeOfAcs(
      Seq(P1),
      iouSynchronizerAlias,
      s"^$IouModule",
      1,
      Some(bank),
    )
    assertSizeOfAcs(
      Seq(P3, P4),
      iouSynchronizerAlias,
      s"^$IouModule",
      1,
      Some(painter),
    )

    assertSizeOfAcs(
      Seq(P4),
      paintSynchronizerAlias,
      s"^$IouModule",
      0,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P5),
      paintSynchronizerAlias,
      s"^$IouModule",
      0,
      Some(alice),
    )
    assertSizeOfAcs(
      Seq(P4),
      paintSynchronizerAlias,
      s"^$PaintModule",
      0,
      Some(painter),
    )
    assertSizeOfAcs(
      Seq(P5),
      paintSynchronizerAlias,
      s"^$PaintModule",
      0,
      Some(alice),
    )

    getCashId
  }

}
