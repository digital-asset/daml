// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, ReassignmentCounter, SynchronizerAlias}
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*

trait RepairTestUtil {
  this: BaseTest =>

  protected val aliceS: String = "Alice"
  protected val bobS: String = "Bob"
  protected val carolS: String = "Carol"

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var amount = 100

  protected def createContract(
      participant: ParticipantReference,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
      synchronizerId: Option[SynchronizerId] = None,
  ): iou.Iou.ContractId = {
    amount = amount + 1
    val createCmd =
      new iou.Iou(
        payer.toProtoPrimitive,
        owner.toProtoPrimitive,
        new iou.Amount(amount.toBigDecimal, currency),
        List.empty.asJava,
      ).create.commands.asScala.toSeq

    val createTx = participant.ledger_api.javaapi.commands.submit_flat(
      Seq(payer),
      createCmd,
      // do not wait on all participants to observe the transaction (gets confused as we assigned the same id to different participants)
      optTimeout = None,
      synchronizerId = synchronizerId,
    )
    JavaDecodeUtil.decodeAllCreated(iou.Iou.COMPANION)(createTx).loneElement.id
  }

  protected def readContractInstance(
      participant: LocalParticipantReference,
      synchronizerAlias: String,
      synchronizerId: SynchronizerId,
      contractId: ContractId[?],
  ): RepairContract =
    readContractInstance(
      participant,
      SynchronizerAlias.tryCreate(synchronizerAlias),
      synchronizerId,
      contractId,
    )

  protected def readContractInstance(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      contractId: ContractId[?],
  ): RepairContract = {
    val contract = participant.testing
      .pcs_search(synchronizerAlias, exactId = contractId.toLf.coid)
      .headOption
      .value
      ._2

    RepairContract(
      synchronizerId,
      contract,
      ReassignmentCounter.Genesis,
    )
  }

  protected def createContractInstance(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): RepairContract =
    readContractInstance(
      participant,
      synchronizerAlias,
      synchronizerId,
      createContract(participant, payer, owner, currency),
    )

  protected def createArchivedContractInstance(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): RepairContract = {
    val archivedContractId = createContract(participant, payer, owner, currency)
    val archivedContract =
      readContractInstance(participant, synchronizerAlias, synchronizerId, archivedContractId)
    exerciseContract(participant, owner, archivedContractId)
    archivedContract
  }

  protected def exerciseContract(
      participant: LocalParticipantReference,
      owner: PartyId,
      cid: iou.Iou.ContractId,
  ): Assertion = {
    val exerciseCmd = cid.exerciseCall().commands.asScala.toSeq
    val exerciseTx =
      participant.ledger_api.javaapi.commands
        .submit_flat(Seq(owner), exerciseCmd, optTimeout = None)
    val archives = exerciseTx.getEvents.asScala.toSeq.collect {
      case x if x.toProtoEvent.hasArchived =>
        val contractId = x.toProtoEvent.getArchived.getContractId
        logger.info(s"Archived contract $contractId at offset ${exerciseTx.getOffset}")
    }.size

    archives shouldBe 1
  }

  protected def createArchivedContract(
      participant: LocalParticipantReference,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): iou.Iou.ContractId = {
    val cid = createContract(participant, payer, owner, currency)
    exerciseContract(participant, owner, cid)
    cid
  }

  protected def assertAcsCounts(
      expectedCounts: (ParticipantReference, Map[PartyId, Int])*
  ): Unit =
    assertAcsCountsWithFilter(_ => true, expectedCounts*)

  protected def assertAcsCountsWithFilter(
      filter: WrappedContractEntry => Boolean,
      expectedCounts: (ParticipantReference, Map[PartyId, Int])*
  ): Unit =
    eventually() {
      expectedCounts.foreach { case (p, partyInfo) =>
        partyInfo.foreach { case (party, expectedCount) =>
          withClue(
            s"Party ${party.toProtoPrimitive} on participant ${p.id} expected to have $expectedCount active contracts, but found"
          ) {
            val contracts = p.ledger_api.state.acs.of_party(party).toList
            contracts.count(filter) shouldBe expectedCount
          }
        }
      }
    }
}
