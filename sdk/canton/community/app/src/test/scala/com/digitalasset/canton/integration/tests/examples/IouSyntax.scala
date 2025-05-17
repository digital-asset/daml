// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction.Transaction
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{LedgerUserId, config}

import scala.jdk.CollectionConverters.*

object IouSyntax {

  import org.scalatest.OptionValues.*

  def testIou(
      payer: PartyId,
      owner: PartyId,
      amount: Double = 100.0,
      observers: List[PartyId] = List.empty,
  ): iou.Iou =
    new iou.Iou(
      payer.toProtoPrimitive,
      owner.toProtoPrimitive,
      new iou.Amount(amount.toBigDecimal, "USD"),
      observers.map(_.toProtoPrimitive).asJava,
    )

  def createIou(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: PartyId,
      owner: PartyId,
      amount: Double = 100.0,
      optTimeout: Option[config.NonNegativeDuration] = Some(
        participant.consoleEnvironment.commandTimeouts.ledgerCommand
      ),
      observers: List[PartyId] = List.empty,
  ): Iou.Contract = {
    val createIouCmd = testIou(payer, owner, amount, observers).create().commands().asScala.toSeq

    val tx = participant.ledger_api.javaapi.commands.submit(
      Seq(payer),
      createIouCmd,
      synchronizerId,
      optTimeout = optTimeout,
    )
    JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).headOption.value
  }

  /** Similar to createIou above but returns the update and command completion
    */
  def createIouComplete(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: PartyId,
      owner: PartyId,
      amount: Double = 100.0,
  ): (Iou.Contract, Transaction, Completion) = {
    val userId: LedgerUserId =
      LedgerUserId.assertFromString("enterprise-user")

    val ledgerEnd = participant.ledger_api.state.end()

    val createIouCmd = IouSyntax.testIou(payer, owner, amount).create().commands().asScala.toSeq

    val tx = participant.ledger_api.javaapi.commands.submit(
      Seq(payer),
      createIouCmd,
      synchronizerId,
      optTimeout = None,
      userId = userId,
    )
    val iou = JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).headOption.value

    val completions = participant.ledger_api.completions.list(
      partyId = payer,
      beginOffsetExclusive = ledgerEnd,
      atLeastNumCompletions = 1,
      userId = userId,
    )

    val transactions = participant.ledger_api.updates.transactions(
      partyIds = Set(payer),
      completeAfter = PositiveInt.one,
      beginOffsetExclusive = ledgerEnd,
    )

    val transaction = transactions.headOption.value.transaction

    (iou, transaction, completions.headOption.value)
  }

  def archive(participant: ParticipantReference, synchronizerId: Option[SynchronizerId] = None)(
      contract: Iou.Contract,
      submittingParty: PartyId,
  ): Unit =
    participant.ledger_api.commands
      .submit(
        Seq(submittingParty),
        contract.id
          .exerciseArchive()
          .commands
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand)),
        synchronizerId,
      )
      .discard
}
