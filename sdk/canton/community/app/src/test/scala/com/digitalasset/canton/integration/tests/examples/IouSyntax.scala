// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.{Party, PartyId, SynchronizerId}
import com.digitalasset.canton.{LedgerUserId, config}
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper

import scala.jdk.CollectionConverters.*

object IouSyntax {

  val modelCompanion: ContractCompanion.WithoutKey[Iou.Contract, Iou.ContractId, Iou] =
    iou.Iou.COMPANION

  def testIou(
      payer: Party,
      owner: Party,
      amount: Double = 100.0,
      observers: List[PartyId] = List.empty,
  ): iou.Iou =
    new iou.Iou(
      payer.toProtoPrimitive,
      owner.toProtoPrimitive,
      new iou.Amount(amount.toBigDecimal, "USD"),
      observers.map(_.toProtoPrimitive).asJava,
    )

  def testDivulgeIouByExercise(
      payer: Party,
      divulgee: Party,
  ): DivulgeIouByExercise =
    new DivulgeIouByExercise(
      payer.toProtoPrimitive,
      divulgee.toProtoPrimitive,
    )

  def createIou(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: Party,
      owner: Party,
      amount: Double = 100.0,
      optTimeout: Option[config.NonNegativeDuration] = Some(
        participant.consoleEnvironment.commandTimeouts.ledgerCommand
      ),
      observers: List[PartyId] = List.empty,
  ): Iou.Contract = {
    val createIouCmds = testIou(payer, owner, amount, observers).create().commands().asScala.toSeq

    val tx = participant.ledger_api.javaapi.commands.submit(
      Seq(payer),
      createIouCmds,
      synchronizerId,
      optTimeout = optTimeout,
    )
    JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement
  }

  def createIous(
      participant: ParticipantReference,
      payer: Party,
      owner: Party,
      amounts: Seq[Int],
      observers: Seq[PartyId] = Seq.empty,
  ): Seq[Iou.Contract] = {
    val createIouCmds = amounts.map(amount =>
      testIou(
        payer,
        owner,
        amount.toDouble,
        observers.toList,
      ).create.commands.loneElement
    )

    JavaDecodeUtil
      .decodeAllCreated(Iou.COMPANION)(
        participant.ledger_api.javaapi.commands.submit(
          Seq(payer),
          createIouCmds,
        )
      )
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
  ): (Iou.Contract, Transaction, Completion) =
    complete(participant, payer) {
      val createIouCmd = IouSyntax.testIou(payer, owner, amount).create().commands().asScala.toSeq

      val tx = participant.ledger_api.javaapi.commands.submit(
        Seq(payer),
        createIouCmd,
        synchronizerId,
        optTimeout = None,
        userId = userId,
      )
      JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement
    }

  /** Similar to createIou above but returns the update and command completion
    */
  def createDivulgeIouByExerciseComplete(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: PartyId,
      divulgee: PartyId,
  ): (DivulgeIouByExercise.Contract, Transaction, Completion) =
    complete(participant, divulgee) {
      val createDivulgeIouByExerciseCmd =
        IouSyntax.testDivulgeIouByExercise(payer, divulgee).create().commands().asScala.toSeq

      val tx = participant.ledger_api.javaapi.commands.submit(
        Seq(divulgee),
        createDivulgeIouByExerciseCmd,
        synchronizerId,
        optTimeout = None,
        userId = userId,
      )
      JavaDecodeUtil.decodeAllCreated(DivulgeIouByExercise.COMPANION)(tx).loneElement
    }

  def immediateDivulgeIouComplete(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: PartyId,
      divulgenceContract: DivulgeIouByExercise.Contract,
  ): (Iou.Contract, Transaction, Completion) =
    complete(participant, payer) {
      val createIouByExerciseCmd =
        divulgenceContract.id.exerciseImmediateDivulgeIou().commands().asScala.toSeq

      val tx = participant.ledger_api.javaapi.commands.submit(
        Seq(payer),
        createIouByExerciseCmd,
        synchronizerId,
        optTimeout = None,
        userId = userId,
      )
      JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement
    }

  def retroactiveDivulgeAndArchiveIouComplete(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId] = None,
  )(
      payer: PartyId,
      divulgenceContract: DivulgeIouByExercise.Contract,
      iouContractId: Iou.ContractId,
  ): (Transaction, Completion) = {
    val (_, transaction, completion) = complete(participant, payer) {
      val archiveIouByExerciseCmd =
        divulgenceContract.id
          .exerciseRetroactiveArchivalDivulgeIou(iouContractId)
          .commands()
          .asScala
          .toSeq

      participant.ledger_api.javaapi.commands.submit(
        Seq(payer),
        archiveIouByExerciseCmd,
        synchronizerId,
        optTimeout = None,
        userId = userId,
      )
    }

    (transaction, completion)
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

  private def complete[T](participant: ParticipantReference, submitterParty: PartyId)(
      submission: => T
  ): (T, Transaction, Completion) = {
    val ledgerEnd = participant.ledger_api.state.end()

    val submissionResult = submission

    val completions = participant.ledger_api.completions.list(
      partyId = submitterParty,
      beginOffsetExclusive = ledgerEnd,
      atLeastNumCompletions = 1,
      userId = userId,
    )

    val transactions = participant.ledger_api.updates.transactions(
      partyIds = Set(submitterParty),
      completeAfter = PositiveInt.one,
      beginOffsetExclusive = ledgerEnd,
    )

    val transaction = transactions.loneElement.transaction

    (submissionResult, transaction, completions.loneElement)
  }

  private val userId: LedgerUserId =
    LedgerUserId.assertFromString("enterprise-user")
}
