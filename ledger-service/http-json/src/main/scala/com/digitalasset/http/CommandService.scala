// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.util.ClientUtil.{uniqueCommandId, workflowIdFromParty}
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.{Commands, Transactions}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.{-\/, Show, \/, \/-}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CommandService(
    resolveTemplateId: Services.ResolveTemplateId,
    submitAndWaitForTransaction: Services.SubmitAndWaitForTransaction,
    timeProvider: TimeProvider,
    defaultTimeToLive: Duration = 30.seconds)(implicit ec: ExecutionContext) {

  def create(jwtPayload: domain.JwtPayload, input: domain.CreateCommand[lav1.value.Value])
    : Future[domain.ActiveContract[lav1.value.Value]] =
    for {
      command <- toFuture(createCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- submitAndWaitForTransaction(request)
      contract <- toFuture(exactlyOneActiveContract(response))
    } yield contract

  def exercise(jwtPayload: domain.JwtPayload, input: domain.ExerciseCommand[lav1.value.Value])
    : Future[List[domain.ActiveContract[lav1.value.Value]]] =
    for {
      command <- toFuture(exerciseCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- submitAndWaitForTransaction(request)
      contracts <- toFuture(activeContracts(response))
    } yield contracts

  private def createCommand(input: domain.CreateCommand[lav1.value.Value])
    : Error \/ lav1.commands.Command.Command.Create = {
    val arguments: lav1.value.Record = input.arguments.map(toRecord).getOrElse(emptyRecord)
    resolveTemplateId(input.templateId)
      .leftMap(e => Error(e.shows))
      .map(x => Commands.create(x, arguments))
  }

  private def exerciseCommand(input: domain.ExerciseCommand[lav1.value.Value])
    : Error \/ lav1.commands.Command.Command.Exercise = {
    val arguments: lav1.value.Record = input.arguments.map(toRecord).getOrElse(emptyRecord)
    resolveTemplateId(input.templateId)
      .leftMap(e => Error(e.shows))
      .map(x => Commands.exercise(x, input.contractId, input.choice, arguments))
  }

  private def toRecord(as: Seq[(String, lav1.value.Value)]) =
    lav1.value.Record(fields = as.map(a => lav1.value.RecordField(a._1, Some(a._2))))

  private val emptyRecord = lav1.value.Record()

  private def submitAndWaitRequest(
      jwtPayload: domain.JwtPayload,
      meta: Option[domain.CommandMeta],
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {

    val ledgerEffectiveTime: Instant =
      meta.flatMap(_.ledgerEffectiveTime).getOrElse(timeProvider.getCurrentTime)
    val maximumRecordTime: Instant = meta
      .flatMap(_.maximumRecordTime)
      .getOrElse(ledgerEffectiveTime.plusNanos(defaultTimeToLive.toNanos))
    val workflowId: lar.WorkflowId =
      meta.flatMap(_.workflowId).getOrElse(workflowIdFromParty(jwtPayload.party))
    val commandId: lar.CommandId = meta.flatMap(_.commandId).getOrElse(uniqueCommandId())

    Commands.submitAndWaitRequest(
      jwtPayload.ledgerId,
      jwtPayload.applicationId,
      workflowId,
      commandId,
      ledgerEffectiveTime,
      maximumRecordTime,
      jwtPayload.party,
      command
    )
  }

  private def exactlyOneActiveContract(
      response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Error \/ domain.ActiveContract[lav1.value.Value] =
    activeContracts(response).flatMap {
      case List(x) => \/-(x)
      case xs @ _ => -\/(Error(s"Expected exactly one active contract, got: $xs"))
    }

  private def activeContracts(response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Error \/ List[domain.ActiveContract[lav1.value.Value]] =
    response.transaction match {
      case None =>
        -\/(Error(s"Received response without transaction: $response"))
      case Some(tx) =>
        activeContracts(tx)
    }

  private def activeContracts(
      tx: lav1.transaction.Transaction): Error \/ List[domain.ActiveContract[lav1.value.Value]] = {

    import scalaz.std.list._
    import scalaz.syntax.traverse._

    Transactions
      .decodeAllCreatedEvents(tx)
      .toList
      .traverseU(domain.ActiveContract.fromLedgerApi)
      .leftMap(Error(_))
  }

}

object CommandService {
  case class Error(message: String)

  object Error {
    implicit val errorShow: Show[Error] = new Show[Error] {
      override def shows(e: Error): String = s"CommandService Error: ${e.message: String}"
    }
  }
}
