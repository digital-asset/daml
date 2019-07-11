// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.util.ClientUtil.workflowIdFromParty
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.{ClientUtil, TransactionUtil}
import com.digitalasset.ledger.api.refinements.ApiTypes.{CommandId, WorkflowId}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.string._
import scalaz.syntax.show._
import scalaz.{-\/, Show, \/, \/-}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CommandService(
    resolveTemplateIds: Services.ResolveTemplateIds,
    submitAndWaitForTransaction: Services.SubmitAndWaitForTransaction,
    timeProvider: TimeProvider,
    defaultTimeToLive: Duration = 30.seconds)(implicit ec: ExecutionContext) {

  def create(
      jwtPayload: domain.JwtPayload,
      input: domain.CreateCommand): Future[domain.ActiveContract[lav1.value.Value]] =
    for {
      command <- toFuture(createCommand(input))
      request = submitAndWaitRequest(jwtPayload, input, command)
      response <- submitAndWaitForTransaction(request)
      contract <- activeContract(response)
    } yield contract

  private def createCommand(
      input: domain.CreateCommand): Error \/ lav1.commands.Command.Command.Create = {
    val arguments = input.arguments.getOrElse(lav1.value.Record())
    resolveTemplateIds(Set(input.templateId))
      .leftMap(e => Error(e.shows))
      .flatMap {
        case List(x) =>
          \/-(createCommand(x, arguments))
        case xs @ _ =>
          -\/(Error(
            s"Template ID resolution error. ${input.templateId} resolved to multiple values: $xs"))
      }
  }

  private def createCommand(
      templateId: lav1.value.Identifier,
      arguments: lav1.value.Record): lav1.commands.Command.Command.Create =
    lav1.commands.Command.Command.Create(
      lav1.commands.CreateCommand(templateId = Some(templateId), createArguments = Some(arguments)))

  private def submitAndWaitRequest(
      jwtPayload: domain.JwtPayload,
      input: domain.CreateCommand,
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {
    val workflowId: WorkflowId =
      input.workflowId.getOrElse(workflowIdFromParty(jwtPayload.party))
    val commandId: CommandId = input.commandId.getOrElse(ClientUtil.uniqueCommandId())
    val ledgerEffectiveTime: Instant =
      input.ledgerEffectiveTime.getOrElse(timeProvider.getCurrentTime)
    val maximumRecordTime: Instant =
      input.maximumRecordTime.getOrElse(ledgerEffectiveTime.plusNanos(defaultTimeToLive.toNanos))

    val commands = lav1.commands.Commands(
      ledgerId = lar.LedgerId.unwrap(jwtPayload.ledgerId),
      workflowId = lar.WorkflowId.unwrap(workflowId),
      applicationId = lar.ApplicationId.unwrap(jwtPayload.applicationId),
      commandId = lar.CommandId.unwrap(commandId),
      party = lar.Party.unwrap(jwtPayload.party),
      ledgerEffectiveTime = Some(fromInstant(ledgerEffectiveTime)),
      maximumRecordTime = Some(fromInstant(maximumRecordTime)),
      commands = Seq(lav1.commands.Command(command))
    )

    lav1.command_service.SubmitAndWaitRequest(Some(commands))
  }

  private def activeContract(response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Future[domain.ActiveContract[lav1.value.Value]] =
    for {
      t <- toFuture(response.transaction)
      c <- toFuture(TransactionUtil.decodeCreatedEvent(t))
      a <- toFuture(domain.ActiveContract.fromLedgerApi(c))
    } yield a
}

object CommandService {
  case class Error(message: String)

  object Error {
    implicit val errorShow: Show[Error] = new Show[Error] {
      override def shows(e: Error): String = s"CommandService Error: ${e.message: String}"
    }
  }
}
