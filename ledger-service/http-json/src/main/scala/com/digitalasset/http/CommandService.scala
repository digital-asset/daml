// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.{ClientUtil, Commands, Transactions}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.string._
import scalaz.syntax.show._
import scalaz.{Show, \/}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CommandService(
    resolveTemplateId: Services.ResolveTemplateId,
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

  def execute(jwtPayload: domain.JwtPayload, input: domain.CreateCommand) = {}

  private def createCommand(
      input: domain.CreateCommand): Error \/ lav1.commands.Command.Command.Create = {
    val arguments = input.arguments.getOrElse(emptyRecord)
    resolveTemplateId(input.templateId)
      .leftMap(e => Error(e.shows))
      .map(x => Commands.create(x, arguments))
  }

  private def exerciseCommand(
      input: domain.ExerciseCommand): Error \/ lav1.commands.Command.Command.Exercise = {
    val arguments = input.arguments.getOrElse(emptyRecord)
//    Commands.exercise(input.contractId, arguments)
    ???
  }

  private val emptyRecord = lav1.value.Record()

  private def submitAndWaitRequest(
      jwtPayload: domain.JwtPayload,
      input: domain.CreateCommand,
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {

    val ledgerEffectiveTime: Instant =
      input.ledgerEffectiveTime.getOrElse(timeProvider.getCurrentTime)
    val maximumRecordTime: Instant =
      input.maximumRecordTime.getOrElse(ledgerEffectiveTime.plusNanos(defaultTimeToLive.toNanos))

    Commands.submitAndWaitRequest(
      jwtPayload.ledgerId,
      jwtPayload.applicationId,
      input.workflowId.getOrElse(ClientUtil.workflowIdFromParty(jwtPayload.party)),
      input.commandId.getOrElse(ClientUtil.uniqueCommandId()),
      ledgerEffectiveTime,
      maximumRecordTime,
      jwtPayload.party,
      command
    )
  }

  private def activeContract(response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Future[domain.ActiveContract[lav1.value.Value]] =
    for {
      t <- toFuture(response.transaction)
      c <- toFuture(Transactions.decodeCreatedEvent(t))
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
