// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.util.ClientUtil.{uniqueCommandId, workflowIdFromParty}
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.refApiIdentifier
import com.digitalasset.http.util.{Commands, Transactions}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.{-\/, Show, \/, \/-}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    submitAndWaitForTransaction: Services.SubmitAndWaitForTransaction,
    timeProvider: TimeProvider,
    defaultTimeToLive: Duration = 30.seconds)(implicit ec: ExecutionContext)
    extends StrictLogging {

  def create(jwtPayload: domain.JwtPayload, input: domain.CreateCommand[lav1.value.Record])
    : Future[domain.ActiveContract[lav1.value.Value]] =
    for {
      command <- toFuture(createCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- logResult('create, submitAndWaitForTransaction(request))
      contract <- toFuture(exactlyOneActiveContract(response))
    } yield contract

  def exercise(jwtPayload: domain.JwtPayload, input: domain.ExerciseCommand[lav1.value.Record])
    : Future[List[domain.ActiveContract[lav1.value.Value]]] =
    for {
      command <- toFuture(exerciseCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- logResult('exercise, submitAndWaitForTransaction(request))
      contracts <- toFuture(activeContracts(response))
    } yield contracts

  private def logResult[A](op: Symbol, fa: Future[A]): Future[A] = {
    fa.onComplete {
      case Success(a) => logger.debug(s"$op success: $a")
      case Failure(e) => logger.error(s"$op failure", e)
    }
    fa
  }

  private def createCommand(input: domain.CreateCommand[lav1.value.Record])
    : Error \/ lav1.commands.Command.Command.Create = {
    resolveTemplateId(input.templateId)
      .leftMap(e => Error(e.shows))
      .map(x => Commands.create(refApiIdentifier(x), input.argument))
  }

  private def exerciseCommand(input: domain.ExerciseCommand[lav1.value.Record])
    : Error \/ lav1.commands.Command.Command.Exercise = {
    resolveTemplateId(input.templateId)
      .leftMap(e => Error(e.shows))
      .map(x =>
        Commands.exercise(refApiIdentifier(x), input.contractId, input.choice, input.argument))
  }

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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def activeContracts(
      tx: lav1.transaction.Transaction): Error \/ List[domain.ActiveContract[lav1.value.Value]] = {

    import scalaz.std.list._
    import scalaz.syntax.traverse._

    Transactions
      .decodeAllCreatedEvents(tx)
      .toList
      .traverse(domain.ActiveContract.fromLedgerApi)
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
