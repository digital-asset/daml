// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.domain.{
  ActiveContract,
  Contract,
  CreateCommand,
  ExerciseCommand,
  ExerciseResponse,
  JwtPayload
}
import com.digitalasset.http.util.ClientUtil.uniqueCommandId
import com.digitalasset.http.util.IdentifierConverters.refApiIdentifier
import com.digitalasset.http.util.{Commands, Transactions}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Show, \/, \/-}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    submitAndWaitForTransaction: LedgerClientJwt.SubmitAndWaitForTransaction,
    submitAndWaitForTransactionTree: LedgerClientJwt.SubmitAndWaitForTransactionTree,
    timeProvider: TimeProvider,
    defaultTimeToLive: Duration = 30.seconds)(implicit ec: ExecutionContext)
    extends StrictLogging {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def create(jwt: Jwt, jwtPayload: JwtPayload, input: CreateCommand[lav1.value.Record])
    : Future[Error \/ ActiveContract[lav1.value.Value]] = {

    val et: EitherT[Future, Error, ActiveContract[lav1.value.Value]] = for {
      command <- EitherT.either(createCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- liftET(logResult('create, submitAndWaitForTransaction(jwt, request)))
      contract <- EitherT.either(exactlyOneActiveContract(response))
    } yield contract

    et.run
  }

  private def liftET[A](fa: Future[A]): EitherT[Future, Error, A] = EitherT.rightT(fa)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def exercise(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      input: ExerciseCommand[lav1.value.Value, (domain.TemplateId.RequiredPkg, domain.ContractId)])
    : Future[Error \/ ExerciseResponse[lav1.value.Value]] = {

    val command = exerciseCommand(input)
    val request = submitAndWaitRequest(jwtPayload, input.meta, command)

    val et: EitherT[Future, Error, ExerciseResponse[lav1.value.Value]] = for {
      response <- liftET(logResult('exercise, submitAndWaitForTransactionTree(jwt, request)))
      exerciseResult <- EitherT.either(exerciseResult(response))
      contracts <- EitherT.either(contracts(response))
    } yield ExerciseResponse(exerciseResult, contracts)

    et.run
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def exerciseWithResult(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      input: ExerciseCommand[lav1.value.Value, (domain.TemplateId.RequiredPkg, domain.ContractId)])
    : Future[Error \/ lav1.value.Value] = {

    val command = exerciseCommand(input)
    val request = submitAndWaitRequest(jwtPayload, input.meta, command)

    val et: EitherT[Future, Error, lav1.value.Value] = for {
      response <- liftET(
        logResult('exerciseWithResult, submitAndWaitForTransactionTree(jwt, request)))
      result <- EitherT.either(exerciseResult(response))
    } yield result

    et.run
  }

  private def logResult[A](op: Symbol, fa: Future[A]): Future[A] = {
    fa.onComplete {
      case Failure(e) => logger.error(s"$op failure", e)
      case Success(a) => logger.debug(s"$op success: $a")
    }
    fa
  }

  private def createCommand(
      input: CreateCommand[lav1.value.Record]): Error \/ lav1.commands.Command.Command.Create = {
    resolveTemplateId(input.templateId)
      .toRightDisjunction(
        Error('createCommand, ErrorMessages.cannotResolveTemplateId(input.templateId)))
      .map(tpId => Commands.create(refApiIdentifier(tpId), input.argument))
  }

  private def exerciseCommand(
      input: ExerciseCommand[lav1.value.Value, (domain.TemplateId.RequiredPkg, domain.ContractId)])
    : lav1.commands.Command.Command.Exercise = {
    Commands.exercise(
      templateId = refApiIdentifier(input.reference._1),
      contractId = input.reference._2,
      choice = input.choice,
      argument = input.argument)
  }

  private def submitAndWaitRequest(
      jwtPayload: JwtPayload,
      meta: Option[domain.CommandMeta],
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {

    val ledgerEffectiveTime: Instant =
      meta.flatMap(_.ledgerEffectiveTime).getOrElse(timeProvider.getCurrentTime)
    val maximumRecordTime: Instant = meta
      .flatMap(_.maximumRecordTime)
      .getOrElse(ledgerEffectiveTime.plusNanos(defaultTimeToLive.toNanos))
    val commandId: lar.CommandId = meta.flatMap(_.commandId).getOrElse(uniqueCommandId())

    Commands.submitAndWaitRequest(
      jwtPayload.ledgerId,
      jwtPayload.applicationId,
      commandId,
      ledgerEffectiveTime,
      maximumRecordTime,
      jwtPayload.party,
      command
    )
  }

  private def exactlyOneActiveContract(
      response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Error \/ ActiveContract[lav1.value.Value] =
    activeContracts(response).flatMap {
      case Seq(x) => \/-(x)
      case xs @ _ =>
        -\/(Error('exactlyOneActiveContract, s"Expected exactly one active contract, got: $xs"))
    }

  private def activeContracts(response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Error \/ ImmArraySeq[ActiveContract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(
        Error('activeContracts, s"Received response without transaction: $response"))
      .flatMap(activeContracts)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def activeContracts(
      tx: lav1.transaction.Transaction): Error \/ ImmArraySeq[ActiveContract[lav1.value.Value]] = {
    Transactions
      .allCreatedEvents(tx)
      .traverse(ActiveContract.fromLedgerApi(_))
      .leftMap(e => Error('activeContracts, e.shows))
  }

  private def contracts(response: lav1.command_service.SubmitAndWaitForTransactionTreeResponse)
    : Error \/ List[Contract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(Error('contracts, s"Received response without transaction: $response"))
      .flatMap(contracts)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def contracts(
      tx: lav1.transaction.TransactionTree): Error \/ List[Contract[lav1.value.Value]] =
    Contract.fromTransactionTree(tx).leftMap(e => Error('contracts, e.shows)).map(_.toList)

  private def exerciseResult(a: lav1.command_service.SubmitAndWaitForTransactionTreeResponse)
    : Error \/ lav1.value.Value = {
    val result: Option[lav1.value.Value] = for {
      transaction <- a.transaction: Option[lav1.transaction.TransactionTree]
      treeEvent <- rootTreeEvent(transaction): Option[lav1.transaction.TreeEvent]
      exercised <- treeEvent.kind.exercised: Option[lav1.event.ExercisedEvent]
      exResult <- exercised.exerciseResult: Option[lav1.value.Value]
    } yield exResult

    result.toRightDisjunction(
      Error(
        'choiceArgument,
        s"Cannot get exerciseResult from the first root event of gRPC response: ${a.toString}"))
  }

  private def rootTreeEvent(
      a: lav1.transaction.TransactionTree): Option[lav1.transaction.TreeEvent] =
    a.rootEventIds.headOption.flatMap { id =>
      a.eventsById.get(id)
    }
}

object CommandService {
  final case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"CommandService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }
}
