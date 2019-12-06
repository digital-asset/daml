// Copyright (c) 2019 The DAML Authors. All rights reserved.
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
    resolveChoiceRecordId: PackageService.ResolveChoiceRecordId,
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
  def exercise(jwt: Jwt, jwtPayload: JwtPayload, input: ExerciseCommand[lav1.value.Value])
    : Future[Error \/ List[Contract[lav1.value.Value]]] = {

    val et: EitherT[Future, Error, List[Contract[lav1.value.Value]]] = for {
      command <- EitherT.either(exerciseCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- liftET(logResult('exercise, submitAndWaitForTransactionTree(jwt, request)))
      _ = println(s"----- response: $response")
      contracts <- EitherT.either(contracts(response))
    } yield contracts

    et.run
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def exerciseWithResult(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      input: ExerciseCommand[lav1.value.Value]): Future[Error \/ lav1.value.Value] = {

    val et: EitherT[Future, Error, lav1.value.Value] = for {
      command <- EitherT.either(exerciseCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
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
      .bimap(
        e => Error('createCommand, e.shows),
        x => Commands.create(refApiIdentifier(x), input.argument))
  }

  private def exerciseCommand(
      input: ExerciseCommand[lav1.value.Value]): Error \/ lav1.commands.Command.Command.Exercise =
    for {
      templateId <- resolveTemplateId(input.templateId)
        .leftMap(e => Error('exerciseCommand, e.shows))
      choiceRecordId <- resolveChoiceRecordId(templateId, input.choice)
        .leftMap(e => Error('exerciseCommand, e.shows))
    } yield
      Commands.exercise(
        refApiIdentifier(templateId),
        input.contractId,
        input.choice,
        choiceRecordId,
        input.argument)

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

  private def contracts(response: lav1.command_service.SubmitAndWaitForTransactionResponse)
    : Error \/ List[Contract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(Error('contracts, s"Received response without transaction: $response"))
      .flatMap(contracts)

  private def contracts(response: lav1.command_service.SubmitAndWaitForTransactionTreeResponse)
    : Error \/ List[Contract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(Error('contracts, s"Received response without transaction: $response"))
      .flatMap(contracts)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def contracts(
      tx: lav1.transaction.Transaction): Error \/ List[Contract[lav1.value.Value]] =
    Contract.fromTransaction(tx).leftMap(e => Error('contracts, e.shows))

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
