// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.http.ErrorMessages.cannotResolveTemplateId
import com.daml.http.domain.{
  ActiveContract,
  Contract,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseCommand,
  ExerciseResponse,
  JwtWritePayload,
}
import com.daml.http.util.ClientUtil.uniqueCommandId
import com.daml.http.util.FutureUtil._
import com.daml.http.util.IdentifierConverters.refApiIdentifier
import com.daml.http.util.Logging.{CorrelationID, RequestID}
import com.daml.http.util.{Commands, Transactions}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Show, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    submitAndWaitForTransaction: LedgerClientJwt.SubmitAndWaitForTransaction,
    submitAndWaitForTransactionTree: LedgerClientJwt.SubmitAndWaitForTransactionTree,
)(implicit ec: ExecutionContext) {

  import CommandService._

  def create(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: CreateCommand[lav1.value.Record],
  )(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[Error \/ ActiveContract[lav1.value.Value]] = {
    logger.trace("sending create command to ledger")
    val et: ET[ActiveContract[lav1.value.Value]] = for {
      command <- either(createCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- rightT(logResult(Symbol("create"), submitAndWaitForTransaction(jwt, request)))
      contract <- either(exactlyOneActiveContract(response))
    } yield contract
    et.run
  }

  def exercise(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: ExerciseCommand[lav1.value.Value, ExerciseCommandRef],
  )(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[Error \/ ExerciseResponse[lav1.value.Value]] = {
    logger.trace("sending exercise command to ledger")
    val command = exerciseCommand(input)
    val request = submitAndWaitRequest(jwtPayload, input.meta, command)

    val et: ET[ExerciseResponse[lav1.value.Value]] = for {
      response <- rightT(
        logResult(Symbol("exercise"), submitAndWaitForTransactionTree(jwt, request))
      )
      exerciseResult <- either(exerciseResult(response))
      contracts <- either(contracts(response))
    } yield ExerciseResponse(exerciseResult, contracts)

    et.run
  }

  def createAndExercise(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: CreateAndExerciseCommand[lav1.value.Record, lav1.value.Value],
  )(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[Error \/ ExerciseResponse[lav1.value.Value]] = {
    logger.trace("sending create and exercise command to ledger")
    val et: ET[ExerciseResponse[lav1.value.Value]] = for {
      command <- either(createAndExerciseCommand(input))
      request = submitAndWaitRequest(jwtPayload, input.meta, command)
      response <- rightT(
        logResult(Symbol("createAndExercise"), submitAndWaitForTransactionTree(jwt, request))
      )
      exerciseResult <- either(exerciseResult(response))
      contracts <- either(contracts(response))
    } yield ExerciseResponse(exerciseResult, contracts)

    et.run
  }

  private def logResult[A](op: Symbol, fa: Future[A])(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Future[A] = {
    fa.onComplete {
      case Failure(e) => logger.error(s"$op failure", e)
      case Success(a) => logger.debug(s"$op success: $a")
    }
    fa
  }

  private def createCommand(
      input: CreateCommand[lav1.value.Record]
  ): Error \/ lav1.commands.Command.Command.Create = {
    resolveTemplateId(input.templateId)
      .toRightDisjunction(Error(Symbol("createCommand"), cannotResolveTemplateId(input.templateId)))
      .map(tpId => Commands.create(refApiIdentifier(tpId), input.payload))
  }

  private def exerciseCommand(
      input: ExerciseCommand[lav1.value.Value, ExerciseCommandRef]
  ): lav1.commands.Command.Command =
    input.reference match {
      case -\/((templateId, contractKey)) =>
        Commands.exerciseByKey(
          templateId = refApiIdentifier(templateId),
          contractKey = contractKey,
          choice = input.choice,
          argument = input.argument,
        )
      case \/-((templateId, contractId)) =>
        Commands.exercise(
          templateId = refApiIdentifier(templateId),
          contractId = contractId,
          choice = input.choice,
          argument = input.argument,
        )
    }

  private def createAndExerciseCommand(
      input: CreateAndExerciseCommand[lav1.value.Record, lav1.value.Value]
  ): Error \/ lav1.commands.Command.Command.CreateAndExercise =
    resolveTemplateId(input.templateId)
      .toRightDisjunction(
        Error(Symbol("createAndExerciseCommand"), cannotResolveTemplateId(input.templateId))
      )
      .map(tpId =>
        Commands
          .createAndExercise(refApiIdentifier(tpId), input.payload, input.choice, input.argument)
      )

  private def submitAndWaitRequest(
      jwtPayload: JwtWritePayload,
      meta: Option[domain.CommandMeta],
      command: lav1.commands.Command.Command,
  ): lav1.command_service.SubmitAndWaitRequest = {

    val commandId: lar.CommandId = meta.flatMap(_.commandId).getOrElse(uniqueCommandId())

    Commands.submitAndWaitRequest(
      jwtPayload.ledgerId,
      jwtPayload.applicationId,
      commandId,
      jwtPayload.actAs,
      jwtPayload.readAs,
      command,
    )
  }

  private def exactlyOneActiveContract(
      response: lav1.command_service.SubmitAndWaitForTransactionResponse
  ): Error \/ ActiveContract[lav1.value.Value] =
    activeContracts(response).flatMap {
      case Seq(x) => \/-(x)
      case xs @ _ =>
        -\/(
          Error(
            Symbol("exactlyOneActiveContract"),
            s"Expected exactly one active contract, got: $xs",
          )
        )
    }

  private def activeContracts(
      response: lav1.command_service.SubmitAndWaitForTransactionResponse
  ): Error \/ ImmArraySeq[ActiveContract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(
        Error(Symbol("activeContracts"), s"Received response without transaction: $response")
      )
      .flatMap(activeContracts)

  private def activeContracts(
      tx: lav1.transaction.Transaction
  ): Error \/ ImmArraySeq[ActiveContract[lav1.value.Value]] = {
    Transactions
      .allCreatedEvents(tx)
      .traverse(ActiveContract.fromLedgerApi(_))
      .leftMap(e => Error(Symbol("activeContracts"), e.shows))
  }

  private def contracts(
      response: lav1.command_service.SubmitAndWaitForTransactionTreeResponse
  ): Error \/ List[Contract[lav1.value.Value]] =
    response.transaction
      .toRightDisjunction(
        Error(Symbol("contracts"), s"Received response without transaction: $response")
      )
      .flatMap(contracts)

  private def contracts(
      tx: lav1.transaction.TransactionTree
  ): Error \/ List[Contract[lav1.value.Value]] =
    Contract.fromTransactionTree(tx).leftMap(e => Error(Symbol("contracts"), e.shows)).map(_.toList)

  private def exerciseResult(
      a: lav1.command_service.SubmitAndWaitForTransactionTreeResponse
  ): Error \/ lav1.value.Value = {
    val result: Option[lav1.value.Value] = for {
      transaction <- a.transaction: Option[lav1.transaction.TransactionTree]
      exercised <- firstExercisedEvent(transaction): Option[lav1.event.ExercisedEvent]
      exResult <- exercised.exerciseResult: Option[lav1.value.Value]
    } yield exResult

    result.toRightDisjunction(
      Error(
        Symbol("choiceArgument"),
        s"Cannot get exerciseResult from the first ExercisedEvent of gRPC response: ${a.toString}",
      )
    )
  }

  private def firstExercisedEvent(
      tx: lav1.transaction.TransactionTree
  ): Option[lav1.event.ExercisedEvent] = {
    val lookup: String => Option[lav1.event.ExercisedEvent] = id =>
      tx.eventsById.get(id).flatMap(_.kind.exercised)
    tx.rootEventIds.collectFirst(Function unlift lookup)
  }
}

object CommandService {
  final case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"CommandService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  private type ET[A] = EitherT[Future, Error, A]

  type ExerciseCommandRef = domain.ResolvedContractRef[lav1.value.Value]

  private val logger = ContextualizedLogger.get(classOf[CommandService])
}
