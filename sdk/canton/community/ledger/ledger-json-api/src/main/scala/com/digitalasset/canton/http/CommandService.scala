// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.http.domain.{
  ActiveContract,
  Choice,
  Contract,
  ContractTypeId,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseCommand,
  ExerciseResponse,
  JwtWritePayload,
}
import com.daml.jwt.Jwt
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.daml.ledger.api.v2 as lav2
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.logging.LoggingContextOf
import com.daml.logging.LoggingContextOf.{label, withEnrichedLoggingContext}
import com.digitalasset.canton.http.LedgerClientJwt.Grpc
import com.digitalasset.canton.http.util.ClientUtil.uniqueCommandId
import com.digitalasset.canton.http.util.FutureUtil.*
import com.digitalasset.canton.http.util.IdentifierConverters.refApiIdentifier
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.{Commands, Transactions}
import com.digitalasset.canton.ledger.service.Grpc.StatusEnvelope
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import scalaz.std.scalaFuture.*
import scalaz.syntax.show.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandService(
    submitAndWaitForTransaction: LedgerClientJwt.SubmitAndWaitForTransaction,
    submitAndWaitForTransactionTree: LedgerClientJwt.SubmitAndWaitForTransactionTree,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  import CommandService.*

  private def withTemplateLoggingContext[CtId <: ContractTypeId.RequiredPkg, T](
      templateId: CtId
  )(implicit lc: LoggingContextOf[T]): withEnrichedLoggingContext[CtId, T] =
    withEnrichedLoggingContext(
      label[CtId],
      "template_id" -> templateId.toString,
    )

  private def withTemplateChoiceLoggingContext[CtId <: ContractTypeId.RequiredPkg, T](
      templateId: CtId,
      choice: domain.Choice,
  )(implicit lc: LoggingContextOf[T]): withEnrichedLoggingContext[Choice, CtId with T] =
    withTemplateLoggingContext(templateId).run(
      withEnrichedLoggingContext(
        label[domain.Choice],
        "choice" -> choice.toString,
      )(_)
    )

  def create(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: CreateCommand[lav2.value.Record, ContractTypeId.Template.RequiredPkg],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      traceContext: TraceContext,
  ): Future[Error \/ domain.CreateCommandResponse[lav2.value.Value]] =
    withTemplateLoggingContext(input.templateId).run { implicit lc =>
      logger.trace(s"sending create command to ledger, ${lc.makeString}")
      val command = createCommand(input)
      val request = submitAndWaitRequest(jwtPayload, input.meta, command, "create")
      val et: ET[domain.CreateCommandResponse[lav2.value.Value]] = for {
        response <- logResult(
          Symbol("create"),
          submitAndWaitForTransaction(jwt, request)(traceContext)(lc),
        )
        contract <- either(exactlyOneActiveContract(response))
      } yield domain.CreateCommandResponse(
        contract.contractId,
        contract.templateId,
        contract.key,
        contract.payload,
        contract.signatories,
        contract.observers,
        domain.CompletionOffset(
          response.transaction.map(_.offset).map(ApiOffset.fromLong).getOrElse("")
        ),
      )
      et.run
    }

  def exercise(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: ExerciseCommand.RequiredPkg[lav2.value.Value, ExerciseCommandRef],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ ExerciseResponse[lav2.value.Value]] =
    withEnrichedLoggingContext(
      label[lav2.value.Value],
      "contract_id" -> input.argument.getContractId,
    ).run(implicit lc =>
      withTemplateChoiceLoggingContext(input.reference.fold(_._1, _._1), input.choice)
        .run { implicit lc =>
          logger.trace(s"sending exercise command to ledger, ${lc.makeString}")
          val command = exerciseCommand(input)

          val request = submitAndWaitRequest(jwtPayload, input.meta, command, "exercise")

          val et: ET[ExerciseResponse[lav2.value.Value]] =
            for {
              response <-
                logResult(Symbol("exercise"), submitAndWaitForTransactionTree(jwt, request)(lc))
              exerciseResult <- either(exerciseResult(response))
              contracts <- either(contracts(response))
            } yield ExerciseResponse(
              exerciseResult,
              contracts,
              domain.CompletionOffset(
                response.transaction.map(_.offset).map(ApiOffset.fromLong).getOrElse("")
              ),
            )

          et.run
        }
    )

  def createAndExercise(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      input: CreateAndExerciseCommand.LAVResolved,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ ExerciseResponse[lav2.value.Value]] =
    withTemplateChoiceLoggingContext(input.templateId, input.choice).run { implicit lc =>
      logger.trace(s"sending create and exercise command to ledger, ${lc.makeString}")
      val command = createAndExerciseCommand(input)
      val request = submitAndWaitRequest(jwtPayload, input.meta, command, "createAndExercise")
      val et: ET[ExerciseResponse[lav2.value.Value]] = for {
        response <- logResult(
          Symbol("createAndExercise"),
          submitAndWaitForTransactionTree(jwt, request)(lc),
        )
        exerciseResult <- either(exerciseResult(response))
        contracts <- either(contracts(response))
      } yield ExerciseResponse(
        exerciseResult,
        contracts,
        domain.CompletionOffset(
          response.transaction.map(_.offset).map(ApiOffset.fromLong).getOrElse("")
        ),
      )
      et.run
    }

  private def logResult[A](
      op: Symbol,
      fa: Grpc.EFuture[Grpc.Category.SubmitError, A],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[A] = {
    val opName = op.name
    EitherT {
      fa.transformWith {
        case Failure(e) =>
          Future.successful(-\/(e match {
            case StatusEnvelope(status) => GrpcError(status)
            case _ => InternalError(Some(op), e)
          }))
        case Success(-\/(e)) =>
          import Grpc.Category.*
          val tagged = e.e match {
            case PermissionDenied => -\/(PermissionDenied)
            case InvalidArgument => \/-(InvalidArgument)
          }
          Future.successful(-\/(ClientError(tagged, e.message)))
        case Success(\/-(a)) =>
          logger.debug(s"$opName success: $a, ${lc.makeString}")
          Future.successful(\/-(a))
      }
    }
  }

  private def createCommand(
      input: CreateCommand[lav2.value.Record, ContractTypeId.Template.RequiredPkg]
  ): lav2.commands.Command.Command.Create =
    Commands.create(refApiIdentifier(input.templateId), input.payload)

  private def exerciseCommand(
      input: ExerciseCommand.RequiredPkg[lav2.value.Value, ExerciseCommandRef]
  ): lav2.commands.Command.Command = {
    val choiceSource =
      input.choiceInterfaceId getOrElse input.reference.fold(_._1, _._1)
    input.reference match {
      case -\/((templateId, contractKey)) =>
        Commands.exerciseByKey(
          templateId = refApiIdentifier(templateId),
          // TODO daml-14549 somehow pass choiceSource
          contractKey = contractKey,
          choice = input.choice,
          argument = input.argument,
        )
      case \/-((_, contractId)) =>
        Commands.exercise(
          templateId = refApiIdentifier(choiceSource),
          contractId = contractId,
          choice = input.choice,
          argument = input.argument,
        )
    }
  }

  // TODO daml-14549 somehow use the choiceInterfaceId
  private def createAndExerciseCommand(
      input: CreateAndExerciseCommand.LAVResolved
  ): lav2.commands.Command.Command.CreateAndExercise =
    Commands
      .createAndExercise(
        refApiIdentifier(input.templateId),
        input.payload,
        input.choice,
        input.argument,
      )

  private def submitAndWaitRequest(
      jwtPayload: JwtWritePayload,
      meta: Option[domain.CommandMeta.LAV],
      command: lav2.commands.Command.Command,
      commandKind: String,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): lav2.command_service.SubmitAndWaitRequest = {
    val commandId: lar.CommandId = meta.flatMap(_.commandId).getOrElse(uniqueCommandId())
    val actAs = meta.flatMap(_.actAs) getOrElse jwtPayload.submitter
    val readAs = meta.flatMap(_.readAs) getOrElse jwtPayload.readAs
    withEnrichedLoggingContext(
      label[lar.CommandId],
      "command_id" -> commandId.toString,
    )
      .run { implicit lc =>
        logger.info(
          s"Submitting $commandKind command, ${lc.makeString}"
        )
        Commands.submitAndWaitRequest(
          jwtPayload.applicationId,
          commandId,
          actAs,
          readAs,
          command,
          meta
            .flatMap(_.deduplicationPeriod)
            .map(_.toProto)
            .getOrElse(DeduplicationPeriod.Empty),
          submissionId = meta.flatMap(_.submissionId),
          workflowId = meta.flatMap(_.workflowId),
          meta.flatMap(_.disclosedContracts) getOrElse Seq.empty,
          meta.flatMap(_.domainId),
          meta.flatMap(_.packageIdSelectionPreference) getOrElse Seq.empty,
        )
      }
  }

  private def exactlyOneActiveContract(
      response: lav2.command_service.SubmitAndWaitForTransactionResponse
  ): Error \/ ActiveContract[ContractTypeId.Template.ResolvedPkgId, lav2.value.Value] =
    activeContracts(response).flatMap {
      case Seq(x) => \/-(x)
      case xs @ _ =>
        -\/(
          InternalError(
            Some(Symbol("exactlyOneActiveContract")),
            s"Expected exactly one active contract, got: $xs",
          )
        )
    }

  private def activeContracts(
      response: lav2.command_service.SubmitAndWaitForTransactionResponse
  ): Error \/ ImmArraySeq[ActiveContract[ContractTypeId.Template.ResolvedPkgId, lav2.value.Value]] =
    response.transaction
      .toRightDisjunction(
        InternalError(
          Some(Symbol("activeContracts")),
          s"Received response without transaction: $response",
        )
      )
      .flatMap(activeContracts)

  private def activeContracts(
      tx: lav2.transaction.Transaction
  ): Error \/ ImmArraySeq[ActiveContract[ContractTypeId.Template.ResolvedPkgId, lav2.value.Value]] =
    Transactions
      .allCreatedEvents(tx)
      .traverse(ActiveContract.fromLedgerApi(domain.ActiveContract.ExtractAs.Template, _))
      .leftMap(e => InternalError(Some(Symbol("activeContracts")), e.shows))

  private def contracts(
      response: lav2.command_service.SubmitAndWaitForTransactionTreeResponse
  ): Error \/ List[Contract[lav2.value.Value]] =
    response.transaction
      .toRightDisjunction(
        InternalError(
          Some(Symbol("contracts")),
          s"Received response without transaction: $response",
        )
      )
      .flatMap(contracts)

  private def contracts(
      tx: lav2.transaction.TransactionTree
  ): Error \/ List[Contract[lav2.value.Value]] =
    Contract
      .fromTransactionTree(tx)
      .leftMap(e => InternalError(Some(Symbol("contracts")), e.shows))
      .map(_.toList)

  private def exerciseResult(
      a: lav2.command_service.SubmitAndWaitForTransactionTreeResponse
  ): Error \/ lav2.value.Value = {
    val result: Option[lav2.value.Value] = for {
      transaction <- a.transaction: Option[lav2.transaction.TransactionTree]
      exercised <- firstExercisedEvent(transaction): Option[lav2.event.ExercisedEvent]
      exResult <- exercised.exerciseResult: Option[lav2.value.Value]
    } yield exResult

    result.toRightDisjunction(
      InternalError(
        Some(Symbol("choiceArgument")),
        s"Cannot get exerciseResult from the first ExercisedEvent of gRPC response: ${a.toString}",
      )
    )
  }

  private def firstExercisedEvent(
      tx: lav2.transaction.TransactionTree
  ): Option[lav2.event.ExercisedEvent] = {
    val lookup: String => Option[lav2.event.ExercisedEvent] = id =>
      tx.eventsById.get(id).flatMap(_.kind.exercised)
    tx.rootEventIds.collectFirst(Function unlift lookup)
  }
}

object CommandService {
  sealed abstract class Error extends Product with Serializable
  final case class ClientError(
      id: Grpc.Category.PermissionDenied \/ Grpc.Category.InvalidArgument,
      message: String,
  ) extends Error
  final case class GrpcError(status: com.google.rpc.Status) extends Error
  final case class InternalError(id: Option[Symbol], error: Throwable) extends Error
  object InternalError {
    def apply(id: Option[Symbol], message: String): InternalError =
      InternalError(id, new Exception(message))
    def apply(id: Option[Symbol], error: Throwable): InternalError =
      InternalError(id, error)
  }

  private type ET[A] = EitherT[Future, Error, A]

  type ExerciseCommandRef = domain.ResolvedContractRef[lav2.value.Value]
}
