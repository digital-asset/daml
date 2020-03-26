// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import io.grpc.StatusRuntimeException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier
}

object ScriptLedgerClient {

  sealed trait Command
  final case class CreateCommand(templateId: Identifier, argument: Value[AbsoluteContractId])
      extends Command
  final case class ExerciseCommand(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command
  final case class ExerciseByKeyCommand(
      templateId: Identifier,
      key: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command
  final case class CreateAndExerciseCommand(
      templateId: Identifier,
      template: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command

  sealed trait CommandResult
  final case class CreateResult(contractId: AbsoluteContractId) extends CommandResult
  final case class ExerciseResult(
      templateId: Identifier,
      choice: String,
      result: Value[AbsoluteContractId])
      extends CommandResult

  final case class ActiveContract(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      argument: Value[AbsoluteContractId])
}

// This abstracts over the interaction with the ledger. This allows
// us to plug in something that interacts with the JSON API as well as
// something that works against the gRPC API.
trait ScriptLedgerClient {
  def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]]

  def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[SParty]
}

class GrpcLedgerClient(val grpcClient: LedgerClient) extends ScriptLedgerClient {
  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val filter = TransactionFilter(
      List((party.value, Filters(Some(InclusiveFilters(Seq(toApiIdentifier(templateId))))))).toMap)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
    acsResponses.map(acsPages =>
      acsPages.flatMap(page =>
        page.activeContracts.map(createdEvent => {
          val argument = ValueValidator.validateRecord(createdEvent.getCreateArguments) match {
            case Left(err) => throw new ConverterException(err.toString)
            case Right(argument) => argument
          }
          val cid = ContractIdString.fromString(createdEvent.contractId) match {
            case Left(err) => throw new ConverterException(err)
            case Right(cid) => AbsoluteContractId(cid)
          }
          ScriptLedgerClient.ActiveContract(templateId, cid, argument)
        })))
  }

  override def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val ledgerCommands = commands.traverseU(toCommand(_)) match {
      case Left(err) => throw new ConverterException(err)
      case Right(cmds) => cmds
    }
    val apiCommands = Commands(
      party = party.value,
      commands = ledgerCommands,
      ledgerId = grpcClient.ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
    )
    val request = SubmitAndWaitRequest(Some(apiCommands))
    val transactionTreeF = grpcClient.commandServiceClient
      .submitAndWaitForTransactionTree(request)
      .map(Right(_))
      .recover({ case s: StatusRuntimeException => Left(s) })
    transactionTreeF.map(r =>
      r.right.map(transactionTree => {
        val events = transactionTree.getTransaction.rootEventIds
          .map(evId => transactionTree.getTransaction.eventsById(evId))
          .toList
        events.traverseU(fromTreeEvent(_)) match {
          case Left(err) => throw new ConverterException(err)
          case Right(results) => results
        }
      }))
  }

  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    grpcClient.partyManagementClient
      .allocateParty(Some(partyIdHint), Some(displayName))
      .map(r => SParty(r.party))
  }

  private def toCommand(command: ScriptLedgerClient.Command): Either[String, Command] =
    command match {
      case ScriptLedgerClient.CreateCommand(templateId, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command().withCreate(CreateCommand(Some(toApiIdentifier(templateId)), Some(arg)))
      case ScriptLedgerClient.ExerciseCommand(templateId, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield
          Command().withExercise(
            ExerciseCommand(Some(toApiIdentifier(templateId)), contractId.coid, choice, Some(arg)))
      case ScriptLedgerClient.ExerciseByKeyCommand(templateId, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withExerciseByKey(
            ExerciseByKeyCommand(
              Some(toApiIdentifier(templateId)),
              Some(key),
              choice,
              Some(argument)))
      case ScriptLedgerClient.CreateAndExerciseCommand(templateId, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withCreateAndExercise(
            CreateAndExerciseCommand(
              Some(toApiIdentifier(templateId)),
              Some(template),
              choice,
              Some(argument)))
    }

  private def fromTreeEvent(ev: TreeEvent): Either[String, ScriptLedgerClient.CommandResult] =
    ev match {
      case TreeEvent(TreeEvent.Kind.Created(created)) =>
        for {
          cid <- ContractIdString.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(AbsoluteContractId(cid))
      case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
        for {
          result <- ValueValidator.validateValue(exercised.getExerciseResult).left.map(_.toString)
          templateId <- Converter.fromApiIdentifier(exercised.getTemplateId)
        } yield ScriptLedgerClient.ExerciseResult(templateId, exercised.choice, result)
      case TreeEvent(TreeEvent.Kind.Empty) =>
        throw new ConverterException("Invalid tree event Empty")
    }
}
