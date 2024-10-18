// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.google.protobuf
import com.daml.ledger.api.v2.command_submission_service
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsTransaction,
  JsTransactionTree,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import io.circe.*
import io.circe.generic.semiauto.deriveCodec

import scala.concurrent.{ExecutionContext, Future}

class JsCommandService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {

  import JsCommandServiceCodecs.*

  private lazy val commands = v2Endpoint.in(sttp.tapir.stringToPath("commands"))

  private def commandServiceClient(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): CommandServiceGrpc.CommandServiceStub =
    ledgerClient.serviceClient(CommandServiceGrpc.stub, token)

  private def commandSubmissionServiceClient(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub =
    ledgerClient.serviceClient(command_submission_service.CommandSubmissionServiceGrpc.stub, token)

  def endpoints() = List(
    jsonWithBody(
      commands.post
        .in(sttp.tapir.stringToPath("submit-and-wait"))
        .description("Submit a batch of commands and wait for the completion details"),
      submitAndWait,
    ),
    jsonWithBody(
      commands.post
        .in(sttp.tapir.stringToPath("submit-and-wait-for-transaction"))
        .description("Submit a batch of commands and wait for the flat transactions response"),
      submitAndWaitForTransaction,
    ),
    jsonWithBody(
      commands.post
        .in(sttp.tapir.stringToPath("submit-and-wait-for-transaction-tree"))
        .description("Submit a batch of commands and wait for the transaction trees response"),
      submitAndWaitForTransactionTree,
    ),
    jsonWithBody(
      commands.post
        .in(sttp.tapir.stringToPath("async"))
        .in(sttp.tapir.stringToPath("submit"))
        .description("Submit a command asynchronously"),
      submitAsync,
    ),
    jsonWithBody(
      commands.post
        .in(sttp.tapir.stringToPath("async"))
        .in(sttp.tapir.stringToPath("submit-reassignment"))
        .description("Submit reassignment command asynchronously"),
      submitReassignmentAsync,
    ),
  )

  def submitAndWait(callerContext: CallerContext): (
      TracedInput[Unit],
      JsCommands,
  ) => Future[
    Either[JsCantonError, JsSubmitAndWaitResponse]
  ] = (req, body) => {
    implicit val token = callerContext.token()
    implicit val tc = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(body)
      submitAndWaitRequest =
        SubmitAndWaitRequest(commands = Some(commands))
      result <- commandServiceClient(callerContext.token())
        .submitAndWait(submitAndWaitRequest)
        .map(protocolConverters.SubmitAndWaitResponse.toJson)(
          ExecutionContext.parasitic
        )
        .resultToRight
    } yield result
  }

  def submitAndWaitForTransactionTree(callerContext: CallerContext): (
      TracedInput[Unit],
      JsCommands,
  ) => Future[
    Either[JsCantonError, JsSubmitAndWaitForTransactionTreeResponse]
  ] = (req, body) => {
    implicit val token = callerContext.token()
    implicit val tc = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(body)
      submitAndWaitRequest =
        SubmitAndWaitRequest(commands = Some(commands))
      result <- commandServiceClient(callerContext.token())
        .submitAndWaitForTransactionTree(submitAndWaitRequest)
        .flatMap(r =>
          protocolConverters.SubmitAndWaitTransactionTreeResponse.toJson(r)
        )
        .resultToRight
    } yield result
  }

  def submitAndWaitForTransaction(callerContext: CallerContext): (
      TracedInput[Unit],
      JsCommands,
  ) => Future[
    Either[JsCantonError, JsSubmitAndWaitForTransactionResponse]
  ] = (req, body) => {
    implicit val token = callerContext.token()
    implicit val tc = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(body)
      submitAndWaitRequest =
        SubmitAndWaitRequest(commands = Some(commands))
      result <- commandServiceClient(callerContext.token())(req.traceContext)
        .submitAndWaitForTransaction(submitAndWaitRequest)
        .flatMap(r =>
          protocolConverters.SubmitAndWaitTransactionResponse.toJson(r)
        )
        .resultToRight
    } yield result
  }

  private def submitAsync(callerContext: CallerContext): (
      TracedInput[Unit],
      JsCommands,
  ) => Future[
    Either[JsCantonError, command_submission_service.SubmitResponse]
  ] = (req, body) => {
    implicit val token = callerContext.token()
    implicit val tc = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(body)
      submitRequest =
        command_submission_service.SubmitRequest(commands = Some(commands))
      result <- commandSubmissionServiceClient(callerContext.token())
        .submit(submitRequest)
        .resultToRight
    } yield result
  }

  private def submitReassignmentAsync(callerContext: CallerContext): (
      TracedInput[Unit],
      JsSubmitReassignmentRequest,
  ) => Future[
    Either[JsCantonError, command_submission_service.SubmitReassignmentResponse]
  ] = (req, body) => {
    val submitRequest = protocolConverters.JsSubmitReassignmentRequest.fromJson(body)
    commandSubmissionServiceClient(callerContext.token())(req.traceContext)
      .submitReassignment(submitRequest)
      .resultToRight

  }
}

final case class JsSubmitAndWaitForTransactionTreeResponse(
    transaction_tree: JsTransactionTree
)

final case class JsSubmitAndWaitForTransactionResponse(
    transaction: JsTransaction
)

final case class JsSubmitAndWaitResponse(
    update_id: String,
    completion_offset: String,
)

case class JsReassignmentCommand(
    workflow_id: String,
    application_id: String,
    command_id: String,
    submitter: String,
    command: JsReassignmentCommand.JsCommand,
    submission_id: String,
)

object JsReassignmentCommand {
  sealed trait JsCommand

  case class JsUnassignCommand(
      contract_id: String,
      source: String,
      target: String,
  ) extends JsCommand

  case class JsAssignCommand(
      unassign_id: String,
      source: String,
      target: String,
  ) extends JsCommand
}

case class JsSubmitReassignmentRequest(
    reassignment_command: Option[JsReassignmentCommand]
)

object JsCommand {
  sealed trait Command
  final case class CreateCommand(
      template_id: String,
      create_arguments: Json,
  ) extends Command

  final case class ExerciseCommand(
      template_id: String,
      contract_id: String,
      choice: String,
      choice_argument: Json,
  ) extends Command

  final case class CreateAndExerciseCommand(
      template_id: String,
      create_arguments: Json,
      choice: String,
      choice_argument: Json,
  ) extends Command

  final case class ExerciseByKeyCommand(
      template_id: String,
      contract_key: Json,
      choice: String,
      choice_argument: Json,
  ) extends Command
}

final case class JsCommands(
    commands: Seq[JsCommand.Command],
    workflow_id: String,
    application_id: String,
    command_id: String,
    deduplication_period: DeduplicationPeriod,
    min_ledger_time_abs: Option[protobuf.timestamp.Timestamp],
    min_ledger_time_rel: Option[protobuf.duration.Duration],
    act_as: Seq[String],
    read_as: Seq[String],
    submission_id: String,
    disclosed_contracts: Seq[JsDisclosedContract],
    domain_id: String,
    package_id_selection_preference: Seq[String],
)
final case class JsDisclosedContract(
    template_id: String,
    contract_id: String,
    created_event_blob: com.google.protobuf.ByteString,
)

object JsCommandServiceCodecs {

  implicit val deduplicationPeriodRW: Codec[DeduplicationPeriod] = deriveCodec
  implicit val deduplicationPeriodDeduplicationDurationRW
      : Codec[DeduplicationPeriod.DeduplicationDuration] = deriveCodec
  implicit val deduplicationPeriodDeduplicationOffsetRW
      : Codec[DeduplicationPeriod.DeduplicationOffset] = deriveCodec

  implicit val durationRW: Codec[protobuf.duration.Duration] = deriveCodec

  implicit val jsTransactionRW: Codec[JsTransaction] =
    deriveCodec

  implicit val jsSubmitAndWaitForTransactionResponseRW
      : Codec[JsSubmitAndWaitForTransactionResponse] =
    deriveCodec

  implicit val submitResponseRW: Codec[command_submission_service.SubmitResponse] =
    deriveCodec

  implicit val submitReassignmentResponseRW
      : Codec[command_submission_service.SubmitReassignmentResponse] =
    deriveCodec

  implicit val jsReassignmentCommandCommandRW: Codec[JsReassignmentCommand.JsCommand] =
    deriveCodec

  implicit val jsReassignmentCommandRW: Codec[JsReassignmentCommand] =
    deriveCodec

  implicit val jsSubmitReassignmentRequestRW: Codec[JsSubmitReassignmentRequest] =
    deriveCodec

  implicit val jsCommandsRW: Codec[JsCommands] = deriveCodec

  implicit val jsCommandCommandRW: Codec[JsCommand.Command] = deriveCodec
  implicit val jsCommandCreateRW: Codec[JsCommand.CreateCommand] = deriveCodec
  implicit val jsCommandExerciseRW: Codec[JsCommand.ExerciseCommand] = deriveCodec

  implicit val jsDisclosedContractRW: Codec[JsDisclosedContract] = deriveCodec

  implicit val jsSubmitAndWaitResponseRW: Codec[JsSubmitAndWaitResponse] =
    deriveCodec
}
