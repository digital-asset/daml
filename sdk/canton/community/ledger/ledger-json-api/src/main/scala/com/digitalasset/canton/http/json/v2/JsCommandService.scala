// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitRequest,
  SubmitAndWaitResponse,
}
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v2.transaction_filter.TransactionFormat
import com.daml.ledger.api.v2.{
  command_completion_service,
  command_service,
  command_submission_service,
  commands,
  completion,
  reassignment_commands,
}
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, v2Endpoint}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsReassignment,
  JsTransaction,
  JsTransactionTree,
}
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf
import io.circe.*
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, webSocketBody}

import scala.concurrent.{ExecutionContext, Future}

class JsCommandService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
    wsConfig: WebsocketConfig,
) extends Endpoints
    with NamedLogging {

  private def commandServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): CommandServiceGrpc.CommandServiceStub =
    ledgerClient.serviceClient(CommandServiceGrpc.stub, token)

  private def commandSubmissionServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub =
    ledgerClient.serviceClient(command_submission_service.CommandSubmissionServiceGrpc.stub, token)

  private def commandCompletionServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub =
    ledgerClient.serviceClient(command_completion_service.CommandCompletionServiceGrpc.stub, token)

  def endpoints() = List(
    withServerLogic(
      JsCommandService.submitAndWait,
      submitAndWait,
    ),
    withServerLogic(
      JsCommandService.submitAndWaitForTransactionEndpoint,
      submitAndWaitForTransaction,
    ),
    withServerLogic(
      JsCommandService.submitAndWaitForReassignmentEndpoint,
      submitAndWaitForReassignment,
    ),
    withServerLogic(
      JsCommandService.submitAndWaitForTransactionTree,
      submitAndWaitForTransactionTree,
    ),
    withServerLogic(
      JsCommandService.submitAsyncEndpoint,
      submitAsync,
    ),
    withServerLogic(
      JsCommandService.submitReassignmentAsyncEndpoint,
      submitReassignmentAsync,
    ),
    websocket(
      JsCommandService.completionStreamEndpoint,
      commandCompletionStream,
    ),
    asList(
      JsCommandService.completionListEndpoint,
      commandCompletionStream,
      timeoutOpenEndedStream = true,
    ),
  )

  private def commandCompletionStream(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    command_completion_service.CompletionStreamRequest,
    command_completion_service.CompletionStreamResponse,
    NotUsed,
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    prepareSingleWsStream(
      commandCompletionServiceClient(caller.token()).completionStream,
      Future.successful[command_completion_service.CompletionStreamResponse],
    )
  }

  def submitAndWait(callerContext: CallerContext): TracedInput[JsCommands] => Future[
    Either[JsCantonError, SubmitAndWaitResponse]
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(req.in)
      submitAndWaitRequest =
        SubmitAndWaitRequest(commands = Some(commands))
      result <- commandServiceClient(callerContext.token())
        .submitAndWait(submitAndWaitRequest)
        .resultToRight
    } yield result
  }

  def submitAndWaitForTransactionTree(
      callerContext: CallerContext
  ): TracedInput[JsCommands] => Future[
    Either[JsCantonError, JsSubmitAndWaitForTransactionTreeResponse]
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    for {

      commands <- protocolConverters.Commands.fromJson(req.in)
      submitAndWaitRequest =
        SubmitAndWaitRequest(commands = Some(commands))
      result <- commandServiceClient(callerContext.token())
        .submitAndWaitForTransactionTree(submitAndWaitRequest)
        .flatMap(r => protocolConverters.SubmitAndWaitTransactionTreeResponse.toJson(r))
        .resultToRight
    } yield result
  }

  def submitAndWaitForTransaction(
      callerContext: CallerContext
  ): TracedInput[JsSubmitAndWaitForTransactionRequest] => Future[
    Either[JsCantonError, JsSubmitAndWaitForTransactionResponse]
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    for {
      submitAndWaitRequest <- protocolConverters.SubmitAndWaitForTransactionRequest.fromJson(req.in)
      result <- commandServiceClient(callerContext.token())
        .submitAndWaitForTransaction(submitAndWaitRequest)
        .flatMap(r => protocolConverters.SubmitAndWaitTransactionResponse.toJson(r))
        .resultToRight
    } yield result
  }

  def submitAndWaitForReassignment(
      callerContext: CallerContext
  ): TracedInput[command_service.SubmitAndWaitForReassignmentRequest] => Future[
    Either[JsCantonError, JsSubmitAndWaitForReassignmentResponse]
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    for {
      result <- commandServiceClient(callerContext.token())
        .submitAndWaitForReassignment(req.in)
        .flatMap(r => protocolConverters.SubmitAndWaitForReassignmentResponse.toJson(r))
        .resultToRight
    } yield result
  }

  private def submitAsync(callerContext: CallerContext): TracedInput[JsCommands] => Future[
    Either[JsCantonError, command_submission_service.SubmitResponse]
  ] = req => {
    implicit val tc: TraceContext = req.traceContext
    for {
      commands <- protocolConverters.Commands.fromJson(req.in)
      submitRequest =
        command_submission_service.SubmitRequest(commands = Some(commands))
      result <- commandSubmissionServiceClient(callerContext.token())
        .submit(submitRequest)
        .resultToRight
    } yield result
  }

  private def submitReassignmentAsync(
      callerContext: CallerContext
  ): TracedInput[command_submission_service.SubmitReassignmentRequest] => Future[
    Either[JsCantonError, command_submission_service.SubmitReassignmentResponse]
  ] = req => {
    commandSubmissionServiceClient(callerContext.token())(req.traceContext)
      .submitReassignment(req.in)
      .resultToRight
  }
}

final case class JsSubmitAndWaitForTransactionRequest(
    commands: JsCommands,
    transactionFormat: TransactionFormat,
)

final case class JsSubmitAndWaitForTransactionTreeResponse(
    transactionTree: JsTransactionTree
)

final case class JsSubmitAndWaitForTransactionResponse(
    transaction: JsTransaction
)

final case class JsSubmitAndWaitForReassignmentResponse(
    reassignment: JsReassignment
)

object JsCommand {
  sealed trait Command
  final case class CreateCommand(
      templateId: String,
      createArguments: Json,
  ) extends Command

  final case class ExerciseCommand(
      templateId: String,
      contractId: String,
      choice: String,
      choiceArgument: Json,
  ) extends Command

  final case class CreateAndExerciseCommand(
      templateId: String,
      createArguments: Json,
      choice: String,
      choiceArgument: Json,
  ) extends Command

  final case class ExerciseByKeyCommand(
      templateId: String,
      contractKey: Json,
      choice: String,
      choiceArgument: Json,
  ) extends Command
}

final case class JsCommands(
    commands: Seq[JsCommand.Command],
    commandId: String,
    actAs: Seq[String],
    userId: Option[String] = None,
    readAs: Seq[String] = Seq.empty,
    workflowId: Option[String] = None,
    deduplicationPeriod: Option[DeduplicationPeriod] = None,
    minLedgerTimeAbs: Option[protobuf.timestamp.Timestamp] = None,
    minLedgerTimeRel: Option[protobuf.duration.Duration] = None,
    submissionId: Option[String] = None,
    disclosedContracts: Seq[com.daml.ledger.api.v2.commands.DisclosedContract] = Seq.empty,
    synchronizerId: Option[String] = None,
    packageIdSelectionPreference: Seq[String] = Seq.empty,
)

object JsCommandService extends DocumentationEndpoints {
  import JsCommandServiceCodecs.*
  private lazy val commands = v2Endpoint.in(sttp.tapir.stringToPath("commands"))

  val submitAndWaitForTransactionEndpoint = commands.post
    .in(sttp.tapir.stringToPath("submit-and-wait-for-transaction"))
    .in(jsonBody[JsSubmitAndWaitForTransactionRequest])
    .out(jsonBody[JsSubmitAndWaitForTransactionResponse])
    .description("Submit a batch of commands and wait for the transaction response")

  val submitAndWaitForReassignmentEndpoint = commands.post
    .in(sttp.tapir.stringToPath("submit-and-wait-for-reassignment"))
    .in(jsonBody[command_service.SubmitAndWaitForReassignmentRequest])
    .out(jsonBody[JsSubmitAndWaitForReassignmentResponse])
    .description("Submit a batch of reassignment commands and wait for the reassignment response")

  val submitAndWaitForTransactionTree = commands.post
    .in(sttp.tapir.stringToPath("submit-and-wait-for-transaction-tree"))
    .in(jsonBody[JsCommands])
    .out(jsonBody[JsSubmitAndWaitForTransactionTreeResponse])
    .description("Submit a batch of commands and wait for the transaction trees response")

  val submitAndWait = commands.post
    .in(sttp.tapir.stringToPath("submit-and-wait"))
    .in(jsonBody[JsCommands])
    .out(jsonBody[SubmitAndWaitResponse])
    .description("Submit a batch of commands and wait for the completion details")

  val submitAsyncEndpoint = commands.post
    .in(sttp.tapir.stringToPath("async"))
    .in(sttp.tapir.stringToPath("submit"))
    .in(jsonBody[JsCommands])
    .out(jsonBody[command_submission_service.SubmitResponse])
    .description("Submit a command asynchronously")

  val submitReassignmentAsyncEndpoint =
    commands.post
      .in(sttp.tapir.stringToPath("async"))
      .in(sttp.tapir.stringToPath("submit-reassignment"))
      .in(jsonBody[command_submission_service.SubmitReassignmentRequest])
      .out(jsonBody[command_submission_service.SubmitReassignmentResponse])
      .description("Submit reassignment command asynchronously")

  val completionStreamEndpoint =
    commands.get
      .in(sttp.tapir.stringToPath("completions"))
      .out(
        webSocketBody[
          command_completion_service.CompletionStreamRequest,
          CodecFormat.Json,
          Either[JsCantonError, command_completion_service.CompletionStreamResponse],
          CodecFormat.Json,
        ](PekkoStreams)
      )
      .description("Get completions stream")

  val completionListEndpoint =
    commands.post
      .in(sttp.tapir.stringToPath("completions"))
      .in(jsonBody[command_completion_service.CompletionStreamRequest])
      .out(jsonBody[Seq[command_completion_service.CompletionStreamResponse]])
      .inStreamListParams()
      .description("Query completions list (blocking call)")

  override def documentation: Seq[AnyEndpoint] = Seq(
    submitAndWait,
    submitAndWaitForTransactionEndpoint,
    submitAndWaitForReassignmentEndpoint,
    submitAndWaitForTransactionTree,
    submitAsyncEndpoint,
    submitReassignmentAsyncEndpoint,
    completionStreamEndpoint,
    completionListEndpoint,
  )
}

object JsCommandServiceCodecs {
  import JsSchema.config
  import JsSchema.JsServicesCommonCodecs.*
  import io.circe.generic.extras.auto.*

  implicit val deduplicationPeriodRW: Codec[DeduplicationPeriod] = deriveConfiguredCodec // ADT

  implicit val deduplicationPeriodDeduplicationDurationRW
      : Codec[DeduplicationPeriod.DeduplicationDuration] = deriveRelaxedCodec
  implicit val deduplicationPeriodDeduplicationOffsetRW
      : Codec[DeduplicationPeriod.DeduplicationOffset] = deriveRelaxedCodec

  implicit val durationRW: Codec[protobuf.duration.Duration] = deriveRelaxedCodec

  implicit val jsSubmitAndWaitRequestRW: Codec[JsSubmitAndWaitForTransactionRequest] =
    deriveConfiguredCodec

  implicit val jsSubmitAndWaitForTransactionResponseRW
      : Codec[JsSubmitAndWaitForTransactionResponse] = deriveConfiguredCodec

  implicit val submitAndWaitForReassignmentRequestRW
      : Codec[command_service.SubmitAndWaitForReassignmentRequest] = deriveRelaxedCodec

  implicit val jsSubmitAndWaitForReassignmentResponseRW
      : Codec[JsSubmitAndWaitForReassignmentResponse] = deriveConfiguredCodec

  implicit val submitResponseRW: Codec[command_submission_service.SubmitResponse] =
    deriveRelaxedCodec

  implicit val submitAndWaitResponseRW: Codec[SubmitAndWaitResponse] =
    deriveRelaxedCodec

  implicit val submitReassignmentResponseRW
      : Codec[command_submission_service.SubmitReassignmentResponse] =
    deriveRelaxedCodec

  implicit val jsCommandsRW: Codec[JsCommands] = deriveConfiguredCodec

  implicit val jsCommandCommandRW: Codec[JsCommand.Command] = deriveConfiguredCodec
  implicit val jsCommandCreateRW: Codec[JsCommand.CreateCommand] = deriveConfiguredCodec
  implicit val jsCommandExerciseRW: Codec[JsCommand.ExerciseCommand] = deriveConfiguredCodec

  implicit val commandCompletionRW: Codec[command_completion_service.CompletionStreamRequest] =
    deriveRelaxedCodec

  implicit val reassignmentCommandsRW: Codec[reassignment_commands.ReassignmentCommands] =
    deriveRelaxedCodec

  implicit val reassignmentCommandRW: Codec[reassignment_commands.ReassignmentCommand] =
    deriveRelaxedCodec

  implicit val reassignmentCommandCommandRW
      : Codec[reassignment_commands.ReassignmentCommand.Command] = deriveConfiguredCodec // ADT

  implicit val reassignmentUnassignCommandRW: Codec[reassignment_commands.UnassignCommand] =
    deriveRelaxedCodec

  implicit val reassignmentAssignCommandRW: Codec[reassignment_commands.AssignCommand] =
    deriveRelaxedCodec

  implicit val reassignmentCommandUnassignCommandRW
      : Codec[reassignment_commands.ReassignmentCommand.Command.UnassignCommand] =
    deriveRelaxedCodec

  implicit val reassignmentCommandAssignCommandRW
      : Codec[reassignment_commands.ReassignmentCommand.Command.AssignCommand] = deriveRelaxedCodec

  implicit val submitReassignmentRequestRW: Codec[
    command_submission_service.SubmitReassignmentRequest
  ] = deriveRelaxedCodec

  implicit val completionStreamResponseRW
      : Codec[command_completion_service.CompletionStreamResponse] = deriveRelaxedCodec
  implicit val completionResponseRW
      : Codec[command_completion_service.CompletionStreamResponse.CompletionResponse] =
    deriveConfiguredCodec // ADT
  implicit val completionResponseOffsetCheckpointRW: Codec[
    command_completion_service.CompletionStreamResponse.CompletionResponse.OffsetCheckpoint
  ] = deriveRelaxedCodec
  implicit val completionResponseOffsetCompletionRW: Codec[
    command_completion_service.CompletionStreamResponse.CompletionResponse.Completion
  ] = deriveRelaxedCodec

  implicit val completionRW: Codec[
    completion.Completion
  ] = deriveRelaxedCodec

  implicit val completionDeduplicationPeriodRW: Codec[
    completion.Completion.DeduplicationPeriod
  ] = deriveConfiguredCodec // ADT

  implicit val disclosedContractRW: Codec[
    commands.DisclosedContract
  ] = deriveRelaxedCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val reassignmentCommandCommandSchema
      : Schema[reassignment_commands.ReassignmentCommand.Command] = Schema.oneOfWrapped

  implicit val deduplicationPeriodSchema: Schema[DeduplicationPeriod] =
    Schema.oneOfWrapped

  implicit val completionDeduplicationPeriodSchema
      : Schema[completion.Completion.DeduplicationPeriod] =
    Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsCommandSchema: Schema[JsCommand.Command] =
    Schema.oneOfWrapped

  implicit val completionStreamResponseSchema
      : Schema[command_completion_service.CompletionStreamResponse.CompletionResponse] =
    Schema.oneOfWrapped

}
