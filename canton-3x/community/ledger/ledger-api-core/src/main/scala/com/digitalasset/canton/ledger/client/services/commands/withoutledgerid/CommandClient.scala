// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands.withoutledgerid

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest,
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.ledger.client.services.commands.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.util.Ctx
import com.google.protobuf.empty.Empty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import scalaz.syntax.tag.*

import scala.concurrent.Future
import scala.util.Try

/** Enables easy access to command services and high level operations on top of them.
  *
  * @param commandSubmissionService gRPC service reference.
  * @param commandCompletionService gRPC service reference.
  * @param applicationId            Will be applied to submitted commands.
  * @param config                   Options for changing behavior.
  */
final class CommandClient(
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    applicationId: String,
    config: CommandClientConfiguration,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit esf: ExecutionSequencerFactory)
    extends NamedLogging {

  private val submissionIdGenerator: SubmissionIdGenerator = SubmissionIdGenerator.Random

  /** Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger.
    */
  def submitSingleCommand(
      submitRequest: SubmitRequest,
      token: Option[String] = None,
  ): Future[Empty] =
    submit(token)(submitRequest)

  private def submit(token: Option[String])(submitRequest: SubmitRequest): Future[Empty] = {
    noTracingLogger.debug(
      "Invoking grpc-submission on commandId={}",
      submitRequest.commands.map(_.commandId).getOrElse("no-command-id"),
    )
    LedgerClient
      .stub(commandSubmissionService, token)
      .submit(submitRequest)
  }

  def completionSource(
      parties: Seq[String],
      offset: LedgerOffset,
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Source[CompletionStreamElement, NotUsed] = {
    noTracingLogger.debug(
      "Connecting to completion service with parties '{}' from offset: '{}'",
      parties,
      offset: Any,
    )
    CommandCompletionSource(
      CompletionStreamRequest(ledgerIdToUse.unwrap, applicationId, parties, Some(offset)),
      LedgerClient.stub(commandCompletionService, token).completionStream,
    )
  }

  def submissionFlow[Context](
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Flow[Ctx[Context, CommandSubmission], Ctx[Context, Try[Empty]], NotUsed] = {
    Flow[Ctx[Context, CommandSubmission]]
      .via(CommandUpdaterFlow[Context](config, submissionIdGenerator, applicationId, ledgerIdToUse))
      .via(
        CommandSubmissionFlow[Context](submit(token), config.maxParallelSubmissions, loggerFactory)
      )
  }

  def getCompletionEnd(
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Future[CompletionEndResponse] =
    LedgerClient
      .stub(commandCompletionService, token)
      .completionEnd(
        CompletionEndRequest(ledgerIdToUse.unwrap)
      )
}
