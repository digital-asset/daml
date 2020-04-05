// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try
import scalaz.syntax.tag._

/**
  * Enables easy access to command services and high level operations on top of them.
  *
  * @param commandSubmissionService gRPC service reference.
  * @param commandCompletionService gRPC service reference.
  * @param ledgerId                 Will be applied to submitted commands.
  * @param applicationId            Will be applied to submitted commands.
  * @param config                   Options for changing behavior.
  * @param timeProviderO            If defined, it will be used to override LET and MRT values on incoming commands.
  *                                 Let will be set based on current time, and TTL will stay the same or be adjusted based on [[config]]
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class CommandClient(
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    ledgerId: LedgerId,
    applicationId: String,
    config: CommandClientConfiguration,
    timeProviderO: Option[TimeProvider] = None)(implicit esf: ExecutionSequencerFactory) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger. In order to get that semantic, use [[trackCommands]] or [[trackCommandsUnbounded]].
    */
  def submitSingleCommand(
      submitRequest: SubmitRequest,
      token: Option[String] = None): Future[Empty] =
    LedgerClient
      .stub(commandSubmissionService, token)
      .submit(submitRequest)

  /**
    * Submits and tracks a single command. High frequency usage is discouraged as it causes a dedicated completion
    * stream to be established and torn down.
    */
  def trackSingleCommand(submitRequest: SubmitRequest, token: Option[String] = None)(
      implicit mat: Materializer): Future[Completion] = {
    implicit val executionContext: ExecutionContextExecutor = mat.executionContext
    for {
      tracker <- trackCommandsUnbounded[Unit](List(submitRequest.getCommands.party), token)
      result <- Source.single(Ctx.unit(submitRequest)).via(tracker).runWith(Sink.head)
    } yield {
      result.value
    }
  }

  /**
    * Tracks the results (including timeouts) of incoming commands.
    * Applies a maximum bound for in-flight commands which have been submitted, but not confirmed through command completions.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommands[Context](parties: Seq[String], token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[
    Flow[Ctx[Context, SubmitRequest], Ctx[Context, Completion], Materialized[NotUsed, Context]]] = {
    for {
      tracker <- trackCommandsUnbounded[Context](parties, token)
    } yield {
      MaxInFlight(config.maxCommandsInFlight)
        .joinMat(tracker)(Keep.right)
    }
  }

  /**
    * Tracks the results (including timeouts) of incoming commands.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommandsUnbounded[Context](parties: Seq[String], token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[
    Flow[Ctx[Context, SubmitRequest], Ctx[Context, Completion], Materialized[NotUsed, Context]]] =
    for {
      ledgerEnd <- getCompletionEnd(token)
    } yield {
      partyFilter(parties.toSet)
        .via(commandUpdaterFlow[Context])
        .viaMat(CommandTrackerFlow[Context, NotUsed](
          CommandSubmissionFlow[(Context, String)](
            LedgerClient.stub(commandSubmissionService, token).submit,
            config.maxParallelSubmissions),
          offset => completionSource(parties, offset, token),
          ledgerEnd.getOffset,
          () => config.defaultDeduplicationTime,
        ))(Keep.right)
    }

  private def partyFilter[Context](allowedParties: Set[String]) =
    Flow[Ctx[Context, SubmitRequest]].map { elem =>
      val commands = elem.value.getCommands
      if (allowedParties.contains(commands.party)) elem
      else
        throw new IllegalArgumentException(
          s"Attempted submission and tracking of command ${commands.commandId} by party ${commands.party} while that party is not part of the subscription set $allowedParties.")
    }

  def completionSource(
      parties: Seq[String],
      offset: LedgerOffset,
      token: Option[String] = None): Source[CompletionStreamElement, NotUsed] = {
    logger.debug(
      "Connecting to completion service with parties '{}' from offset: '{}'",
      parties,
      offset: Any)
    CommandCompletionSource(
      CompletionStreamRequest(ledgerId.unwrap, applicationId, parties, Some(offset)),
      LedgerClient.stub(commandCompletionService, token).completionStream)
  }

  private def commandUpdaterFlow[Context] =
    Flow[Ctx[Context, SubmitRequest]]
      .map(_.map { r =>
        val commands = r.getCommands
        if (LedgerId(commands.ledgerId) != ledgerId)
          throw new IllegalArgumentException(
            s"Failing fast on submission request of command ${commands.commandId} with invalid ledger ID ${commands.ledgerId} (client expected $ledgerId)")
        else if (commands.applicationId != applicationId)
          throw new IllegalArgumentException(
            s"Failing fast on submission request of command ${commands.commandId} with invalid application ID ${commands.applicationId} (client expected $applicationId)")
        val updateDedupTime = commands.deduplicationTime.orElse(Some(Duration
          .of(config.defaultDeduplicationTime.getSeconds, config.defaultDeduplicationTime.getNano)))
        r.copy(commands = Some(commands.copy(deduplicationTime = updateDedupTime)))
      })

  def submissionFlow[Context](token: Option[String] = None)
    : Flow[Ctx[Context, SubmitRequest], Ctx[Context, Try[Empty]], NotUsed] = {
    Flow[Ctx[Context, SubmitRequest]]
      .via(commandUpdaterFlow)
      .via(
        CommandSubmissionFlow[Context](
          LedgerClient.stub(commandSubmissionService, token).submit,
          config.maxParallelSubmissions))
  }

  def getCompletionEnd(token: Option[String] = None): Future[CompletionEndResponse] =
    LedgerClient
      .stub(commandCompletionService, token)
      .completionEnd(CompletionEndRequest(ledgerId.unwrap))

  /**
    * Returns a new CommandClient which will update ledger effective times and maximum record times based on the new time provider.
    */
  def withTimeProvider(newProvider: Option[TimeProvider]) =
    new CommandClient(
      commandSubmissionService,
      commandCompletionService,
      ledgerId,
      applicationId,
      config,
      newProvider)
}
