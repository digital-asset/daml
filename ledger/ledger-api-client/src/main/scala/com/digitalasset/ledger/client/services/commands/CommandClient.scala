// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest
}
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.digitalasset.util.Ctx
import com.digitalasset.util.akkastreams.MaxInFlight
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
class CommandClient(
    commandSubmissionService: CommandSubmissionService,
    commandCompletionService: CommandCompletionService,
    ledgerId: LedgerId,
    applicationId: String,
    config: CommandClientConfiguration,
    val timeProviderO: Option[TimeProvider] = None)(implicit esf: ExecutionSequencerFactory) {

  private val commandUpdater =
    new CommandUpdater(timeProviderO, config.ttl, config.overrideTtl)

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger. In order to get that semantic, use [[trackCommands]] or [[trackCommandsUnbounded]].
    */
  def submitSingleCommand(submitRequest: SubmitRequest): Future[Empty] =
    commandSubmissionService.submit(
      submitRequest.copy(commands = submitRequest.commands.map(commandUpdater.applyOverrides)))

  /**
    * Submits and tracks a single command. High frequency usage is discouraged as it causes a dedicated completion
    * stream to be established and torn down.
    */
  def trackSingleCommand(submitRequest: SubmitRequest)(
      implicit mat: Materializer): Future[Completion] = {
    implicit val executionContext: ExecutionContextExecutor = mat.executionContext
    for {
      tracker <- trackCommandsUnbounded[Unit](List(submitRequest.getCommands.party))
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
  def trackCommands[Context](parties: Seq[String])(implicit ec: ExecutionContext): Future[
    Flow[Ctx[Context, SubmitRequest], Ctx[Context, Completion], Materialized[NotUsed, Context]]] = {
    for {
      tracker <- trackCommandsUnbounded[Context](parties)
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
  def trackCommandsUnbounded[Context](parties: Seq[String])(implicit ec: ExecutionContext): Future[
    Flow[Ctx[Context, SubmitRequest], Ctx[Context, Completion], Materialized[NotUsed, Context]]] =
    for {
      ledgerEnd <- getCompletionEnd
    } yield {
      partyFilter(parties.toSet)
        .via(commandUpdaterFlow[Context])
        .viaMat(CommandTrackerFlow[Context, NotUsed](
          CommandSubmissionFlow[(Context, String)](
            commandSubmissionService.submit,
            config.maxParallelSubmissions),
          offset => completionSource(parties, offset),
          ledgerEnd.getOffset
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
      offset: LedgerOffset): Source[CompletionStreamElement, NotUsed] = {
    logger.debug(
      "Connecting to completion service with parties '{}' from offset: '{}'",
      parties,
      offset: Any)
    CommandCompletionSource(
      CompletionStreamRequest(ledgerId.unwrap, applicationId, parties, Some(offset)),
      commandCompletionService.completionStream)
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
        r.copy(commands = r.commands.map(commandUpdater.applyOverrides))
      })

  def submissionFlow[Context]
    : Flow[Ctx[Context, SubmitRequest], Ctx[Context, Try[Empty]], NotUsed] = {
    Flow[Ctx[Context, SubmitRequest]]
      .via(commandUpdaterFlow)
      .via(
        CommandSubmissionFlow[Context](
          commandSubmissionService.submit,
          config.maxParallelSubmissions))
  }

  def getCompletionEnd: Future[CompletionEndResponse] = {
    commandCompletionService.completionEnd(CompletionEndRequest(ledgerId.unwrap))
  }

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
