// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.withoutledgerid

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest,
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.CommandsValidator
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.ledger.client.services.commands._
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.client.services.commands.tracker.TrackedCommandKey
import com.daml.metrics.api.noop.NoOpCounter
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

/** Enables easy access to command services and high level operations on top of them.
  *
  * @param commandSubmissionService gRPC service reference.
  * @param commandCompletionService gRPC service reference.
  * @param applicationId            Will be applied to submitted commands.
  * @param config                   Options for changing behavior.
  */
private[daml] final class CommandClient(
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    applicationId: String,
    config: CommandClientConfiguration,
    logger: Logger = LoggerFactory.getLogger(getClass),
)(implicit esf: ExecutionSequencerFactory) {

  type TrackCommandFlow[Context] =
    Flow[
      Ctx[Context, CommandSubmission],
      Ctx[Context, Either[CompletionFailure, CompletionSuccess]],
      Materialized[
        NotUsed,
        Context,
      ],
    ]

  private val submissionIdGenerator: SubmissionIdGenerator = SubmissionIdGenerator.Random

  /** Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger. In order to get that semantic, use [[trackCommands]] or [[trackCommandsUnbounded]].
    */
  def submitSingleCommand(
      submitRequest: SubmitRequest,
      token: Option[String] = None,
  ): Future[Empty] =
    submit(token)(submitRequest)

  private def submit(token: Option[String])(submitRequest: SubmitRequest): Future[Empty] = {
    logger.debug(
      "Invoking grpc-submission on commandId={}",
      submitRequest.commands.map(_.commandId).getOrElse("no-command-id"),
    )
    LedgerClient
      .stub(commandSubmissionService, token)
      .submit(submitRequest)
  }

  /** Submits and tracks a single command. High frequency usage is discouraged as it causes a dedicated completion
    * stream to be established and torn down.
    */
  def trackSingleCommand(
      submitRequest: SubmitRequest,
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  )(implicit
      mat: Materializer
  ): Future[Either[CompletionFailure, CompletionSuccess]] = {
    implicit val executionContext: ExecutionContextExecutor = mat.executionContext
    val commands = submitRequest.getCommands
    val effectiveActAs = CommandsValidator.effectiveSubmitters(commands).actAs
    for {
      tracker <- trackCommandsUnbounded[Unit](effectiveActAs.toList, ledgerIdToUse, token)
      result <- Source
        .single(Ctx.unit(CommandSubmission(commands)))
        .via(tracker)
        .runWith(Sink.head)
    } yield {
      result.value
    }
  }

  /** Tracks the results (including timeouts) of incoming commands.
    * Applies a maximum bound for in-flight commands which have been submitted, but not confirmed through command completions.
    *
    * The resulting flow will backpressure if downstream backpressures, independently of the number of in-flight commands.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommands[Context](
      parties: Seq[String],
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  )(implicit
      ec: ExecutionContext
  ): Future[TrackCommandFlow[Context]] = {
    for {
      tracker <- trackCommandsUnbounded[Context](parties, ledgerIdToUse, token)
    } yield {
      // The counters are ignored on the client
      MaxInFlight(
        config.maxCommandsInFlight,
        NoOpCounter("capacity"),
        NoOpCounter("name"),
      )
        .joinMat(tracker)(Keep.right)
    }
  }

  /** Tracks the results (including timeouts) of incoming commands.
    *
    * The resulting flow will backpressure if downstream backpressures, independently of the number of in-flight commands.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommandsUnbounded[Context](
      parties: Seq[String],
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  )(implicit
      ec: ExecutionContext
  ): Future[TrackCommandFlow[Context]] =
    for {
      ledgerEnd <- getCompletionEnd(ledgerIdToUse, token)
    } yield {
      partyFilter(parties.toSet)
        .via(
          CommandUpdaterFlow[Context](config, submissionIdGenerator, applicationId, ledgerIdToUse)
        )
        .viaMat(
          CommandTrackerFlow[Context, NotUsed](
            commandSubmissionFlow = CommandSubmissionFlow[(Context, TrackedCommandKey)](
              submit(token),
              config.maxParallelSubmissions,
            ),
            createCommandCompletionSource =
              offset => completionSource(parties, offset, ledgerIdToUse, token),
            startingOffset = ledgerEnd.getOffset,
            maximumCommandTimeout = config.defaultDeduplicationTime,
          )
        )(Keep.right)
    }

  private def partyFilter[Context](allowedParties: Set[String]) =
    Flow[Ctx[Context, CommandSubmission]].map { elem =>
      val commands = elem.value.commands
      val effectiveActAs = CommandsValidator.effectiveSubmitters(commands).actAs
      if (effectiveActAs.subsetOf(allowedParties)) elem
      else
        throw new IllegalArgumentException(
          s"Attempted submission and tracking of command ${commands.commandId} by parties $effectiveActAs while some of those parties are not part of the subscription set $allowedParties."
        )
    }

  def completionSource(
      parties: Seq[String],
      offset: LedgerOffset,
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Source[CompletionStreamElement, NotUsed] = {
    logger.debug(
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
      .via(CommandSubmissionFlow[Context](submit(token), config.maxParallelSubmissions))
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
