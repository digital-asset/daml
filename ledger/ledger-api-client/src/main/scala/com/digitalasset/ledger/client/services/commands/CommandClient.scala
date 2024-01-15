// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service.CompletionEndResponse
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Enables easy access to command services and high level operations on top of them.
  *
  * @param commandSubmissionService gRPC service reference.
  * @param commandCompletionService gRPC service reference.
  * @param ledgerId                 Will be applied to submitted commands.
  * @param applicationId            Will be applied to submitted commands.
  * @param config                   Options for changing behavior.
  */
final class CommandClient(
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    ledgerId: LedgerId,
    applicationId: String,
    config: CommandClientConfiguration,
    logger: Logger = LoggerFactory.getLogger(getClass),
)(implicit esf: ExecutionSequencerFactory) {
  private val it = new withoutledgerid.CommandClient(
    commandSubmissionService,
    commandCompletionService,
    applicationId,
    config,
    logger,
  )

  type TrackCommandFlow[Context] =
    Flow[
      Ctx[Context, CommandSubmission],
      Ctx[Context, Either[CompletionFailure, CompletionSuccess]],
      Materialized[
        NotUsed,
        Context,
      ],
    ]

  /** Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger. In order to get that semantic, use [[trackCommands]] or [[trackCommandsUnbounded]].
    */
  def submitSingleCommand(
      submitRequest: SubmitRequest,
      token: Option[String] = None,
  ): Future[Empty] =
    it.submitSingleCommand(submitRequest, token)

  /** Submits and tracks a single command. High frequency usage is discouraged as it causes a dedicated completion
    * stream to be established and torn down.
    */
  def trackSingleCommand(submitRequest: SubmitRequest, token: Option[String] = None)(implicit
      mat: Materializer
  ): Future[Either[CompletionFailure, CompletionSuccess]] =
    it.trackSingleCommand(submitRequest, ledgerId, token)

  /** Tracks the results (including timeouts) of incoming commands.
    * Applies a maximum bound for in-flight commands which have been submitted, but not confirmed through command completions.
    *
    * The resulting flow will backpressure if downstream backpressures, independently of the number of in-flight commands.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommands[Context](parties: Seq[String], token: Option[String] = None)(implicit
      ec: ExecutionContext
  ): Future[TrackCommandFlow[Context]] = it.trackCommands(parties, ledgerId, token)

  /** Tracks the results (including timeouts) of incoming commands.
    *
    * The resulting flow will backpressure if downstream backpressures, independently of the number of in-flight commands.
    *
    * @param parties Commands that have a submitting party which is not part of this collection will fail the stream.
    */
  def trackCommandsUnbounded[Context](parties: Seq[String], token: Option[String] = None)(implicit
      ec: ExecutionContext
  ): Future[TrackCommandFlow[Context]] = it.trackCommandsUnbounded(parties, ledgerId, token)

  def completionSource(
      parties: Seq[String],
      offset: LedgerOffset,
      token: Option[String] = None,
  ): Source[CompletionStreamElement, NotUsed] =
    it.completionSource(parties, offset, ledgerId, token)

  def submissionFlow[Context](
      token: Option[String] = None
  ): Flow[Ctx[Context, CommandSubmission], Ctx[Context, Try[Empty]], NotUsed] =
    it.submissionFlow(ledgerId, token)

  def getCompletionEnd(token: Option[String] = None): Future[CompletionEndResponse] =
    it.getCompletionEnd(ledgerId, token)
}
