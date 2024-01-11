// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service.CompletionEndResponse
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.util.Ctx
import com.google.protobuf.empty.Empty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import scala.concurrent.Future
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
    loggerFactory: NamedLoggerFactory,
)(implicit esf: ExecutionSequencerFactory) {
  private val it = new withoutledgerid.CommandClient(
    commandSubmissionService,
    commandCompletionService,
    applicationId,
    config,
    loggerFactory,
  )

  /** Submit a single command. Successful result does not guarantee that the resulting transaction has been written to
    * the ledger.
    */
  def submitSingleCommand(
      submitRequest: SubmitRequest,
      token: Option[String] = None,
  ): Future[Empty] =
    it.submitSingleCommand(submitRequest, token)

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
