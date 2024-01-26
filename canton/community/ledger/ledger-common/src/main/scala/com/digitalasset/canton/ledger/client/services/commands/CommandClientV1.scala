// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

/** Enables easy access to command services and high level operations on top of them.
  *
  * @param commandSubmissionService gRPC service reference.
  * @param commandCompletionService gRPC service reference.
  * @param applicationId            Will be applied to submitted commands.
  * @param config                   Options for changing behavior.
  */
final class CommandClientV1(
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    applicationId: String,
    config: CommandClientConfiguration,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

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
}
