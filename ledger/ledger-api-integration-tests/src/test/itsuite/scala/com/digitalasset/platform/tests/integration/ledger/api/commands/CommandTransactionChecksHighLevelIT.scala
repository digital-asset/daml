// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.platform.apitesting.{CommandTransactionChecks, LedgerContext}
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException

import scala.concurrent.Future

class CommandTransactionChecksHighLevelIT extends CommandTransactionChecks {
  private[this] def emptyToCompletion(
      commandId: String,
      emptyF: Future[Empty]): Future[Completion] =
    emptyF
      .map(_ => Completion(commandId, Some(Status(io.grpc.Status.OK.getCode.value(), ""))))
      .recover {
        case sre: StatusRuntimeException =>
          Completion(
            commandId,
            Some(Status(sre.getStatus.getCode.value(), sre.getStatus.getDescription)))
      }

  override protected def submitCommand(
      ctx: LedgerContext,
      submitRequest: SubmitRequest): Future[Completion] = {
    emptyToCompletion(
      submitRequest.commands.value.commandId,
      ctx.commandService.submitAndWait(
        SubmitAndWaitRequest(submitRequest.commands, submitRequest.traceContext)))
  }
}
