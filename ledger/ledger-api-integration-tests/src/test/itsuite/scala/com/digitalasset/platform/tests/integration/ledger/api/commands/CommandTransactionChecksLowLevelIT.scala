// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.platform.apitesting.{CommandTransactionChecks, LedgerContext}
import com.digitalasset.util.Ctx

import scala.concurrent.Future

class CommandTransactionChecksLowLevelIT extends CommandTransactionChecks {
  override protected def submitCommand(
      ctx: LedgerContext,
      submitRequest: SubmitRequest): Future[Completion] = {
    for {
      commandClient <- ctx.commandClient()
      tracker <- commandClient.trackCommands[Int](List(submitRequest.getCommands.party))
      completion <- Source
        .single(Ctx(0, submitRequest))
        .via(tracker)
        .runWith(Sink.head)
    } yield completion.value
  }
}
