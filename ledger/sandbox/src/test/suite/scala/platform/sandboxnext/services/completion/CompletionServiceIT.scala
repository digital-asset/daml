// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext.services.completion

import com.daml.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundEach}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.testing.StreamConsumer
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt

final class CompletionServiceIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxNextFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundEach {

  "CommandCompletionService can stream completions from the beginning" in {
    val lid = ledgerId()
    val party = "partyA"
    val commandId = "commandId"

    val submissionService = CommandSubmissionServiceGrpc.stub(channel)
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid.unwrap))
      _ <- submissionService.submit(dummyCommands(lid, commandId, party))
      completions <- new StreamConsumer[CompletionStreamResponse](
        completionService.completionStream(
          CompletionStreamRequest(
            lid.unwrap,
            MockMessages.applicationId,
            List(party),
            Some(end.getOffset),
          ),
          _,
        )
      ).within(2.seconds)
        .map(_.flatMap(_.completions).map(_.commandId))
    } yield {
      completions shouldBe Vector(commandId)
    }
  }
}
