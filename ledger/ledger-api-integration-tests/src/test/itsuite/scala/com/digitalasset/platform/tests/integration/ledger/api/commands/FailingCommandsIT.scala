// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.platform.apitesting.LedgerContext
import com.digitalasset.util.Ctx
import io.grpc.Status
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class FailingCommandsIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerCommandUtils
    with SuiteResourceManagementAroundAll
    with Matchers
    with OptionValues {

  "Command Client" when {
    "provided a failing command" should {

      "fail with the expected status on a ledger Id mismatch" in allFixtures { ctx =>
        val contexts = 1 to 10

        val resultsF = Source(contexts.map(i => Ctx(i, failingRequest)))
          .via(ctx.commandClientWithoutTime(testNotLedgerId).submissionFlow)
          .runWith(Sink.head)

        resultsF.failed map { t =>
          t.getMessage shouldEqual "Failing fast on submission request of command asyncFail " +
            s"with invalid ledger ID ledgerId (client expected $testNotLedgerId)"
        }
      }

      "fail with the expected status on a ledger Id mismatch via sync service (multiple reqs)" in allFixtures {
        ctx =>
          val contexts = 1 to 10

          val cmd1 = SubmitAndWaitRequest(
            failingRequest.commands.map(
              _.update(_.ledgerId := testNotLedgerId.unwrap, _.commandId := "sync ledgerId 1")))
          val result1F = ctx.commandService.submitAndWait(cmd1).failed
          val result2F = ctx.commandService
            .submitAndWait(cmd1.update(_.commands.commandId := "sync ledgerId 2"))
            .failed

          for {
            result1 <- result1F
            result2 <- result2F
          } yield {
            val isNotFound = IsStatusException(Status.Code.NOT_FOUND) _
            isNotFound(result1)
            isNotFound(result2)
          }
      }

      "return the failures for commands that fail in an async manner" in allFixtures { ctx =>
        val submittedCtx = 0

        for {
          client <- ctx.commandClient()
          tracker <- client.trackCommands[Int](List(failingRequest.getCommands.party))
          result <- Source
            .single(
              Ctx(submittedCtx, failingRequest.update(_.commands.ledgerId := ctx.ledgerId.unwrap)))
            .via(tracker)
            .runWith(Sink.head)
        } yield {
          result.value should matchPattern {
            case Completion(`failingCommandId`, Some(status), _, _) if status.code == 3 =>
          }
        }
      }

      "return the failures for commands that fail via sync service" in allFixtures { ctx =>
        val cmd1 = SubmitAndWaitRequest(
          failingRequest.commands.map(
            _.update(_.ledgerId := ctx.ledgerId.unwrap, _.commandId := "wrong template id 1")))
        val cmd2 = cmd1.update(_.commands.commandId := "wrong template id 2")
        assertCommandsResultInSameError(ctx, cmd1, cmd2, Status.Code.INVALID_ARGUMENT)
      }

      "return the failures for duplicate commands that fail via sync service" in allFixtures {
        ctx =>
          val cmd = SubmitAndWaitRequest(
            failingRequest.commands.map(
              _.update(
                _.ledgerId := ctx.ledgerId.unwrap,
                _.commandId := "wrong template id and same commandId")))

          assertCommandsResultInSameError(ctx, cmd, cmd, Status.Code.INVALID_ARGUMENT)
      }

      "return the failures for empty submissions that fail via sync service" in allFixtures { ctx =>
        val cmd = SubmitAndWaitRequest()
        assertCommandsResultInSameError(ctx, cmd, cmd, Status.Code.INVALID_ARGUMENT)
      }
    }
  }
  private def assertCommandsResultInSameError(
      ctx: LedgerContext,
      cmd1: SubmitAndWaitRequest,
      cmd2: SubmitAndWaitRequest,
      expectedCode: Status.Code) = {
    val result1F = ctx.commandService.submitAndWait(cmd1)
    val result2F = ctx.commandService.submitAndWait(cmd2)
    for {
      result1 <- result1F.failed
      result2 <- result2F.failed
    } yield {
      val isInvalidArgument = IsStatusException(expectedCode) _
      isInvalidArgument(result1)
      isInvalidArgument(result2)
    }
  }

}
