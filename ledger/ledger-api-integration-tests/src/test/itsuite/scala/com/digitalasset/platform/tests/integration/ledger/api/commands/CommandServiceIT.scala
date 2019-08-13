// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.tests.integration.ledger.api.TransactionServiceHelpers
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.{AsyncWordSpec, Matchers}
import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll {

  val helpers = new TransactionServiceHelpers(config)

  private def request(
      ctx: LedgerContext,
      id: String = UUID.randomUUID().toString,
      ledgerId: Option[String] = None) =
    helpers.applyTime(
      MockMessages.submitAndWaitRequest
        .update(
          _.commands.ledgerId := ledgerId.getOrElse(ctx.ledgerId.unwrap),
          _.commands.commandId := id)
        .copy(traceContext = None),
      ctx
    )

  "Commands Service" when {
    "submitting commands" should {
      "complete with an empty response if successful" in allFixtures { ctx =>
        for {
          req <- request(ctx)
          resp <- ctx.commandService.submitAndWait(req)
        } yield {
          resp shouldEqual Empty()
        }
      }

      "return the transaction id if successful" in allFixtures { ctx =>
        for {
          req <- request(ctx)
          resp <- ctx.commandService.submitAndWaitForTransactionId(req)
        } yield {
          resp.transactionId should not be empty

        }
      }

      "return the flat transaction if successful" in allFixtures { ctx =>
        for {
          req <- request(ctx)
          resp <- ctx.commandService.submitAndWaitForTransaction(req)
        } yield {
          resp.transaction should not be empty

        }
      }

      "return the transaction tree if successful" in allFixtures { ctx =>
        for {
          req <- request(ctx)
          resp <- ctx.commandService.submitAndWaitForTransactionTree(req)
        } yield {
          resp.transaction should not be empty

        }
      }

      "complete with an empty response if resending a successful command" in allFixtures { ctx =>
        val commandId = UUID.randomUUID().toString
        for {
          resp1 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWait)
          resp2 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWait)
        } yield {
          resp1 shouldEqual Empty()
          resp2 shouldEqual Empty()
        }
      }

      "return the transaction id if resending a successful command" in allFixtures { ctx =>
        val commandId = UUID.randomUUID().toString
        for {
          resp1 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransactionId)
          resp2 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransactionId)
        } yield {
          resp1.transactionId should not be empty
          resp2.transactionId should not be empty
          resp1.transactionId shouldEqual resp2.transactionId
        }
      }

      "return the flat transaction if resending a successful command" in allFixtures { ctx =>
        val commandId = UUID.randomUUID().toString
        for {
          resp1 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransaction)
          resp2 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransaction)
        } yield {
          resp1.transaction should not be empty
          resp2.transaction should not be empty
          resp1.transaction shouldEqual resp2.transaction
        }
      }

      "return the transaction tree if resending a successful command" in allFixtures { ctx =>
        val commandId = UUID.randomUUID().toString
        for {
          resp1 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransactionTree)
          resp2 <- request(ctx, id = commandId) flatMap (ctx.commandService.submitAndWaitForTransactionTree)
        } yield {
          resp1.transaction should not be empty
          resp2.transaction should not be empty
          resp1.transaction shouldEqual resp2.transaction
        }
      }

      "fail with not found if ledger id is invalid" in allFixtures { ctx =>
        request(ctx, ledgerId = Some(UUID.randomUUID().toString))
          .flatMap(ctx.commandService.submitAndWait)
          .failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }

      "fail SubmitAndWaitForTransactionId with not found if ledger id is invalid" in allFixtures {
        ctx =>
          request(ctx, ledgerId = Some(UUID.randomUUID().toString))
            .flatMap(ctx.commandService.submitAndWaitForTransactionId)
            .failed map {
            IsStatusException(Status.NOT_FOUND)(_)
          }
      }

      "fail SubmitAndWaitForTransaction with not found if ledger id is invalid" in allFixtures {
        ctx =>
          request(ctx, ledgerId = Some(UUID.randomUUID().toString))
            .flatMap(ctx.commandService.submitAndWaitForTransaction)
            .failed map {
            IsStatusException(Status.NOT_FOUND)(_)
          }

      }

      "fail SubmitAndWaitForTransactionTree with not found if ledger id is invalid" in allFixtures {
        ctx =>
          request(ctx, ledgerId = Some(UUID.randomUUID().toString))
            .flatMap(ctx.commandService.submitAndWaitForTransaction)
            .failed map {
            IsStatusException(Status.NOT_FOUND)(_)
          }

      }

    }

  }

  override protected def config: Config = Config.default
}
