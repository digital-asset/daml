// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.{AsyncWordSpec, Matchers}

class CommandServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll {

  private def request(
      ctx: LedgerContext,
      id: String = UUID.randomUUID().toString,
      ledgerId: String = config.ledgerId.getOrElse("")) =
    MockMessages.submitAndWaitRequest
      .update(_.commands.ledgerId := ledgerId, _.commands.commandId := id)
      .copy(traceContext = None)

  "Commands Service" when {
    "submitting commands" should {
      "complete with an empty response if successful" in allFixtures { ctx =>
        ctx.commandService.submitAndWait(request(ctx)) map {
          _ shouldEqual Empty()
        }
      }

      "complete with an empty response if resending a successful command" in allFixtures { ctx =>
        val commandId = UUID.randomUUID().toString
        ctx.commandService.submitAndWait(request(ctx, id = commandId)) map {
          _ shouldEqual Empty()
        }
        ctx.commandService.submitAndWait(request(ctx, id = commandId)) map {
          _ shouldEqual Empty()
        }
      }

      "fail with not found if ledger id is invalid" in allFixtures { ctx =>
        ctx.commandService
          .submitAndWait(request(ctx, ledgerId = UUID.randomUUID().toString))
          .failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }

    }

  }

  override protected def config: Config = Config.default
}
