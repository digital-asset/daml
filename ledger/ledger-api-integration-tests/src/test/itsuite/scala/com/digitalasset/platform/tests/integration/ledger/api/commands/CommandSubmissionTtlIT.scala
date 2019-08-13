// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.time.Duration
import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.platform.apitesting.{LedgerContext, TestIdsGenerator}
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers, TryValues}
import scalaz.syntax.tag._

class CommandSubmissionTtlIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerCommandUtils
    with SuiteResourceManagementAroundAll
    with TryValues
    with Matchers
    with ScalaFutures {
  private val timeModel = config.timeModel
  private val epsilonDuration = Duration.ofMillis(10L)
  private val testIds = new TestIdsGenerator(config)

  "Command Service" when {

    "commands arrive with extreme TTLs" should {

      "return INVALID_ARGUMENT status on too short TTL" in allFixtures { ctx =>
        val resultsF = submitRequestWithTtl(ctx, timeModel.minTtl.minus(epsilonDuration))

        resultsF.failed.map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return INVALID_ARGUMENT status on too long TTL" in allFixtures { ctx =>
        val resultsF = submitRequestWithTtl(ctx, timeModel.maxTtl.plus(epsilonDuration))

        resultsF.failed.map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return success if TTL is on the minimum boundary" in allFixtures { ctx =>
        val resultsF = submitRequestWithTtl(ctx, timeModel.minTtl)

        resultsF.map(_ => succeed)
      }

      "return success if TTL is on the maximum boundary" in allFixtures { ctx =>
        val resultsF = submitRequestWithTtl(ctx, timeModel.maxTtl)

        resultsF.map(_ => succeed)
      }
    }
  }

  private def submitRequestWithTtl(ctx: LedgerContext, ttl: Duration) = {
    val commands = submitRequest.getCommands
      .withCommandId(testIds.testCommandId(s"TTL_of_$ttl-${UUID.randomUUID()}"))
      .withLedgerId(ctx.ledgerId.unwrap)

    for {
      req <- helpers.applyTime(SubmitAndWaitRequest(Some(commands)), ctx, ttl)
      resp <- ctx.commandService.submitAndWaitForTransactionId(req)
    } yield resp
  }
}
