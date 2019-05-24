// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.time.Duration

import com.digitalasset.api.util.TimestampConversion.{fromInstant, toInstant}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.platform.apitesting.LedgerContext
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers, TryValues}

import scala.concurrent.Future

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
    val commands = requestWithTtl(ttl)
    submitSingleCommand(ctx, commands)
  }

  private def requestWithTtl(ttl: Duration) =
    submitRequest.getCommands
      .withMaximumRecordTime(
        fromInstant(toInstant(submitRequest.getCommands.getLedgerEffectiveTime).plus(ttl)))
      .withCommandId(s"TTL_of_$ttl")

  private def submitSingleCommand(ctx: LedgerContext, commands: Commands): Future[Empty] =
    ctx.commandService.submitAndWait(SubmitAndWaitRequest(Some(commands)))

}
