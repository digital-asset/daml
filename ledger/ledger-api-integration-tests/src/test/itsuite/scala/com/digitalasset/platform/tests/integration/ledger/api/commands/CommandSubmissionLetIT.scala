// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.time.Duration

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.util.Ctx
import com.google.rpc.status.Status
import io.grpc.Status.{ABORTED, OK}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

class CommandSubmissionLetIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with SuiteResourceManagementAroundAll
    with MultiLedgerCommandUtils {
  private val timeModel = config.timeModel

  "Command Service" when {

    "commands arrive with invalid Ledger Effective Times" should {

      "return ABORTED if LET is too high" in allFixtures { ctx =>
        for {
          c <- ctx.commandClient()
          skewedClient = c.withTimeProvider(
            c.timeProviderO.map(
              _.map(_.plus(timeModel.futureAcceptanceWindow).plus(Duration.ofSeconds(1L)))))
          result <- submitSingleCommand(
            submitRequest.getCommands.copy(commandId = "LET_TOO_HIGH"),
            skewedClient)
        } yield {
          result.value.getStatus shouldEqual Status(
            ABORTED.getCode.value(),
            "TRANSACTION_OUT_OF_TIME_WINDOW: " +
              "[currentTime:1970-01-01T00:00:00Z, " +
              "ledgerEffectiveTime:1970-01-01T00:00:02Z, " +
              "lowerBound:1970-01-01T00:00:01Z, " +
              "upperBound:1970-01-01T00:00:32Z]"
          )
        }
      }

      "return OK if LET is within the accepted interval" in allFixtures { ctx =>
        for {
          c <- ctx.commandClient()
          skewedClient = c.withTimeProvider(
            c.timeProviderO.map(_.map(_.plus(timeModel.futureAcceptanceWindow))))
          result <- submitSingleCommand(
            submitRequest.getCommands.copy(commandId = "LET_JUST_RIGHT"),
            skewedClient)
        } yield {
          result.value.getStatus.code shouldEqual OK.getCode.value()
        }
      }

      "return ABORTED if LET is too low" in allFixtures { ctx =>
        // In this case, the ledger's response races with the client's timeout detection.
        // So we can't be sure what the error message will be.
        for {
          c <- ctx.commandClient()
          skewedClient = c.withTimeProvider(
            c.timeProviderO.map(_.map(_.minus(timeModel.maxTtl).minus(Duration.ofSeconds(1L)))))
          result <- submitSingleCommand(
            submitRequest.getCommands.copy(commandId = "LET_TOO_LOW"),
            skewedClient)
        } yield {
          result.value.getStatus.code shouldEqual ABORTED.getCode.value()
        }
      }
    }
  }

  private def submitSingleCommand(commands: Commands, clientUsed: CommandClient) = {
    clientUsed.trackCommands[Int](List(submitRequest.getCommands.party)).flatMap { tracker =>
      Source
        .single(
          Ctx(
            1,
            submitRequest.copy(commands = Some(commands))
          ))
        .via(tracker)
        .runWith(Sink.head)
    }
  }
}
