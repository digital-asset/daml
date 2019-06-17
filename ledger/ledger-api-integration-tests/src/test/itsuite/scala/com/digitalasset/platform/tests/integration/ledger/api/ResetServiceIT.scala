// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.services.TestCommands
import com.digitalasset.platform.sandbox.utils.InfiniteRetries
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, Suite}

class ResetServiceIT
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Suite
    with InfiniteRetries
    with Matchers
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture // TODO: this suite shoul not be using LedgerContext, as it is smart and hides too much of the reset mechanism
    with ScalaFutures
    with TestCommands
    with SuiteResourceManagementAroundEach {

  override def timeLimit: Span = 30.seconds

  override protected val config: Config =
    Config.default.withLedgerIdMode(LedgerIdMode.Dynamic())

  override protected def darFile: File = new File(rlocation("ledger/sandbox/Test.dar"))

  private val allTemplatesForParty = M.transactionFilter

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  "ResetService" when {
    "state is reset" should {

      "return a new ledger ID" in allFixtures { ctx1 =>
        val lid1 = ctx1.ledgerId
        for {
          ctx2 <- ctx1.reset()
          lid2 = ctx2.ledgerId
          throwable <- ctx1.reset().failed
        } yield {
          lid1 should not equal lid2
          IsStatusException(Status.Code.NOT_FOUND)(throwable)
        }
      }

      "remove contracts from ACS after reset" in allFixtures { ctx =>
        val req = dummyCommands(ctx.ledgerId, "commandId1")
        for {
          _ <- ctx.commandService.submitAndWait(SubmitAndWaitRequest(commands = req.commands))
          snapshot <- ctx.acsClient.getActiveContracts(allTemplatesForParty).runWith(Sink.seq)
          _ = {
            val responses = snapshot.init // last response is just ledger offset
            val events = responses.flatMap(extractEvents)
            events.size shouldBe 3
          }
          newContext <- ctx.reset()
          newSnapshot <- newContext.acsClient
            .getActiveContracts(allTemplatesForParty)
            .runWith(Sink.seq)
        } yield {
          newSnapshot.size shouldBe 1
          val newEvents = newSnapshot.flatMap(extractEvents)
          newEvents.size shouldBe 0
        }
      }

    }
  }
}
