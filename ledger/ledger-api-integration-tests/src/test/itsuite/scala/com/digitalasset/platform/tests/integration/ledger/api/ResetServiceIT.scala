// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.services.TestCommands
import com.digitalasset.platform.sandbox.utils.InfiniteRetries
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

  override protected def darFile: File = new File("ledger/sandbox/Test.dar")

  private val allTemplatesForParty = M.transactionFilter

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  "ResetService" when {
    "state is reset" should {

      "return a new ledger ID" in allFixtures { ctx =>
        for {
          lid1 <- ctx.ledgerIdentityService.getLedgerIdentity(GetLedgerIdentityRequest())
          lid1Bis <- ctx.ledgerIdentityService.getLedgerIdentity(GetLedgerIdentityRequest())
          lid2 <- ctx.reset()
          lid3 <- ctx.reset()
        } yield {
          lid1 shouldEqual lid1Bis
          lid1.ledgerId should not equal lid2
          lid2 should not equal lid3
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
          _ <- ctx.reset()
          newSnapshot <- ctx.acsClient.getActiveContracts(allTemplatesForParty).runWith(Sink.seq)
        } yield {
          newSnapshot.size shouldBe 1
          val newEvents = newSnapshot.flatMap(extractEvents)
          newEvents.size shouldBe 0
        }
      }

    }
  }
}
