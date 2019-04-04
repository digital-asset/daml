// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.platform.sandbox.TestExecutionSequencerFactory
import com.digitalasset.platform.sandbox.config.{LedgerIdMode, SandboxConfig}
import com.digitalasset.platform.sandbox.utils.InfiniteRetries
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, Suite}

import scala.concurrent.Future

class ResetServiceIT
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Suite
    with InfiniteRetries
    with Matchers
    with AkkaBeforeAndAfterAll
    with TestExecutionSequencerFactory
    with ScalaFutures
    with TestCommands
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  override def timeLimit: Span = 30.seconds

  private def newSyncClient = new SynchronousCommandClient(CommandServiceGrpc.stub(channel))

  override protected lazy val config: SandboxConfig =
    super.config.copy(ledgerIdMode = LedgerIdMode.Random)

  private def submitRequest(request: SubmitAndWaitRequest): Future[Empty] =
    newSyncClient.submitAndWait(request)

  private def reset(originalLedgerId: String): Future[String] =
    for {
      _ <- ResetServiceGrpc.stub(channel).reset(ResetRequest(originalLedgerId))
      lid <- retry(
        LedgerIdentityServiceGrpc.stub(channel).getLedgerIdentity(GetLedgerIdentityRequest()))
    } yield {
      lid.ledgerId
    }

  private def newACClient(ledgerId: String) =
    new ActiveContractSetClient(ledgerId, ActiveContractsServiceGrpc.stub(channel))

  private val allTemplatesForParty = M.transactionFilter

  private def getSnapshot(
      ledgerId: String,
      transactionFilter: TransactionFilter = allTemplatesForParty)
    : Future[Seq[GetActiveContractsResponse]] =
    newACClient(ledgerIdOnServer)
      .getActiveContracts(transactionFilter)
      .runWith(Sink.seq)

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  "ResetService" when {
    "state is reset" should {

      "return a new ledger ID" in {
        for {
          lid1 <- LedgerIdentityServiceGrpc
            .stub(channel)
            .getLedgerIdentity(GetLedgerIdentityRequest())
          lid1SecondInstance <- LedgerIdentityServiceGrpc
            .stub(channel)
            .getLedgerIdentity(GetLedgerIdentityRequest())
          lid2 <- reset(lid1.ledgerId)
          lid3 <- reset(lid2)
          throwable <- reset(lid2).failed
        } yield {
          lid1 shouldEqual lid1SecondInstance
          lid1 should not equal lid2
          lid2 should not equal lid3
          IsStatusException(Status.Code.NOT_FOUND)(throwable)
        }
      }

      "remove contracts from ACS after reset" in {
        val req = dummyCommands(ledgerId, "commandId1")
        for {
          _ <- submitRequest(SubmitAndWaitRequest(commands = req.commands))
          snapshot <- getSnapshot(ledgerIdOnServer)
          _ = {
            val responses = snapshot.init // last response is just ledger offset
            val events = responses.flatMap(extractEvents)
            events.size shouldBe 3
          }
          newLedgerId <- reset(ledgerIdOnServer)
          newSnapshot <- getSnapshot(newLedgerId)
        } yield {
          newSnapshot.size shouldBe 1
          val newEvents = newSnapshot.flatMap(extractEvents)
          newEvents.size shouldBe 0
        }
      }
    }
  }
}
