// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.MockMessages.transactionFilter
import com.daml.ledger.api.testing.utils.{SuiteResourceManagementAroundEach, MockMessages => M}
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsResponse,
}
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter._
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.services.acs.ActiveContractSetClient
import com.daml.ledger.client.services.commands.SynchronousCommandClient
import com.daml.ledger.client.services.transactions.TransactionClient
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, Suite}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
abstract class ScenarioLoadingITBase
    extends AsyncWordSpec
    with Suite
    with Matchers
    with ScalaFutures
    with TestCommands
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(15000, Millis)), scaled(Span(150, Millis)))

  override final def scenario: Option[String] = Some("Test:testScenario")

  private def newACClient(ledgerId: LedgerId) =
    new ActiveContractSetClient(ledgerId, ActiveContractsServiceGrpc.stub(channel))

  private def newSyncClient = new SynchronousCommandClient(CommandServiceGrpc.stub(channel))

  private def submitRequest(request: SubmitAndWaitRequest) =
    newSyncClient.submitAndWait(request)

  private def newTransactionClient(ledgerId: LedgerId): TransactionClient = {
    new TransactionClient(ledgerId, TransactionServiceGrpc.stub(channel))
  }

  private def getSnapshot(transactionFilter: TransactionFilter = allTemplatesForParty) =
    newACClient(ledgerId())
      .getActiveContracts(transactionFilter)
      .runWith(Sink.seq)

  private def lookForContract(
      events: Seq[CreatedEvent],
      template: Identifier,
      present: Boolean = true,
  ): Assertion = {
    val occurrence = if (present) 1 else 0
    events.collect { case ce @ CreatedEvent(_, _, Some(`template`), _, _, _, _, _, _) =>
      ce.contractId should fullyMatch regex "00([0-9a-f][0-9a-f]){32,94}"
      ce
    }.size should equal(occurrence)
  }

  private def validateResponses(response: GetActiveContractsResponse): Unit = {
    response.workflowId.startsWith("scenario-workflow") shouldBe true
    response.activeContracts.foreach(_.witnessParties should equal(List(M.party)))
  }

  private def extractEvents(response: GetActiveContractsResponse) =
    response.activeContracts.toSet

  private val allTemplatesForParty = M.transactionFilter

  private lazy val dummyRequest = dummyCommands(ledgerId(), "commandId1")

  "ScenarioLoading" when {
    "contracts have been created" should {
      "return them in an ACS snapshot" in {
        for {
          resp <- getSnapshot()
        } yield {
          resp.size should equal(5)

          val responses = resp.init // last response is just the ledger offset

          responses.foreach(validateResponses)

          val events = responses.flatMap(extractEvents)

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
          lookForContract(events, templateIds.dummyContractFactory)

          val GetActiveContractsResponse(offset, workflowId, activeContracts) = resp.last
          offset should not be empty
          workflowId shouldBe empty
          activeContracts shouldBe empty
        }
      }

      "return them in an transaction service" in {
        val startExclusive =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        for {
          txs <- newTransactionClient(ledgerId())
            .getTransactions(startExclusive, None, transactionFilter)
            .take(4)
            .runWith(Sink.seq)
        } yield {
          val events = txs.flatMap(_.events).map(_.getCreated)
          events.length shouldBe 4

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
          lookForContract(events, templateIds.dummyContractFactory)
        }
      }

      "does not recycle contract ids" in {
        for {
          _ <- submitRequest(SubmitAndWaitRequest(commands = dummyRequest.commands))
          resp <- getSnapshot()
        } yield {
          val responses = resp.init // last response is just ledger offset
          val events = responses.flatMap(extractEvents)
          val contractIds = events.map(_.contractId).toSet

          contractIds.size shouldBe 7
        }
      }

      "event ids from the active contracts service can be used to load transactions" in {
        val client = newTransactionClient(ledgerId())
        for {
          _ <- submitRequest(SubmitAndWaitRequest(commands = dummyRequest.commands))
          resp <- getSnapshot()
          responses = resp.init // last response is just ledger offset
          eventIds = responses.flatMap(_.activeContracts).map(_.eventId)
          txByEventId <- Future
            .sequence(
              eventIds.map(evId =>
                client.getFlatTransactionByEventId(evId, Seq(M.party)).map(evId -> _)
              )
            )
            .map(_.toMap)
        } yield {
          eventIds.foreach { evId =>
            txByEventId.keySet should contain(evId)
          }
          succeed
        }
      }

      "event ids from the transaction service can be used to load transactions" in {
        val startExclusive =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        val client = newTransactionClient(ledgerId())
        for {
          txs <- client
            .getTransactions(startExclusive, None, transactionFilter)
            .take(4)
            .runWith(Sink.seq)
          events = txs.flatMap(_.events).map(_.getCreated)
          _ = events.length shouldBe 4
          txByEventId <- Future
            .sequence(
              events.map(e =>
                client
                  .getFlatTransactionByEventId(e.eventId, Seq(M.party))
                  .map(e.eventId -> _)
              )
            )
            .map(_.toMap)
        } yield {
          events.foreach { event =>
            txByEventId.keys should contain(event.eventId)
          }
          succeed
        }
      }
    }
  }

}
