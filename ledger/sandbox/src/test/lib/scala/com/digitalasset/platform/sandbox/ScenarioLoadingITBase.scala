// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.MockMessages.transactionFilter
import com.digitalasset.ledger.api.testing.utils.{
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction_filter._
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, Suite, WordSpec}

import scala.concurrent.Future

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.StringPlusAny"
  ))
abstract class ScenarioLoadingITBase
    extends WordSpec
    with Suite
    with Matchers
    with ScalaFutures
    with TestCommands
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  private def newACClient(ledgerId: LedgerId) =
    new ActiveContractSetClient(ledgerId, ActiveContractsServiceGrpc.stub(channel))

  private def newSyncClient = new SynchronousCommandClient(CommandServiceGrpc.stub(channel))

  private def submitRequest(request: SubmitAndWaitRequest) =
    newSyncClient.submitAndWait(request)

  private def newTransactionClient(ledgerId: LedgerId): TransactionClient = {
    new TransactionClient(ledgerId, TransactionServiceGrpc.stub(channel))
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(15000, Millis)), scaled(Span(150, Millis)))

  private val allTemplatesForParty = M.transactionFilter

  private def getSnapshot(transactionFilter: TransactionFilter = allTemplatesForParty) =
    newACClient(ledgerId())
      .getActiveContracts(transactionFilter)
      .runWith(Sink.seq)

  private def lookForContract(
      events: Seq[CreatedEvent],
      template: Identifier,
      present: Boolean = true): Unit = {
    val occurrence = if (present) 1 else 0
    val _ = events.collect {
      case ce @ CreatedEvent(_, _, Some(`template`), _, _, _, _, _, _) =>
        // the absolute contract ids are opaque -- they have no specified format. however, for now
        // we like to keep it consistent between the DAML studio and the sandbox. Therefore verify
        // that they have the same format.
        ce.contractId should fullyMatch regex "#[0-9]+:[0-9]+"
        ce
    }.size should equal(occurrence)
  }

  private def validateResponses(response: GetActiveContractsResponse) = {
    response.workflowId.startsWith("scenario-workflow") shouldBe true
    response.activeContracts.foreach(_.witnessParties should equal(List(M.party)))
  }

  private def extractEvents(response: GetActiveContractsResponse) =
    response.activeContracts.toSet

  lazy val dummyRequest = {
    // we need to adjust the time of the request because we pass 10
    // days in the test scenario.
    val letInstant = Instant.EPOCH.plus(10, ChronoUnit.DAYS)
    val let = Timestamp(letInstant.getEpochSecond, letInstant.getNano)
    val mrt = Timestamp(let.seconds + 30L, let.nanos)
    dummyCommands(ledgerId(), "commandId1").update(
      _.commands.ledgerEffectiveTime := let,
      _.commands.maximumRecordTime := mrt
    )
  }

  implicit val ec = DirectExecutionContext

  "ScenarioLoading" when {

    "contracts have been created" should {
      "return them in an ACS snapshot" in {
        whenReady(getSnapshot()) { resp =>
          resp.size should equal(5)

          val responses = resp.init // last response is just the ledger offset

          responses.foreach(validateResponses)

          val events = responses.flatMap(extractEvents)

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
          lookForContract(events, templateIds.dummyContractFactory)

          resp.last should equal(GetActiveContractsResponse("7", "", Seq.empty, None))
        }
      }

      "return them in an transaction service" in {

        val beginOffset =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        val resultsF =
          newTransactionClient(ledgerId())
            .getTransactions(beginOffset, None, transactionFilter)
            .take(4)
            .runWith(Sink.seq)

        whenReady(resultsF) { txs =>
          val events = txs.flatMap(_.events).map(_.getCreated)
          events.length shouldBe 4

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
          lookForContract(events, templateIds.dummyContractFactory)
        }
      }

      "does not recycle contract ids" in {
        whenReady(submitRequest(SubmitAndWaitRequest(commands = dummyRequest.commands))) { _ =>
          whenReady(getSnapshot()) { resp =>
            val responses = resp.init // last response is just ledger offset
            val events = responses.flatMap(extractEvents)
            val contractIds = events.map(_.contractId).toSet

            // note how we skip #0, #2 and #5 because of the `pass`es in the scenario.
            contractIds shouldBe Set("#4:0", "#7:2", "#3:0", "#7:1", "#1:0", "#7:0", "#6:0")
          }
        }
      }

      "event ids can be used to load transactions (ACS)" in {
        val client = newTransactionClient(ledgerId())
        whenReady(submitRequest(SubmitAndWaitRequest(commands = dummyRequest.commands))) { _ =>
          whenReady(getSnapshot()) { resp =>
            val responses = resp.init // last response is just ledger offset
            val eventIds = responses.flatMap(_.activeContracts).map(_.eventId)
            val txByEventIdF = Future
              .sequence(eventIds.map(evId =>
                client.getFlatTransactionByEventId(evId, Seq(M.party)).map(evId -> _)))
              .map(_.toMap)
            whenReady(txByEventIdF) { txByEventId =>
              eventIds.foreach { evId =>
                txByEventId.keySet should contain(evId)
              }
            }
          }
        }
      }

      "event ids are the same as contract ids (transaction service)" in {
        val beginOffset =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        val client = newTransactionClient(ledgerId())
        val resultsF = client
          .getTransactions(beginOffset, None, transactionFilter)
          .take(4)
          .runWith(Sink.seq)

        whenReady(resultsF) { txs =>
          val events = txs.flatMap(_.events).map(_.getCreated)
          events.length shouldBe 4

          val txByEventIdF = Future
            .sequence(
              events.map(e =>
                client
                  .getFlatTransactionByEventId(e.eventId, Seq(M.party))
                  .map(e.eventId -> _)))
            .map(_.toMap)

          whenReady(txByEventIdF) { txByEventId =>
            events.foreach { event =>
              txByEventId.keys should contain(event.eventId)
            }

          }

        }
      }
    }
  }

}
