// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_filter._
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.StringPlusAny"
  ))
class ScenarioLoadingITDivulgence
    extends WordSpec
    with Matchers
    with ScalaFutures
    with TestCommands
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  override def scenario: Option[String] = Some("Test:testDivulgenceSuccess")

  private def newACClient(ledgerId: LedgerId) =
    new ActiveContractSetClient(ledgerId, ActiveContractsServiceGrpc.stub(channel))

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(15000, Millis)), scaled(Span(150, Millis)))

  private val allTemplatesForParty = M.transactionFilter

  private def getSnapshot(transactionFilter: TransactionFilter = allTemplatesForParty) =
    newACClient(ledgerId())
      .getActiveContracts(transactionFilter)
      .runWith(Sink.seq)

  implicit val ec = DirectExecutionContext

  "ScenarioLoading" when {
    "running a divulgence scenario" should {
      "not fail" in {
        // The testDivulgenceSuccess scenario uses divulgence
        // This test checks whether the scenario completes without failing
        whenReady(getSnapshot()) { resp =>
          resp.size should equal(1)
        }
      }
    }
  }

}
