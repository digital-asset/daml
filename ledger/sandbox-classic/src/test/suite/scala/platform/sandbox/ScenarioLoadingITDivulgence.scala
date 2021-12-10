// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{SuiteResourceManagementAroundEach, MockMessages => M}
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.transaction_filter._
import com.daml.ledger.client.services.acs.ActiveContractSetClient
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
class ScenarioLoadingITDivulgence
    extends AnyWordSpec
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
