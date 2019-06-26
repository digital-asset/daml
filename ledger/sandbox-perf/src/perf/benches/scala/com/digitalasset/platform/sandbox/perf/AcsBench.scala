// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.io.File

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.platform.sandbox.services.TestCommands
import org.openjdk.jmh.annotations.Benchmark

class AcsBench extends TestCommands with InfAwait {

  override protected def darFile: File = new File(rlocation("ledger/sandbox/Test.dar"))

  private def generateCommand(
      sequenceNumber: Int,
      contractId: String,
      ledgerId: domain.LedgerId,
      template: Identifier): SubmitAndWaitRequest = {
    buildRequest(
      ledgerId = ledgerId,
      commandId = s"command-id-exercise-$sequenceNumber",
      commands = Seq(exerciseWithUnit(template, contractId, "DummyChoice1")),
      appId = "app1"
    ).toSync
  }

  private def extractContractId(
      response: GetActiveContractsResponse,
      template: Identifier): Option[String] = {
    val events = response.activeContracts.toSet
    events.collectFirst {
      case CreatedEvent(contractId, _, Some(id), _, _, _, _, _, _) if id == template => contractId
    }
  }

  private def getContractIds(
      state: PerfBenchState,
      template: Identifier,
      ledgerId: domain.LedgerId) =
    new ActiveContractSetClient(ledgerId, state.ledger.acsService)(state.esf)
      .getActiveContracts(MockMessages.transactionFilter)
      .map(extractContractId(_, template))

  @Benchmark
  def consumeAcs(state: AcsBenchState): Unit = {
    val ledgerId = state.ledger.ledgerId
    val template = templateIds.dummy
    await(
      getContractIds(state, template, ledgerId).zipWithIndex
        .collect {
          case (Some(contractId), i) =>
            generateCommand(i.toInt, contractId, ledgerId, template)
        }
        .mapAsync(100)(state.ledger.commandService.submitAndWait)
        .runWith(Sink.ignore)(state.mat))
    ()

  }

}
