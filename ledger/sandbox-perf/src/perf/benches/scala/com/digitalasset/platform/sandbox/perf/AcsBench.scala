// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.MockMessages
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.services.acs.ActiveContractSetClient
import com.daml.ledger.test.ModelTestDar
import com.daml.platform.sandbox.services.TestCommands
import org.openjdk.jmh.annotations.{Benchmark, Level, Setup}

class AcsBenchState extends PerfBenchState with DummyCommands with InfAwait {

  def commandCount = 10000L

  @Setup(Level.Invocation)
  def submitCommands(): Unit = {
    await(
      dummyCreates(ledger.ledgerId)
        .take(commandCount)
        .mapAsync(100)(ledger.commandService.submitAndWait)
        .runWith(Sink.ignore)(mat)
    )
    ()
  }
}

class AcsBench extends TestCommands with InfAwait {

  override protected def darFile: File = new File(rlocation(ModelTestDar.path))

  private def generateCommand(
      sequenceNumber: Int,
      contractId: String,
      ledgerId: domain.LedgerId,
      template: Identifier,
  ): SubmitAndWaitRequest = {
    buildRequest(
      ledgerId = ledgerId,
      commandId = s"command-id-exercise-$sequenceNumber",
      commands = Seq(exerciseWithUnit(template, contractId, "DummyChoice1")),
      applicationId = "app1",
    ).toSync
  }

  private def extractContractId(
      response: GetActiveContractsResponse,
      template: Identifier,
  ): Option[String] = {
    val events = response.activeContracts.toSet
    events.collectFirst {
      case CreatedEvent(contractId, _, Some(id), _, _, _, _, _, _, _, _) if id == template =>
        contractId
    }
  }

  private def getContractIds(
      state: PerfBenchState,
      template: Identifier,
      ledgerId: domain.LedgerId,
  ) =
    new ActiveContractSetClient(ledgerId, state.ledger.acsService)(state.esf)
      .getActiveContracts(MockMessages.transactionFilter)
      .map(extractContractId(_, template))

  @Benchmark
  def consumeAcs(state: AcsBenchState): Unit = {
    val ledgerId = state.ledger.ledgerId
    val template = templateIds.dummy
    await(
      getContractIds(state, template, ledgerId).zipWithIndex
        .collect { case (Some(contractId), i) =>
          generateCommand(i.toInt, contractId, ledgerId, template)
        }
        .mapAsync(100)(state.ledger.commandService.submitAndWait)
        .runWith(Sink.ignore)(state.mat)
    )
    ()

  }

}
