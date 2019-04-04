// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.google.protobuf.empty.Empty
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.concurrent.Future

class CommandSubmissionClientImplTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-submission-service-ledger")

  behavior of "[3.1] CommandSubmissionClientImpl.submit"

  it should "send a commands to the ledger" in {
    ledgerServices.withCommandSubmissionClient(Future.successful(Empty.defaultInstance)) {
      (client, serviceImpl) =>
        val commands = genCommands(List.empty)
        client
          .submit(
            commands.getWorkflowId,
            commands.getApplicationId,
            commands.getCommandId,
            commands.getParty,
            commands.getLedgerEffectiveTime,
            commands.getMaximumRecordTime,
            commands.getCommands
          )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
        val receivedCommands = serviceImpl.getSubmittedRequest.value.getCommands
        receivedCommands.ledgerId shouldBe ledgerServices.ledgerId
        receivedCommands.applicationId shouldBe commands.getApplicationId
        receivedCommands.workflowId shouldBe commands.getWorkflowId
        receivedCommands.commandId shouldBe commands.getCommandId
        receivedCommands.getLedgerEffectiveTime.seconds shouldBe commands.getLedgerEffectiveTime.getEpochSecond
        receivedCommands.getLedgerEffectiveTime.nanos shouldBe commands.getLedgerEffectiveTime.getNano
        receivedCommands.getMaximumRecordTime.seconds shouldBe commands.getMaximumRecordTime.getEpochSecond
        receivedCommands.getMaximumRecordTime.nanos shouldBe commands.getMaximumRecordTime.getNano
        receivedCommands.party shouldBe commands.getParty
        receivedCommands.commands.size shouldBe commands.getCommands.size()
    }
  }
}
