// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.{CreateCommand, Identifier, Record}
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitResponse
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.JavaConverters._
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandClientImplTest extends FlatSpec with Matchers with OptionValues with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-service-ledger")

  behavior of "[2.1] CommandClientImpl.submitAndWait"

  it should "send the given command to the Ledger" in {
    ledgerServices.withCommandClient(Future.successful(SubmitAndWaitResponse.defaultInstance)) {
      (client, service) =>
        val commands = genCommands(List.empty)
        client
          .submitAndWait(
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
        service.getLastRequest.value.getCommands.commands shouldBe empty
    }
  }

  behavior of "[2.2] CommandClientImpl.submitAndWait"

  it should "send the given command with the correct parameters" in {
    ledgerServices.withCommandClient(Future.successful(SubmitAndWaitResponse.defaultInstance)) {
      (client, service) =>
        val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
        val record = new Record(recordId, List.empty[Record.Field].asJava)
        val command = new CreateCommand(new Identifier("a", "a", "b"), record)
        val commands = genCommands(List(command))
        client
          .submitAndWait(
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
        service.getLastRequest.value.getCommands.applicationId shouldBe commands.getApplicationId
        service.getLastRequest.value.getCommands.commandId shouldBe commands.getCommandId
        service.getLastRequest.value.getCommands.party shouldBe commands.getParty
        service.getLastRequest.value.getCommands.workflowId shouldBe commands.getWorkflowId
        service.getLastRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
        service.getLastRequest.value.getCommands.getMaximumRecordTime.seconds shouldBe commands.getMaximumRecordTime.getEpochSecond
        service.getLastRequest.value.getCommands.getMaximumRecordTime.nanos shouldBe commands.getMaximumRecordTime.getNano
        service.getLastRequest.value.getCommands.getLedgerEffectiveTime.seconds shouldBe commands.getLedgerEffectiveTime.getEpochSecond
        service.getLastRequest.value.getCommands.getLedgerEffectiveTime.nanos shouldBe commands.getLedgerEffectiveTime.getNano
        service.getLastRequest.value.getCommands.commands should have size 1
        val receivedCommand = service.getLastRequest.value.getCommands.commands.head.command
        receivedCommand.isCreate shouldBe true
        receivedCommand.isExercise shouldBe false
        receivedCommand.create.value.getTemplateId.packageId shouldBe command.getTemplateId.getPackageId
        receivedCommand.create.value.getTemplateId.moduleName shouldBe command.getTemplateId.getModuleName
        receivedCommand.create.value.getTemplateId.entityName shouldBe command.getTemplateId.getEntityName
        receivedCommand.create.value.getCreateArguments.getRecordId.packageId shouldBe recordId.getPackageId
        receivedCommand.create.value.getCreateArguments.getRecordId.moduleName shouldBe recordId.getModuleName
        receivedCommand.create.value.getCreateArguments.getRecordId.entityName shouldBe recordId.getEntityName
        receivedCommand.create.value.getCreateArguments.fields shouldBe empty
    }
  }
}
