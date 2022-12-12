// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}
import com.daml.ledger.javaapi.data.{
  Command,
  CommandsSubmission,
  CreateCommand,
  DamlRecord,
  Identifier,
}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.google.protobuf.empty.Empty
import io.reactivex.Single
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.Optional
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class CommandClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-service-ledger")

  private def withCommandClient(authService: AuthService = AuthServiceWildcard) = {
    ledgerServices.withCommandClient(
      Future.successful(Empty.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance),
      authService,
    ) _
  }

  implicit class JavaOptionalAsScalaOption[A](opt: Optional[A]) {
    def asScala: Option[A] = if (opt.isPresent) Some(opt.get()) else None
  }

  behavior of "[2.1] CommandClientImpl.submitAndWait"

  it should "send the given command to the Ledger" in {
    withCommandClient() { (client, service) =>
      val commands = genCommands(List.empty)
      val params = CommandsSubmission
        .create(commands.getApplicationId, commands.getCommandId, commands.getCommands)
        .withActAs(commands.getParty)
        .withMinLedgerTimeAbs(commands.getMinLedgerTimeAbsolute)
        .withMinLedgerTimeRel(commands.getMinLedgerTimeRelative)
        .withDeduplicationTime(commands.getDeduplicationTime)

      client
        .submitAndWait(params)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()

      service.getLastRequest.value.getCommands.commands shouldBe empty
    }
  }

  behavior of "[2.2] CommandClientImpl.submitAndWait"

  it should "send the given command with the correct parameters" in {
    withCommandClient() { (client, service) =>
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val commands = genCommands(List(command))

      val params = CommandsSubmission
        .create(commands.getApplicationId, commands.getCommandId, commands.getCommands)
        .withWorkflowId(commands.getWorkflowId)
        .withActAs(commands.getParty)
        .withMinLedgerTimeAbs(commands.getMinLedgerTimeAbsolute)
        .withMinLedgerTimeRel(commands.getMinLedgerTimeRelative)
        .withDeduplicationTime(commands.getDeduplicationTime)

      client
        .submitAndWait(params)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()

      service.getLastRequest.value.getCommands.applicationId shouldBe commands.getApplicationId
      service.getLastRequest.value.getCommands.commandId shouldBe commands.getCommandId
      service.getLastRequest.value.getCommands.party shouldBe commands.getParty
      service.getLastRequest.value.getCommands.actAs shouldBe commands.getActAs.asScala
      service.getLastRequest.value.getCommands.readAs shouldBe commands.getReadAs.asScala
      commands.getActAs.get(0) shouldBe commands.getParty
      service.getLastRequest.value.getCommands.workflowId shouldBe commands.getWorkflowId
      service.getLastRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
      service.getLastRequest.value.getCommands.minLedgerTimeRel
        .map(_.seconds) shouldBe commands.getMinLedgerTimeRelative.asScala.map(_.getSeconds)
      service.getLastRequest.value.getCommands.minLedgerTimeRel
        .map(_.nanos) shouldBe commands.getMinLedgerTimeRelative.asScala.map(_.getNano)
      service.getLastRequest.value.getCommands.minLedgerTimeAbs
        .map(_.seconds) shouldBe commands.getMinLedgerTimeAbsolute.asScala.map(_.getEpochSecond)
      service.getLastRequest.value.getCommands.minLedgerTimeAbs
        .map(_.nanos) shouldBe commands.getMinLedgerTimeAbsolute.asScala.map(_.getNano)
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

  private val dummyCommands = {
    val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
    val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
    val command: Command = new CreateCommand(new Identifier("a", "a", "b"), record)
    List(command).asJava
  }

  private type SubmitAndWait[A] = CommandsSubmission => Single[A]

  private def submitAndWaitFor[A](
      submit: SubmitAndWait[A]
  )(commands: java.util.List[Command], party: String, token: Option[String]) = {
    val params = CommandsSubmission
      .create(
        randomUUID().toString,
        randomUUID().toString,
        token.fold(dummyCommands)(_ => commands),
      )
      .withActAs(party)
      .withAccessToken(Optional.ofNullable(token.orNull))

    submit(params).timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS).blockingGet()
  }

  private def submitAndWait(client: CommandClient) =
    submitAndWaitFor(client.submitAndWait) _

  private def submitAndWaitForTransaction(client: CommandClient) =
    submitAndWaitFor(client.submitAndWaitForTransaction) _

  private def submitAndWaitForTransactionId(client: CommandClient) =
    submitAndWaitFor(client.submitAndWaitForTransactionId) _

  private def submitAndWaitForTransactionTree(client: CommandClient) =
    submitAndWaitFor(client.submitAndWaitForTransactionTree) _

  behavior of "Authorization"

  it should "deny access without token" in {
    withCommandClient(mockedAuthService) { (client, _) =>
      withClue("submitAndWait") {
        expectUnauthenticated {
          submitAndWait(client)(dummyCommands, someParty, None)
        }
      }
      withClue("submitAndWaitForTransaction") {
        expectUnauthenticated {
          submitAndWaitForTransaction(client)(dummyCommands, someParty, None)
        }
      }
      withClue("submitAndWaitForTransactionId") {
        expectUnauthenticated {
          submitAndWaitForTransactionId(client)(dummyCommands, someParty, None)
        }
      }
      withClue("submitAndWaitForTransactionTree") {
        expectUnauthenticated {
          submitAndWaitForTransactionTree(client)(dummyCommands, someParty, None)
        }
      }
    }
  }

  it should "deny access with the wrong token" in {
    withCommandClient(mockedAuthService) { (client, _) =>
      withClue("submitAndWait") {
        expectPermissionDenied {
          submitAndWait(client)(dummyCommands, someParty, Option(someOtherPartyReadWriteToken))
        }
      }
      withClue("submitAndWaitForTransaction") {
        expectPermissionDenied {
          submitAndWaitForTransaction(client)(
            dummyCommands,
            someParty,
            Option(someOtherPartyReadWriteToken),
          )
        }
      }
      withClue("submitAndWaitForTransactionId") {
        expectPermissionDenied {
          submitAndWaitForTransactionId(client)(
            dummyCommands,
            someParty,
            Option(someOtherPartyReadWriteToken),
          )
        }
      }
      withClue("submitAndWaitForTransactionTree") {
        expectPermissionDenied {
          submitAndWaitForTransactionTree(client)(
            dummyCommands,
            someParty,
            Option(someOtherPartyReadWriteToken),
          )
        }
      }
    }
  }

  // not throwing is enough to pass these tests
  it should "allow access with the right token" in {
    withCommandClient(mockedAuthService) { (client, _) =>
      withClue("submitAndWait") {
        submitAndWait(client)(dummyCommands, someParty, Option(somePartyReadWriteToken))
      }
      withClue("submitAndWaitForTransaction") {
        submitAndWaitForTransaction(client)(
          dummyCommands,
          someParty,
          Option(somePartyReadWriteToken),
        )
      }
      withClue("submitAndWaitForTransactionId") {
        submitAndWaitForTransactionId(client)(
          dummyCommands,
          someParty,
          Option(somePartyReadWriteToken),
        )
      }
      withClue("submitAndWaitForTransactionTree") {
        submitAndWaitForTransactionTree(client)(
          dummyCommands,
          someParty,
          Option(somePartyReadWriteToken),
        )
      }
    }
  }

}
