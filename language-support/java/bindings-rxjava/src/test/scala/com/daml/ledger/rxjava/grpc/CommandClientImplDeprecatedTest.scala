// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.time.{Duration, Instant}
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.{Command, CreateCommand, DamlRecord, Identifier}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}
import com.google.protobuf.empty.Empty
import io.reactivex.Single
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

//TODO: Should be removed in rxjava 3 copy #15180
class CommandClientImplDeprecatedTest
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
      client
        .submitAndWait(
          commands.getWorkflowId,
          commands.getApplicationId,
          commands.getCommandId,
          commands.getParty,
          commands.getMinLedgerTimeAbsolute,
          commands.getMinLedgerTimeRelative,
          commands.getDeduplicationTime,
          commands.getCommands,
        )
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
      client
        .submitAndWait(
          commands.getWorkflowId,
          commands.getApplicationId,
          commands.getCommandId,
          commands.getParty,
          commands.getMinLedgerTimeAbsolute,
          commands.getMinLedgerTimeRelative,
          commands.getDeduplicationTime,
          commands.getCommands,
        )
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

  private type SubmitAndWait[A] =
    (
        String,
        String,
        String,
        String,
        Optional[Instant],
        Optional[Duration],
        Optional[Duration],
        java.util.List[Command],
    ) => Single[A]
  private type SubmitAndWaitWithToken[A] =
    (
        String,
        String,
        String,
        String,
        Optional[Instant],
        Optional[Duration],
        Optional[Duration],
        java.util.List[Command],
        String,
    ) => Single[A]

  private def submitAndWaitFor[A](
      noToken: SubmitAndWait[A],
      withToken: SubmitAndWaitWithToken[A],
  )(commands: java.util.List[Command], party: String, token: Option[String]): A =
    token
      .fold(
        noToken(
          UUID.randomUUID.toString,
          UUID.randomUUID.toString,
          UUID.randomUUID.toString,
          party,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          dummyCommands,
        )
      )(
        withToken(
          UUID.randomUUID.toString,
          UUID.randomUUID.toString,
          UUID.randomUUID.toString,
          party,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          commands,
          _,
        )
      )
      .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
      .blockingGet()

  private def submitAndWait(client: CommandClient) =
    submitAndWaitFor(client.submitAndWait, client.submitAndWait) _

  private def submitAndWaitForTransaction(client: CommandClient) =
    submitAndWaitFor(client.submitAndWaitForTransaction, client.submitAndWaitForTransaction) _

  private def submitAndWaitForTransactionId(client: CommandClient) =
    submitAndWaitFor(client.submitAndWaitForTransactionId, client.submitAndWaitForTransactionId) _

  private def submitAndWaitForTransactionTree(client: CommandClient) =
    submitAndWaitFor(
      client.submitAndWaitForTransactionTree,
      client.submitAndWaitForTransactionTree,
    ) _

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
