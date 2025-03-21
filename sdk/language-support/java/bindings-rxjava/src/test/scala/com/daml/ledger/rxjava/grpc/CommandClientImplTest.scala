// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import com.digitalasset.canton.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitResponse,
}
import com.daml.ledger.javaapi.data.{
  Command,
  CommandsSubmission,
  CreateCommand,
  DamlRecord,
  EventFormat,
  Identifier,
  TransactionFormat,
  TransactionShape,
}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import io.reactivex.Single
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.{Optional, UUID}
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.chaining.scalaUtilChainingOps

class CommandClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-service-ledger")

  private def withCommandClient(authService: AuthService = AuthServiceWildcard) = {
    ledgerServices.withCommandClient(
      Future.successful(SubmitAndWaitResponse.defaultInstance),
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
      val synchronizerId = UUID.randomUUID().toString
      val params = genCommands(List.empty, Some(synchronizerId))
        .withActAs("party")

      client
        .submitAndWait(params)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()

      service.getLastCommands.value.commands shouldBe empty
    }
  }

  behavior of "[2.2] CommandClientImpl.submitAndWait"

  it should "send the given command with the correct parameters" in {
    withCommandClient() { (client, service) =>
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val synchronizerId = UUID.randomUUID().toString

      val params = genCommands(List(command), Some(synchronizerId))
        .withActAs("party")

      client
        .submitAndWait(params)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()

      service.getLastCommands.value.userId shouldBe params.getUserId
      service.getLastCommands.value.commandId shouldBe params.getCommandId
      service.getLastCommands.value.actAs shouldBe params.getActAs.asScala
      service.getLastCommands.value.readAs shouldBe params.getReadAs.asScala
      service.getLastCommands.value.workflowId shouldBe params.getWorkflowId.get()
      service.getLastCommands.value.synchronizerId shouldBe synchronizerId
      service.getLastCommands.value.minLedgerTimeRel
        .map(_.seconds) shouldBe params.getMinLedgerTimeRel.asScala.map(_.getSeconds)
      service.getLastCommands.value.minLedgerTimeRel
        .map(_.nanos) shouldBe params.getMinLedgerTimeRel.asScala.map(_.getNano)
      service.getLastCommands.value.minLedgerTimeAbs
        .map(_.seconds) shouldBe params.getMinLedgerTimeAbs.asScala.map(_.getEpochSecond)
      service.getLastCommands.value.minLedgerTimeAbs
        .map(_.nanos) shouldBe params.getMinLedgerTimeAbs.asScala.map(_.getNano)
      service.getLastCommands.value.commands should have size 1
      val receivedCommand = service.getLastCommands.value.commands.head.command
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
        randomUUID().toString,
        token.fold(dummyCommands)(_ => commands),
      )
      .withActAs(party)
      .pipe(p => token.fold(p)(p.withAccessToken))

    submit(params).timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS).blockingGet()
  }

  private def submitAndWait(client: CommandClient) =
    submitAndWaitFor(client.submitAndWait) _

  private def submitAndWaitForTransaction(client: CommandClient) = {
    val transactionFormat = new TransactionFormat(
      new EventFormat(Map.empty.asJava, Optional.empty(), false),
      TransactionShape.ACS_DELTA,
    )
    submitAndWaitFor(commands => client.submitAndWaitForTransaction(commands, transactionFormat)) _
  }

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
