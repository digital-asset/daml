// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.Optional
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.{Command, CreateCommand, DamlRecord, Identifier}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.google.protobuf.empty.Empty
import io.grpc.Deadline
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class CommandSubmissionClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("command-submission-service-ledger")

  implicit class JavaOptionalAsScalaOption[A](opt: Optional[A]) {
    def asScala: Option[A] = if (opt.isPresent) Some(opt.get()) else None
  }

  behavior of "[3.1] CommandSubmissionClientImpl.submit"

  it should "timeout after deadline exceeded" in {
    ledgerServices.withCommandSubmissionClient(
      Future.never,
      deadline = Optional.of(Deadline.after(5, TimeUnit.SECONDS)),
    ) { (client, serviceImpl) =>
      val commands = genCommands(List.empty)
      expectDeadlineExceeded(
        client
          .submit(
            commands.getWorkflowId,
            commands.getApplicationId,
            commands.getCommandId,
            commands.getParty,
            commands.getCommands,
          )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      )
      val receivedCommands = serviceImpl.getSubmittedRequest.value.getCommands
      receivedCommands.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

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
            commands.getMinLedgerTimeAbsolute,
            commands.getMinLedgerTimeRelative,
            commands.getDeduplicationTime,
            commands.getCommands,
          )
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
        val receivedCommands = serviceImpl.getSubmittedRequest.value.getCommands
        receivedCommands.ledgerId shouldBe ledgerServices.ledgerId
        receivedCommands.applicationId shouldBe commands.getApplicationId
        receivedCommands.workflowId shouldBe commands.getWorkflowId
        receivedCommands.commandId shouldBe commands.getCommandId
        receivedCommands.minLedgerTimeAbs.map(
          _.seconds
        ) shouldBe commands.getMinLedgerTimeAbsolute.asScala
          .map(_.getEpochSecond)
        receivedCommands.minLedgerTimeAbs.map(
          _.nanos
        ) shouldBe commands.getMinLedgerTimeAbsolute.asScala
          .map(_.getNano)
        receivedCommands.minLedgerTimeRel.map(
          _.seconds
        ) shouldBe commands.getMinLedgerTimeRelative.asScala
          .map(_.getSeconds)
        receivedCommands.minLedgerTimeRel.map(
          _.nanos
        ) shouldBe commands.getMinLedgerTimeRelative.asScala
          .map(_.getNano)
        receivedCommands.party shouldBe commands.getParty
        receivedCommands.commands.size shouldBe commands.getCommands.size()
    }
  }

  def toAuthenticatedServer(fn: CommandSubmissionClient => Any): Any =
    ledgerServices.withCommandSubmissionClient(
      Future.successful(Empty.defaultInstance),
      mockedAuthService,
    ) { (client, _) =>
      fn(client)
    }

  def submitDummyCommand(client: CommandSubmissionClient, accessToken: Option[String] = None) = {
    val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
    val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
    val command = new CreateCommand(new Identifier("a", "a", "b"), record)
    val commands = genCommands(List[Command](command), Option(someParty))
    accessToken
      .fold(
        client
          .submit(
            commands.getWorkflowId,
            commands.getApplicationId,
            commands.getCommandId,
            commands.getParty,
            commands.getMinLedgerTimeAbsolute,
            commands.getMinLedgerTimeRelative,
            commands.getDeduplicationTime,
            commands.getCommands,
          )
      )(
        client
          .submit(
            commands.getWorkflowId,
            commands.getApplicationId,
            commands.getCommandId,
            commands.getParty,
            commands.getMinLedgerTimeAbsolute,
            commands.getMinLedgerTimeRelative,
            commands.getDeduplicationTime,
            commands.getCommands,
            _,
          )
      )
      .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
      .blockingGet()
  }

  behavior of "Authorization"

  it should "fail without a token" in {
    toAuthenticatedServer { client =>
      expectUnauthenticated {
        submitDummyCommand(client)
      }
    }
  }

  it should "fail with the wrong token" in {
    toAuthenticatedServer { client =>
      expectPermissionDenied {
        submitDummyCommand(client, Option(someOtherPartyReadWriteToken))
      }
    }
  }

  it should "fail with insufficient authorization" in {
    toAuthenticatedServer { client =>
      expectPermissionDenied {
        submitDummyCommand(client, Option(somePartyReadToken))
      }
    }
  }

  it should "succeed with the correct authorization" in {
    toAuthenticatedServer { client =>
      submitDummyCommand(client, Option(somePartyReadWriteToken))
    }
  }

}
