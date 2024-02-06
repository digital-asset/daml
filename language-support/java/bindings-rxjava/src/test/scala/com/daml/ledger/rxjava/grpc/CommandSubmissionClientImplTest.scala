// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import com.daml.ledger.javaapi.data.{Command, CreateCommand, DamlRecord, Identifier}
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.v2.command_submission_service.SubmitResponse

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.chaining.scalaUtilChainingOps

class CommandSubmissionClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  import CommandSubmissionClientImplTest._

  val ledgerServices = new LedgerServices("command-submission-service-ledger")

  implicit class JavaOptionalAsScalaOption[A](opt: Optional[A]) {
    def asScala: Option[A] = if (opt.isPresent) Some(opt.get()) else None
  }

  behavior of "[3.1] CommandSubmissionClientImpl.submit"

  it should "timeout should work as expected across calls" in {
    ledgerServices.withCommandSubmissionClient(
      sequence(stuck, success),
      timeout = Optional.of(Duration.of(5, ChronoUnit.SECONDS)),
    ) { (client, _) =>
      val domainId = UUID.randomUUID().toString

      val params = genCommands(List.empty, Some(domainId))
        .withActAs("party")

      withClue("The first command should be stuck") {
        expectDeadlineExceeded(
          client
            .submit(params)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        )
      }

      withClue("The second command should go through") {
        val res = Option(
          client
            .submit(params)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        )
        res.isDefined shouldBe true
      }
    }
  }

  it should "send a commands to the ledger" in {
    ledgerServices.withCommandSubmissionClient(alwaysSucceed) { (client, serviceImpl) =>
      val domainId = UUID.randomUUID().toString

      val params = genCommands(List.empty, Some(domainId))
        .withActAs("party")

      client
        .submit(params)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()

      val receivedCommands = serviceImpl.getSubmittedRequest.value.getCommands

      receivedCommands.domainId shouldBe domainId
      receivedCommands.applicationId shouldBe params.getApplicationId
      receivedCommands.workflowId shouldBe params.getWorkflowId.get()
      receivedCommands.commandId shouldBe params.getCommandId
      receivedCommands.minLedgerTimeAbs.map(
        _.seconds
      ) shouldBe params.getMinLedgerTimeAbs.asScala
        .map(_.getEpochSecond)
      receivedCommands.minLedgerTimeAbs.map(
        _.nanos
      ) shouldBe params.getMinLedgerTimeAbs.asScala
        .map(_.getNano)
      receivedCommands.minLedgerTimeRel.map(
        _.seconds
      ) shouldBe params.getMinLedgerTimeRel.asScala
        .map(_.getSeconds)
      receivedCommands.minLedgerTimeRel.map(
        _.nanos
      ) shouldBe params.getMinLedgerTimeRel.asScala
        .map(_.getNano)
      receivedCommands.commands.size shouldBe params.getCommands.size()
    }
  }

  private def toAuthenticatedServer(fn: CommandSubmissionClient => Any): Any =
    ledgerServices.withCommandSubmissionClient(
      alwaysSucceed,
      mockedAuthService,
    ) { (client, _) =>
      fn(client)
    }

  private def submitDummyCommand(
      client: CommandSubmissionClient,
      accessToken: Option[String] = None,
  ) = {
    val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
    val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
    val command = new CreateCommand(new Identifier("a", "a", "b"), record)
    val domainId = UUID.randomUUID().toString

    val params = genCommands(List[Command](command), Some(domainId))
      .withActAs(someParty)
      .pipe(p => accessToken.fold(p)(p.withAccessToken))

    client
      .submit(params)
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

object CommandSubmissionClientImplTest {

  private val stuck = Future.never

  private val success = Future.successful(SubmitResponse.defaultInstance)

  private val alwaysSucceed: () => Future[SubmitResponse] = () => success

  private def sequence(
      first: Future[SubmitResponse],
      following: Future[SubmitResponse]*
  ): () => Future[SubmitResponse] = {
    val it = Iterator.single(first) ++ Iterator(following: _*)
    () =>
      try {
        it.next()
      } catch {
        case e: NoSuchElementException =>
          throw new RuntimeException("CommandSubmissionClientImplTest.sequence exhausted", e)
      }
  }

}
