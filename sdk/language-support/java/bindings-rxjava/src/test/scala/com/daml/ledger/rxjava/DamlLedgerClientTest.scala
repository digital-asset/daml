// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import com.daml.ledger.javaapi.data.{CreateCommand, DamlRecord, Identifier}
import com.daml.ledger.rxjava.grpc.helpers._
import com.digitalasset.canton.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitResponse,
}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.package_service._
import com.daml.ledger.api.v2.command_submission_service.SubmitResponse
import com.google.protobuf.ByteString
import io.grpc.Server
import io.reactivex.Observable
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class DamlLedgerClientTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices(getClass.getSimpleName)

  behavior of "DamlLedgerClient.forHost"

  it should "connect to an existing ledger-api grpc service and use it in the clients" in {
    withFakeLedgerServer(AuthServiceWildcard) { (server, impls) =>
      val damlLedgerClient = DamlLedgerClient
        .newBuilder("localhost", server.getPort)
        .build()
      testDamlLedgerClient(damlLedgerClient, impls)
    }
  }

  it should "work with authentication" in {
    withFakeLedgerServer(mockedAuthService) { (server, ledgerServicesImpls) =>
      val damlLedgerClient = DamlLedgerClient
        .newBuilder("localhost", server.getPort)
        .withAccessToken(somePartyReadWriteToken)
        .build()
      damlLedgerClient.connect()
      testStateServiceClient(
        damlLedgerClient.getStateClient,
        ledgerServicesImpls.stateServiceImpl,
      )
      testCommandClient(damlLedgerClient.getCommandClient, ledgerServicesImpls.commandServiceImpl)
      testCommandCompletionClient(
        damlLedgerClient.getCommandCompletionClient,
        ledgerServicesImpls.commandCompletionServiceImpl,
      )
      testCommandSubmissionClient(
        damlLedgerClient.getCommandSubmissionClient,
        ledgerServicesImpls.commandSubmissionServiceImpl,
      )
      testTimeClientGet(damlLedgerClient.getTimeClient)
      expectPermissionDenied {
        testTimeClientSet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
      }
      testPackageClient(damlLedgerClient.getPackageClient, ledgerServicesImpls.packageServiceImpl)
      damlLedgerClient.close()
    }
  }

  private def clueFor(clientName: String): String =
    s"DamlLedgerClient failed to activate $clientName:"

  private def testDamlLedgerClient(
      damlLedgerClient: DamlLedgerClient,
      ledgerServicesImpls: LedgerServicesImpls,
  ): Any = {
    damlLedgerClient.connect()
    testStateServiceClient(
      damlLedgerClient.getStateClient,
      ledgerServicesImpls.stateServiceImpl,
    )
    testCommandClient(damlLedgerClient.getCommandClient, ledgerServicesImpls.commandServiceImpl)
    testCommandCompletionClient(
      damlLedgerClient.getCommandCompletionClient,
      ledgerServicesImpls.commandCompletionServiceImpl,
    )
    testCommandSubmissionClient(
      damlLedgerClient.getCommandSubmissionClient,
      ledgerServicesImpls.commandSubmissionServiceImpl,
    )
    testTimeClientGet(damlLedgerClient.getTimeClient)
    testTimeClientSet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
    testPackageClient(damlLedgerClient.getPackageClient, ledgerServicesImpls.packageServiceImpl)
    damlLedgerClient.close()
  }

  private def testStateServiceClient(
      stateServiceClient: StateClient,
      activeContractsServiceImpl: StateServiceImpl,
  ): Assertion = {
    withClue(clueFor("StateClient")) {
      val currentEnd = stateServiceClient.getLedgerEnd.blockingGet()
      stateServiceClient
        .getActiveContracts(eventsFor(someParty), currentEnd)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .asScala
        .toList
      activeContractsServiceImpl.getLastRequest.value.eventFormat
        .flatMap(_.filtersByParty.get(someParty)) should not be empty
    }
  }

  private def testCommandClient(
      commandClient: CommandClient,
      commandServiceImpl: CommandServiceImpl,
  ): Assertion = {
    withClue(clueFor("CommandClient")) {
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val synchronizerId = UUID.randomUUID().toString

      val params = genCommands(List(command), Some(synchronizerId))
        .withActAs(someParty)

      commandClient
        .submitAndWait(params)
        .timeout(1L, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandServiceImpl.getLastCommands.value.synchronizerId shouldBe synchronizerId
    }
  }

  private def testCommandCompletionClient(
      commandCompletionClient: CommandCompletionClient,
      commandCompletionServiceImpl: CommandCompletionServiceImpl,
  ): Assertion = {
    withClue(clueFor("CommandCompletionClient")) {
      commandCompletionClient
        .completionStream("userId", 0L, List(someParty).asJava)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      commandCompletionServiceImpl.getLastCompletionStreamRequest.value.userId shouldBe "userId"
    }
  }

  private def testCommandSubmissionClient(
      commandSubmissionClient: CommandSubmissionClient,
      commandSubmissionServiceImpl: CommandSubmissionServiceImpl,
  ): Assertion = {
    withClue("CommandSubmissionClient") {
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new DamlRecord(recordId, List.empty[DamlRecord.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val synchronizerId = UUID.randomUUID().toString

      val params = genCommands(List(command), Some(synchronizerId))
        .withActAs(someParty)

      commandSubmissionClient
        .submit(params)
        .timeout(1L, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandSubmissionServiceImpl.getSubmittedRequest.value.getCommands.synchronizerId shouldBe synchronizerId
    }
  }

  private def testTimeClientGet(
      timeClient: TimeClient
  ): Assertion = {
    withClue("TimeClientGet") {
      timeClient.getTime
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      succeed
    }
  }

  private def testTimeClientSet(
      timeClient: TimeClient,
      timeServiceImpl: TimeServiceImpl,
  ): Assertion = {
    withClue("TimeClientSet") {
      val newTime = Instant.ofEpochSecond(10L)
      timeClient
        .setTime(Instant.EPOCH, newTime)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      timeServiceImpl.getLastSetTimeRequest.value.newTime.map(_.seconds) shouldBe Some(10L)
    }
  }

  private def testPackageClient(
      packageClient: PackageClient,
      packageServiceImpl: PackageServiceImpl,
  ): Assertion = {
    withClue("PackageClient") {
      packageClient
        .listPackages()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      packageClient
        .getPackage("packageIdA")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      packageServiceImpl.getLastGetPackagesRequest.value.packageId shouldBe "packageIdA"
      packageClient
        .getPackageStatus("packageIdB")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      packageServiceImpl.getLastGetPackageStatusRequest.value.packageId shouldBe "packageIdB"
    }
  }

  // a custom withFakeLedgerServer that sets all parameters such that testing ledgerId is possible
  private def withFakeLedgerServer(
      authService: AuthService
  )(f: (Server, LedgerServicesImpls) => Any): Any = {
    ledgerServices.withFakeLedgerServer(
      Observable.fromArray(genGetActiveContractsResponse),
      Observable.empty(),
      Future.successful(SubmitResponse.defaultInstance),
      List(CompletionStreamResponse.defaultInstance),
      Future.successful(SubmitAndWaitResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance),
      Future.successful(SubmitAndWaitForReassignmentResponse.defaultInstance),
      Future.successful(genGetTimeResponse),
      Future.successful(GetEventsByContractIdResponse.defaultInstance),
      Future.successful(ListPackagesResponse(Seq("id1"))),
      Future.successful(GetPackageResponse(HashFunction.HASH_FUNCTION_SHA256, ByteString.EMPTY)),
      Future.successful(GetPackageStatusResponse(PackageStatus.values.head)),
      authService,
    )(f)
  }
}
