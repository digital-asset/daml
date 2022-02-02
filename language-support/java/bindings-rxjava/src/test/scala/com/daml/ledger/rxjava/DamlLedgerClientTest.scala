// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.LedgerOffset.Absolute
import com.daml.ledger.javaapi.data.{Command, CreateCommand, DamlRecord, Identifier}
import com.daml.ledger.rxjava.grpc.helpers.{CommandServiceImpl, _}
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}
import com.daml.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import com.daml.ledger.api.v1.package_service._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
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

  behavior of "DamlLedgerClient.forLedgerIdAndHost"

  it should "connect to an existing ledger-api grpc service with the correct ledgerId and pass the ledgerId to the clients" in {
    withFakeLedgerServer(AuthServiceWildcard) { (server, impls) =>
      val damlLedgerClient = DamlLedgerClient
        .newBuilder("localhost", server.getPort)
        .withExpectedLedgerId(ledgerServices.ledgerId)
        .build(): @annotation.nowarn(
        "cat=deprecation&origin=com\\.daml\\.ledger\\.rxjava\\.DamlLedgerClient\\.Builder\\.withExpectedLedgerId"
      )
      testDamlLedgerClient(damlLedgerClient, impls)
    }
  }

  it should "connect to an existing ledger-api grpc service, autodiscover the ledgerId and pass it to the clients" in {
    withFakeLedgerServer(AuthServiceWildcard) { (server, impls) =>
      val damlLedgerClient = DamlLedgerClient.newBuilder("localhost", server.getPort).build()
      testDamlLedgerClient(damlLedgerClient, impls)
    }
  }

  it should "work with authentication" in {
    withFakeLedgerServer(mockedAuthService) { (server, ledgerServicesImpls) =>
      val damlLedgerClient = DamlLedgerClient
        .newBuilder("localhost", server.getPort)
        .withExpectedLedgerId(ledgerServices.ledgerId)
        .withAccessToken(somePartyReadWriteToken)
        .build(): @annotation.nowarn(
        "cat=deprecation&origin=com\\.daml\\.ledger\\.rxjava\\.DamlLedgerClient\\.Builder\\.withExpectedLedgerId"
      )
      damlLedgerClient.connect()
      damlLedgerClient.getLedgerId shouldBe ledgerServices.ledgerId
      testActiveContractSetClient(
        damlLedgerClient.getActiveContractSetClient,
        ledgerServicesImpls.activeContractsServiceImpl,
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
      testLedgerConfigurationClient(
        damlLedgerClient.getLedgerConfigurationClient,
        ledgerServicesImpls.ledgerConfigurationServiceImpl,
      )
      testTimeClientGet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
      expectPermissionDenied {
        testTimeClientSet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
      }
      testPackageClient(damlLedgerClient.getPackageClient, ledgerServicesImpls.packageServiceImpl)
      damlLedgerClient.close()
    }
  }

  private def clueFor(clientName: String): String =
    s"DamlLedgerClient failed to propagate ledgerId to the $clientName:"

  private def testDamlLedgerClient(
      damlLedgerClient: DamlLedgerClient,
      ledgerServicesImpls: LedgerServicesImpls,
  ): Any = {
    damlLedgerClient.connect()
    damlLedgerClient.getLedgerId shouldBe ledgerServices.ledgerId
    testActiveContractSetClient(
      damlLedgerClient.getActiveContractSetClient,
      ledgerServicesImpls.activeContractsServiceImpl,
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
    testLedgerConfigurationClient(
      damlLedgerClient.getLedgerConfigurationClient,
      ledgerServicesImpls.ledgerConfigurationServiceImpl,
    )
    testTimeClientGet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
    testTimeClientSet(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
    testPackageClient(damlLedgerClient.getPackageClient, ledgerServicesImpls.packageServiceImpl)
    damlLedgerClient.close()
  }

  private def testActiveContractSetClient(
      activeContractSetClient: ActiveContractsClient,
      activeContractsServiceImpl: ActiveContractsServiceImpl,
  ): Assertion = {
    withClue(clueFor("ActiveContractsClient")) {
      activeContractSetClient
        .getActiveContracts(filterFor(someParty), false)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .asScala
        .toList
      activeContractsServiceImpl.getLastRequest.value.ledgerId shouldBe ledgerServices.ledgerId
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
      val commands = genCommands(List(command), Option(someParty))
      commandClient
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
        .timeout(1L, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandServiceImpl.getLastRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testCommandCompletionClient(
      commandCompletionClient: CommandCompletionClient,
      commandCompletionServiceImpl: CommandCompletionServiceImpl,
  ): Assertion = {
    withClue(clueFor("CommandCompletionClient")) {
      commandCompletionClient
        .completionStream("applicationId", new Absolute(""), Set(someParty).asJava)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      commandCompletionServiceImpl.getLastCompletionStreamRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      commandCompletionClient
        .completionEnd()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandCompletionServiceImpl.getLastCompletionEndRequest.value.ledgerId shouldBe ledgerServices.ledgerId
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
      val commands = genCommands(List[Command](command), Option(someParty))
      commandSubmissionClient
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
        .timeout(1L, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandSubmissionServiceImpl.getSubmittedRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testTimeClientGet(
      timeClient: TimeClient,
      timeServiceImpl: TimeServiceImpl,
  ): Assertion = {
    withClue("TimeClientGet") {
      timeClient.getTime
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      timeServiceImpl.getLastGetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testTimeClientSet(
      timeClient: TimeClient,
      timeServiceImpl: TimeServiceImpl,
  ): Assertion = {
    withClue("TimeClientSet") {
      timeClient
        .setTime(Instant.EPOCH, Instant.ofEpochSecond(10L))
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      timeServiceImpl.getLastSetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testLedgerConfigurationClient(
      ledgerConfigurationClient: LedgerConfigurationClient,
      ledgerConfigurationServiceImpl: LedgerConfigurationServiceImpl,
  ): Assertion = {
    withClue("LedgerConfigurationClient") {
      ledgerConfigurationClient.getLedgerConfiguration
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      ledgerConfigurationServiceImpl.getLastRequest.value.ledgerId shouldBe ledgerServices.ledgerId
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
      packageServiceImpl.getLastListPackageRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      packageClient
        .getPackage("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      packageServiceImpl.getLastGetPackagesRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      packageClient
        .getPackageStatus("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      packageServiceImpl.getLastGetPackageStatusRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  // a custom withFakeLedgerServer that sets all parameters such that testing ledgerId is possible
  private def withFakeLedgerServer(
      authService: AuthService
  )(f: (Server, LedgerServicesImpls) => Any): Any = {
    ledgerServices.withFakeLedgerServer(
      Observable.fromArray(genGetActiveContractsResponse),
      Observable.empty(),
      Future.successful(Empty.defaultInstance),
      List(CompletionStreamResponse(None, Seq())),
      genCompletionEndResponse("completionEndResponse"),
      Future.successful(Empty.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance),
      List(genGetTimeResponse),
      Seq(GetLedgerConfigurationResponse.defaultInstance),
      Future.successful(ListPackagesResponse(Seq("id1"))),
      Future.successful(GetPackageResponse(HashFunction.SHA256, ByteString.EMPTY)),
      Future.successful(GetPackageStatusResponse(PackageStatus.values.head)),
      authService,
    )(f)
  }
}
