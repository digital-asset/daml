// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava

import java.time.Instant
import java.util.Optional
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.LedgerOffset.Absolute
import com.daml.ledger.javaapi.data.{Command, CreateCommand, Identifier, Record}
import com.daml.ledger.rxjava.grpc.helpers._
import com.daml.ledger.testkit.services._
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse
}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import com.digitalasset.ledger.api.v1.package_service._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.Server
import io.reactivex.Observable
import org.scalatest.{Assertion, FlatSpec, Matchers, OptionValues}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class DamlLedgerClientTest extends FlatSpec with Matchers with OptionValues with DataLayerHelpers {

  val ledgerServices = new LedgerServices(getClass.getSimpleName)

  behavior of "DamlLedgerClient.forLedgerIdAndHost"

  it should "connect to an existing ledger-api grpc service with the correct ledgerId and pass the ledgerId to the clients" in {
    withFakeLedgerServer() { (server, impls) =>
      val damlLedgerClient = DamlLedgerClient.forLedgerIdAndHost(
        ledgerServices.ledgerId,
        "localhost",
        server.getPort,
        Optional.empty())
      testDamlLedgerClient(damlLedgerClient, impls)
    }
  }

  it should "connect to an existing ledger-api grpc service, autodiscover the ledgerId and pass it to the clients" in {
    withFakeLedgerServer() { (server, impls) =>
      val damlLedgerClient =
        DamlLedgerClient.forHostWithLedgerIdDiscovery("localhost", server.getPort, Optional.empty())
      testDamlLedgerClient(damlLedgerClient, impls)
    }
  }

  private def clueFor(clientName: String): String =
    s"DamlLedgerClient failed to propagate ledgerId to the $clientName:"

  private def testDamlLedgerClient(
      damlLedgerClient: DamlLedgerClient,
      ledgerServicesImpls: LedgerServicesImpls): Any = {
    damlLedgerClient.connect()
    damlLedgerClient.getLedgerId shouldBe ledgerServices.ledgerId
    testActiveContractSetClient(
      damlLedgerClient.getActiveContractSetClient,
      ledgerServicesImpls.activeContractsServiceImpl)
    testCommandClient(damlLedgerClient.getCommandClient, ledgerServicesImpls.commandServiceImpl)
    testCommandCompletionClient(
      damlLedgerClient.getCommandCompletionClient,
      ledgerServicesImpls.commandCompletionServiceImpl)
    testCommandSubmissionClient(
      damlLedgerClient.getCommandSubmissionClient,
      ledgerServicesImpls.commandSubmissionServiceImpl)
    testLedgerConfigurationClient(
      damlLedgerClient.getLedgerConfigurationClient,
      ledgerServicesImpls.ledgerConfigurationServiceImpl)
    testTimeClient(damlLedgerClient.getTimeClient, ledgerServicesImpls.timeServiceImpl)
    testPackageClient(damlLedgerClient.getPackageClient, ledgerServicesImpls.packageServiceImpl)
    damlLedgerClient.getPackageClient
    damlLedgerClient.close()
  }

  private def testActiveContractSetClient(
      activeContractSetClient: ActiveContractsClient,
      activeContractsServiceImpl: ActiveContractsServiceImpl): Assertion = {
    withClue(clueFor("ActiveContractsClient")) {
      activeContractSetClient
        .getActiveContracts(filterNothing, false)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingIterable()
        .asScala
        .toList
      activeContractsServiceImpl.getLastRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testCommandClient(
      commandClient: CommandClient,
      commandServiceImpl: CommandServiceImpl): Assertion = {
    withClue(clueFor("CommandClient")) {
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new Record(recordId, List.empty[Record.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val commands = genCommands(List(command))
      commandClient
        .submitAndWait(
          commands.getWorkflowId,
          commands.getApplicationId,
          commands.getCommandId,
          commands.getParty,
          commands.getLedgerEffectiveTime,
          commands.getMaximumRecordTime,
          commands.getCommands
        )
        .timeout(1l, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandServiceImpl.getLastRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testCommandCompletionClient(
      commandCompletionClient: CommandCompletionClient,
      commandCompletionServiceImpl: CommandCompletionServiceImpl): Assertion = {
    withClue(clueFor("CommandCompletionClient")) {
      commandCompletionClient
        .completionStream("applicationId", new Absolute(""), Set("Alice").asJava)
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
      commandSubmissionServiceImpl: CommandSubmissionServiceImpl): Assertion = {
    withClue("CommandSubmissionClient") {
      val recordId = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
      val record = new Record(recordId, List.empty[Record.Field].asJava)
      val command = new CreateCommand(new Identifier("a", "a", "b"), record)
      val commands = genCommands(List[Command](command))
      commandSubmissionClient
        .submit(
          commands.getWorkflowId,
          commands.getApplicationId,
          commands.getCommandId,
          commands.getParty,
          commands.getLedgerEffectiveTime,
          commands.getMaximumRecordTime,
          commands.getCommands
        )
        .timeout(1l, TimeUnit.SECONDS)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      commandSubmissionServiceImpl.getSubmittedRequest.value.getCommands.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testTimeClient(
      timeClient: TimeClient,
      timeServiceImpl: TimeServiceImpl): Assertion = {
    withClue("TimeClient") {
      timeClient.getTime
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      timeServiceImpl.getLastGetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      timeClient
        .setTime(Instant.EPOCH, Instant.ofEpochSecond(10l))
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      timeServiceImpl.getLastSetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testLedgerConfigurationClient(
      ledgerConfigurationClient: LedgerConfigurationClient,
      ledgerConfigurationServiceImpl: LedgerConfigurationServiceImpl): Assertion = {
    withClue("LedgerConfigurationClient") {
      ledgerConfigurationClient.getLedgerConfiguration
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      ledgerConfigurationServiceImpl.getLastRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  private def testPackageClient(
      packageClient: PackageClient,
      packageServiceImpl: PackageServiceImpl): Assertion = {
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
  private def withFakeLedgerServer()(f: (Server, LedgerServicesImpls) => Any): Any = {
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
      Future.successful(GetPackageStatusResponse(PackageStatus.values.head))
    )(f)
  }
}
