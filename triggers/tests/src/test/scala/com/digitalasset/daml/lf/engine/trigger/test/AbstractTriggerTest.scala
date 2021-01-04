// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger
package test

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand, _}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.lf.archive.{DarReader, Decode}
import com.daml.lf.data.Ref._
import com.daml.lf.engine.trigger.{Runner, RunnerConfig, Trigger}
import com.daml.logging.LoggingContextOf.{label, newLoggingContext}
import com.daml.platform.sandbox.services.{SandboxFixture, TestCommands}
import org.scalatest._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait AbstractTriggerTest extends SandboxFixture with TestCommands {
  self: Suite =>

  protected val applicationId = RunnerConfig.DefaultApplicationId

  protected def ledgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
      token = None,
    )

  protected def ledgerClient(
      maxInboundMessageSize: Int = RunnerConfig.DefaultMaxInboundMessageSize)(
      implicit ec: ExecutionContext): Future[LedgerClient] =
    for {
      client <- LedgerClient
        .singleHost(
          "localhost",
          serverPort.value,
          ledgerClientConfiguration.copy(maxInboundMessageSize = maxInboundMessageSize),
        )
    } yield client

  override protected def darFile =
    Try(requiredResource("triggers/tests/acs.dar"))
      .getOrElse(requiredResource("triggers/tests/acs-1.dev.dar"))

  protected val dar = DarReader().readArchiveFromFile(darFile).get.map {
    case (pkgId, archive) => Decode.readArchivePayload(pkgId, archive)
  }
  protected val compiledPackages =
    PureCompiledPackages(dar.all.toMap, speedy.Compiler.Config.Dev).right.get

  protected def getRunner(client: LedgerClient, name: QualifiedName, party: String): Runner = {
    val triggerId = Identifier(packageId, name)
    val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).right.get
    newLoggingContext(label[Trigger], trigger.loggingExtension) { implicit lc =>
      new Runner(
        compiledPackages,
        trigger,
        client,
        config.timeProviderType.get,
        applicationId,
        party)
    }
  }

  protected def allocateParty(client: LedgerClient)(implicit ec: ExecutionContext): Future[String] =
    client.partyManagementClient.allocateParty(None, None).map(_.party)

  protected def create(client: LedgerClient, party: String, cmd: CreateCommand)(
      implicit ec: ExecutionContext): Future[String] = {
    val commands = Seq(Command().withCreate(cmd))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = ApplicationId.unwrap(applicationId),
          commandId = UUID.randomUUID.toString
        )))
    for {
      response <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield response.getTransaction.events.head.getCreated.contractId
  }

  protected def archive(
      client: LedgerClient,
      party: String,
      templateId: LedgerApi.Identifier,
      contractId: String)(implicit ec: ExecutionContext): Future[Unit] = {
    val commands = Seq(
      Command().withExercise(
        ExerciseCommand(
          templateId = Some(templateId),
          contractId = contractId,
          choice = "Archive",
          choiceArgument = Some(LedgerApi.Value().withRecord(LedgerApi.Record())))))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = ApplicationId.unwrap(applicationId),
          commandId = UUID.randomUUID.toString
        )))
    for {
      _ <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield ()
  }

  protected def queryACS(client: LedgerClient, party: String)(
      implicit ec: ExecutionContext): Future[Map[LedgerApi.Identifier, Seq[LedgerApi.Record]]] = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    val contractsF: Future[Seq[CreatedEvent]] = client.activeContractSetClient
      .getActiveContracts(filter, verbose = true)
      .runWith(Sink.seq)
      .map(_.flatMap(x => x.activeContracts))
    contractsF.map(
      contracts =>
        contracts
          .map(created => (created.getTemplateId, created.getCreateArguments))
          .groupBy(_._1)
          .mapValues(cs => cs.map(_._2)))
  }

}
