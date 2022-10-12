// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger
package test

import java.util.UUID
import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand, _}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.lf.archive.DarDecoder
import com.daml.lf.data.Ref._
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.platform.sandbox.{SandboxBackend, SandboxRequiringAuthorizationFuns}
import com.daml.platform.sandbox.services.TestCommands
import org.scalatest._
import scalaz.syntax.tag._
import com.daml.platform.sandbox.fixture.SandboxFixture
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait AbstractTriggerTest
    extends SandboxFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SandboxRequiringAuthorizationFuns {
  self: Suite =>

  protected def toHighLevelResult(s: SValue) = s match {
    case SRecord(_, _, values) if values.size == 5 =>
      AbstractTriggerTest.HighLevelResult(
        values.get(0),
        values.get(1),
        values.get(2),
        values.get(3),
        values.get(4),
      )
    case _ => throw new IllegalArgumentException(s"Expected record with 5 fields but got $s")
  }

  protected val applicationId = RunnerConfig.DefaultApplicationId

  protected def ledgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = None,
    )

  protected def ledgerClientChannelConfiguration =
    LedgerClientChannelConfiguration.InsecureDefaults

  protected def ledgerClient(
      maxInboundMessageSize: Int = RunnerConfig.DefaultMaxInboundMessageSize,
      config: Option[LedgerClientConfiguration] = None,
  )(implicit ec: ExecutionContext): Future[LedgerClient] = {
    val effectiveConfig = config.getOrElse(ledgerClientConfiguration)
    for {
      client <- LedgerClient
        .singleHost(
          "localhost",
          serverPort.value,
          effectiveConfig,
          ledgerClientChannelConfiguration.copy(maxInboundMessageSize = maxInboundMessageSize),
        )
    } yield client
  }

  override protected def darFile = {
    val dars = for {
      trigger_version <- List("stable", "dev").view
      lf_version <- List("1.14", "1.dev")
      x <- Try(
        BazelRunfiles.requiredResource(s"triggers/tests/acs-${trigger_version}-${lf_version}.dar")
      ).toOption.toList
    } yield x

    dars.head
  }

  protected val dar = DarDecoder.assertReadArchiveFromFile(darFile)
  protected val compiledPackages =
    PureCompiledPackages.assertBuild(dar.all.toMap, speedy.Compiler.Config.Dev)

  protected def getRunner(
      client: LedgerClient,
      name: QualifiedName,
      party: String,
      readAs: Set[String] = Set.empty,
  ): Runner = {
    val triggerId = Identifier(packageId, name)
    Trigger.newLoggingContext(triggerId, Party(party), Party.subst(readAs)) {
      implicit loggingContext =>
        val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).toOption.get
        new Runner(
          compiledPackages,
          trigger,
          client,
          config.participants(ParticipantId).apiServer.timeProviderType,
          applicationId,
          TriggerParties(
            actAs = Party(party),
            readAs = Party.subst(readAs),
          ),
        )
    }
  }

  protected def allocateParty(client: LedgerClient)(implicit ec: ExecutionContext): Future[String] =
    client.partyManagementClient.allocateParty(None, None).map(_.party)

  protected def create(client: LedgerClient, party: String, cmd: CreateCommand)(implicit
      ec: ExecutionContext
  ): Future[String] = {
    val commands = Seq(Command().withCreate(cmd))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = ApplicationId.unwrap(applicationId),
          commandId = UUID.randomUUID.toString,
        )
      )
    )
    for {
      response <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield response.getTransaction.events.head.getCreated.contractId
  }

  protected def archive(
      client: LedgerClient,
      party: String,
      templateId: LedgerApi.Identifier,
      contractId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val commands = Seq(
      Command().withExercise(
        ExerciseCommand(
          templateId = Some(templateId),
          contractId = contractId,
          choice = "Archive",
          choiceArgument = Some(LedgerApi.Value().withRecord(LedgerApi.Record())),
        )
      )
    )
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = ApplicationId.unwrap(applicationId),
          commandId = UUID.randomUUID.toString,
        )
      )
    )
    for {
      _ <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield ()
  }

  protected def queryACS(client: LedgerClient, party: String)(implicit
      ec: ExecutionContext
  ): Future[Map[LedgerApi.Identifier, Seq[LedgerApi.Record]]] = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    val contractsF: Future[Seq[CreatedEvent]] = client.activeContractSetClient
      .getActiveContracts(filter, verbose = true)
      .runWith(Sink.seq)
      .map(_.flatMap(x => x.activeContracts))
    contractsF.map(contracts =>
      contracts
        .map(created => (created.getTemplateId, created.getCreateArguments))
        .groupBy(_._1)
        .view
        .mapValues(cs => cs.map(_._2))
        .toMap
    )
  }

}

object AbstractTriggerTest {
  final case class HighLevelResult(
      acs: SValue,
      party: SValue,
      readAs: SValue,
      state: SValue,
      commandsInFlight: SValue,
  )
}
