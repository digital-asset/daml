// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger
package test

import java.util.UUID
import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand, _}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{CommandClientConfiguration, LedgerClientChannelConfiguration, LedgerClientConfiguration, LedgerIdRequirement}
import com.daml.lf.data.Ref._
import com.daml.lf.engine.trigger.TriggerRunnerConfig.DefaultTriggerRunnerConfig
import com.daml.lf.integrationtest.CantonFixture
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.platform.services.time.TimeProviderType
import org.scalatest._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

// TODO: once test migration work has completed, rename this trait to AbstractTriggerTest
trait AbstractTriggerTestWithCanton
  extends CantonFixture
  with SuiteResourceManagementAroundAll {
  self: Suite =>

  import CantonFixture._

  private[this] lazy val darFile =
    Try(BazelRunfiles.requiredResource("triggers/tests/acs.dar"))
      .getOrElse(BazelRunfiles.requiredResource("triggers/tests/acs-1.dev.dar"))

  override protected def authSecret = None
  override protected def darFiles = List(darFile.toPath)
  override protected def devMode = true
  override protected def nParticipants = 1
  override protected def timeProviderType = TimeProviderType.Static
  override protected def tlsEnable = false

  protected def toHighLevelResult(s: SValue) = s match {
    case SRecord(_, _, values) if values.size == 6 =>
      AbstractTriggerTestWithCanton.HighLevelResult(
        values.get(0),
        values.get(1),
        values.get(2),
        values.get(3),
        values.get(4),
        values.get(5),
      )
    case _ => throw new IllegalArgumentException(s"Expected record with 6 fields but got $s")
  }

  protected val applicationId: ApplicationId = RunnerConfig.DefaultApplicationId

  protected def ledgerClientConfiguration: LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = None,
    )

  protected def ledgerClientChannelConfiguration: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration.InsecureDefaults

  protected def triggerRunnerConfiguration: TriggerRunnerConfig = DefaultTriggerRunnerConfig

  protected val CompiledDar(packageId, compiledPackages) = readDar(darFile.toPath, speedy.Compiler.Config.Dev)

  protected def getRunner(
                           client: LedgerClient,
                           name: QualifiedName,
                           party: String,
                           readAs: Set[String] = Set.empty,
                         ): Runner = {
    val triggerId = Identifier(packageId, name)

    Trigger.newTriggerLogContext(
      triggerId,
      Party(party),
      Party.subst(readAs),
      "test-trigger",
      ApplicationId("test-trigger-app"),
    ) { implicit triggerContext: TriggerLogContext =>
      val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).toOption.get

      Runner(
        compiledPackages,
        trigger,
        triggerRunnerConfiguration,
        client,
        timeProviderType,
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

  protected def exercise(
                          client: LedgerClient,
                          party: String,
                          templateId: LedgerApi.Identifier,
                          contractId: String,
                          choice: String,
                          choiceArgument: LedgerApi.Value,
                        )(implicit ec: ExecutionContext): Future[Unit] = {
    val commands = Seq(
      Command().withExercise(
        ExerciseCommand(
          templateId = Some(templateId),
          contractId = contractId,
          choice = choice,
          choiceArgument = Some(choiceArgument),
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

  protected def archive(
                         client: LedgerClient,
                         party: String,
                         templateId: LedgerApi.Identifier,
                         contractId: String,
                       )(implicit ec: ExecutionContext): Future[Unit] = {
    exercise(
      client,
      party,
      templateId,
      contractId,
      "Archive",
      LedgerApi.Value().withRecord(LedgerApi.Record()),
    )
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

object AbstractTriggerTestWithCanton {
  final case class HighLevelResult(
                                    acs: SValue,
                                    party: SValue,
                                    readAs: SValue,
                                    state: SValue,
                                    commandsInFlight: SValue,
                                    config: SValue,
                                  )
}
