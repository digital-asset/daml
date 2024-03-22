// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package trigger
package test

import org.apache.pekko.stream.Materializer

import java.util.UUID
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
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
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ApplicationId => _, Party => _, _}
import com.daml.lf.engine.trigger.TriggerRunnerConfig.DefaultTriggerRunnerConfig
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import org.scalatest._
import scalaz.syntax.tag._

import java.nio.file.Path
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait AbstractTriggerTest extends CantonFixture {
  self: Suite =>

  protected val majorLanguageVersion: LanguageMajorVersion

  protected lazy val darFile: Path =
    BazelRunfiles.requiredResource(s"triggers/tests/acs-${majorLanguageVersion.pretty}.dar").toPath

  override protected lazy val darFiles: List[Path] = List(darFile)
  // TODO(#17366): remove once 2.0 is introduced
  override protected lazy val devMode: Boolean = (majorLanguageVersion == LanguageMajorVersion.V2)

  implicit override protected lazy val applicationId: Option[Ref.ApplicationId] =
    RunnerConfig.DefaultApplicationId

  protected def toHighLevelResult(s: SValue) = s match {
    case SRecord(_, _, values) if values.size == 6 =>
      AbstractTriggerTest.HighLevelResult(
        values.get(0),
        values.get(1),
        values.get(2),
        values.get(3),
        values.get(4),
        values.get(5),
      )
    case _ => throw new IllegalArgumentException(s"Expected record with 6 fields but got $s")
  }

  protected def ledgerClientConfiguration: LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = applicationId.getOrElse(""),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = None,
    )

  protected def ledgerClientChannelConfiguration: LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration.InsecureDefaults

  protected def triggerRunnerConfiguration: TriggerRunnerConfig = DefaultTriggerRunnerConfig

  protected val CompiledDar(packageId, compiledPackages) = {
    CompiledDar.read(darFile, speedy.Compiler.Config.Dev(majorLanguageVersion))
  }

  protected def getRunner(
      client: LedgerClient,
      name: QualifiedName,
      party: Ref.Party,
      readAs: Set[Ref.Party] = Set.empty,
  ): Runner = {
    val triggerId = Identifier(packageId, name)

    Trigger.newTriggerLogContext(
      triggerId,
      party,
      readAs,
      "test-trigger",
      applicationId,
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
          actAs = party,
          readAs = readAs,
        ),
      )
    }
  }
}

object AbstractTriggerTest {
  final case class HighLevelResult(
      acs: SValue,
      party: SValue,
      readAs: SValue,
      state: SValue,
      commandsInFlight: SValue,
      config: SValue,
  )

  def allocateParty(
      client: LedgerClient,
      hint: Option[String] = None,
      displayName: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[Ref.Party] =
    client.partyManagementClient.allocateParty(hint, displayName).map(_.party)

  def queryACS(client: LedgerClient, party: Ref.Party)(implicit
      ec: ExecutionContext,
      materializer: Materializer,
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

  def create(client: LedgerClient, party: Ref.Party, cmd: CreateCommand)(implicit
      ec: ExecutionContext,
      applicationId: Option[Ref.ApplicationId],
  ): Future[String] = {
    val commands = Seq(Command().withCreate(cmd))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = applicationId.getOrElse(""),
          commandId = UUID.randomUUID.toString,
        )
      )
    )
    for {
      response <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield response.getTransaction.events.head.getCreated.contractId
  }

  def create(
      client: LedgerClient,
      party: Ref.Party,
      commands: Seq[CreateCommand],
      elements: Int = 50,
      per: FiniteDuration = 1.second,
  )(implicit
      ec: ExecutionContext,
      materializer: Materializer,
      applicationId: Option[Ref.ApplicationId],
  ): Future[Unit] = {
    Source(commands)
      .mapAsync(8) { cmd =>
        create(client, party, cmd)
      }
      .throttle(elements, per)
      .run()
      .map(_ => ())
  }

  def exercise(
      client: LedgerClient,
      party: Ref.Party,
      templateId: LedgerApi.Identifier,
      contractId: String,
      choice: String,
      choiceArgument: LedgerApi.Value,
  )(implicit ec: ExecutionContext, applicationId: Option[Ref.ApplicationId]): Future[Unit] = {
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
          applicationId = applicationId.getOrElse(""),
          commandId = UUID.randomUUID.toString,
        )
      )
    )
    for {
      _ <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield ()
  }

  def archive(
      client: LedgerClient,
      party: Ref.Party,
      templateId: LedgerApi.Identifier,
      contractId: String,
  )(implicit ec: ExecutionContext, applicationId: Option[Ref.ApplicationId]): Future[Unit] = {
    exercise(
      client,
      party,
      templateId,
      contractId,
      "Archive",
      LedgerApi.Value().withRecord(LedgerApi.Record()),
    )
  }
}
