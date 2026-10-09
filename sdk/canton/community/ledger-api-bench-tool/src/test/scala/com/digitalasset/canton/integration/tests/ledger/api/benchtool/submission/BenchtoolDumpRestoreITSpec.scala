// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixtureIsolated
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  ActiveContractsObserver,
  AllocatedParties,
  ObservedEvents,
}
import com.digitalasset.canton.ledger.api.benchtool.util.TypedActorSystemResourceOwner.Creator
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.actor.typed.{ActorSystem, SpawnProtocol}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class BenchtoolDumpRestoreITSpec extends BenchtoolSandboxFixtureIsolated with HasTempDirectory {
  registerPlugin(NoAuthPlugin(loggerFactory))

  private val postgresPlugin = new UsePostgres(loggerFactory)
  private val sequencerPlugin =
    new UseReferenceBlockSequencer[Postgres](loggerFactory, postgres = Some(postgresPlugin))

  registerPlugin(postgresPlugin)
  registerPlugin(sequencerPlugin)

  final val User = "someUser"
  final val Template1 = "Foo1"
  final val Template2 = "Foo2"
  final val CreateCount = 20
  final val ConsumedFraction = 0.5
  final val Timeout = 30.seconds

  private var system: ActorSystem[SpawnProtocol.Command] = _

  override def beforeAll(): Unit = {
    system = ActorSystem(Creator(), "Creator")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  "benchtool" should {

    val submissionConfig = WorkflowConfig.FooSubmissionConfig(
      numberOfInstances = CreateCount,
      numberOfObservers = 1,
      uniqueParties = false,
      instanceDistribution = List(
        WorkflowConfig.FooSubmissionConfig.ContractDescription(
          template = Template1,
          weight = 1,
          payloadSizeBytes = 0,
        )
      ),
      consumingExercises = Some(
        WorkflowConfig.FooSubmissionConfig.ConsumingExercises(
          probability = ConsumedFraction,
          payloadSizeBytes = 0,
        )
      ),
    )

    "create persistent contracts" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env: TestConsoleEnvironment =>
        import env.*

        def acsEvents(
            apiServices: LedgerApiServices,
            allocatedParties: AllocatedParties,
        ): Future[ObservedEvents] =
          acsObserver(apiServices, allocatedParties.observers.head)

        (for {
          (apiServices, allocatedParties, submission) <- benchtoolFooSubmissionFixture(
            submissionConfig
          )
          _ <- submission.performSubmission(submissionConfig)
          beforeDrop <- acsEvents(apiServices, allocatedParties)
          _ <- Future(stopAll())
          _ <- sequencerPlugin.dumpAllDatabases(actualConfig, tempDirectory)
          _ <- postgresPlugin
            .recreateDatabases(actualConfig)
            .failOnShutdownToAbortException("recreateDatabases")
          _ <- Future(startAll())
          apiServices <- ledgerApiServices
          afterDrop <- acsEvents(apiServices, allocatedParties)
          _ <- Future(stopAll())
          _ <- sequencerPlugin.restoreAllDatabases(actualConfig, tempDirectory)
          _ <- Future(startAll())
          apiServices <- ledgerApiServices
          afterRestore <- acsEvents(apiServices, allocatedParties)
        } yield {
          beforeDrop.numberOfCreatesPerTemplateName(
            Template1
          ) shouldBe CreateCount * ConsumedFraction + 1
          beforeDrop.numberOfConsumingExercisesPerTemplateName(Template1) shouldBe 0

          afterDrop.numberOfCreatesPerTemplateName(Template1) shouldBe 0

          afterRestore.numberOfCreatesPerTemplateName(
            Template1
          ) shouldBe CreateCount * ConsumedFraction + 1
          afterRestore.numberOfConsumingExercisesPerTemplateName(Template1) shouldBe 0
          succeed
        }).futureValue
    }
  }

  private def ledgerApiServices(implicit ec: ExecutionContext): Future[LedgerApiServices] =
    LedgerApiServices
      .forChannel(
        channel = channel,
        authorizationHelper = None,
      )
      .map(_(User))

  // Stolen from PruningITSpec
  private def acsObserver(apiServices: LedgerApiServices, party: Party)(implicit
      ec: ExecutionContext
  ): Future[ObservedEvents] = {
    val eventsObserver = ActiveContractsObserver(expectedTemplateNames = Set(Template1, Template2))
    val config = WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
      name = "dump-restore",
      filters = List(
        WorkflowConfig.StreamConfig.PartyFilter(
          party = party.getValue,
          templates = Nil,
          interfaces = Nil,
        )
      ),
      objectives = None,
      maxItemCount = None,
      timeoutO = None,
    )
    apiServices.stateService.getActiveContracts(
      config = config,
      observer = eventsObserver,
    )
  }

}
