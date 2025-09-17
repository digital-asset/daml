// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.ledger.javaapi.data.Event
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{
  BaseTest,
  ProtocolVersionChecksFixtureAnyWordSpec,
  RepeatableTestSuiteTest,
  config,
}
import org.scalactic.source
import org.scalactic.source.Position
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, ConfigMap, Outcome}
import org.slf4j.event.Level.WARN

import scala.collection.immutable
import scala.jdk.CollectionConverters.*

/** A highly opinionated base trait for writing integration tests interacting with a canton
  * environment using console commands. Tests must mixin a further [[EnvironmentSetup]]
  * implementation to define when the canton environment is setup around the individual tests:
  *   - [[IsolatedEnvironments]] will construct a fresh environment for each test.
  *   - [[SharedEnvironment]] will construct only a single environment and reuse this for each test
  *     executed in the test class.
  *
  * Test classes must override [[HasEnvironmentDefinition.environmentDefinition]] to describe how
  * they would like their environment configured.
  *
  * This here is the abstract definition. A concrete integration test must derive the configuration
  * type. Have a look at the EnvironmentDefinition objects in the community or enterprise app
  * sub-project.
  *
  * Code blocks interacting with the environment are provided a [[TestEnvironment]] instance.
  * [[TestEnvironment]] provides all implicits and commands to interact with the environment as if
  * you were operating in the canton console. For convenience you will want to mark this value as an
  * `implicit` and import the instances members into your scope (see `withSetup` and tests in the
  * below example). [[TestEnvironment]] also includes [[CommonTestAliases]] which will give you
  * references to synchronizers and participants commonly used in our tests. If your test attempts
  * to use a participant or synchronizer which is not configured in your environment it will
  * immediately fail.
  *
  * By default sbt will attempt to run many tests concurrently. This can be problematic as starting
  * many canton environments concurrently is very resource intensive. We use
  * [[ConcurrentEnvironmentLimiter]] to limit how many environments are running concurrently. By
  * default this limit is 2 but can be modified by setting the system property
  * [[ConcurrentEnvironmentLimiter.IntegrationTestConcurrencyLimit]].
  *
  * All integration tests must be located in package [[com.digitalasset.canton.integration.tests]]
  * or a subpackage thereof. This is required to correctly compute unit test coverage.
  */
private[integration] trait BaseIntegrationTest
    extends FixtureAnyWordSpec
    with BaseTest
    with RepeatableTestSuiteTest
    with ProtocolVersionChecksFixtureAnyWordSpec {
  this: EnvironmentSetup =>

  type FixtureParam = TestConsoleEnvironment

  override protected def withFixture(test: OneArgTest): Outcome = {
    val integrationTestPackage = "com.digitalasset.canton.integration.tests"
    getClass.getName should startWith(
      integrationTestPackage
    ) withClue s"\nAll integration tests must be located in $integrationTestPackage or a subpackage thereof."

    super[RepeatableTestSuiteTest].withFixture(new TestWithSetup(test))
  }

  /** Version of [[com.digitalasset.canton.logging.SuppressingLogger.assertThrowsAndLogs]] that is
    * specifically tailored to [[com.digitalasset.canton.console.CommandFailure]].
    */
  // We cannot define this in SuppressingLogger, because CommandFailure is not visible there.
  def assertThrowsAndLogsCommandFailures(within: => Any, assertions: (LogEntry => Assertion)*)(
      implicit pos: source.Position
  ): Assertion =
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      within,
      assertions.map { assertion => (entry: LogEntry) =>
        assertion(entry)
        entry.commandFailureMessage
        succeed
      } *,
    )

  /** Version of [[com.digitalasset.canton.logging.SuppressingLogger.assertThrowsAndLogs]] that is
    * specifically tailored to [[com.digitalasset.canton.console.CommandFailure]].
    */
  def assertThrowsAndLogsCommandFailuresUnordered(
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit
      pos: source.Position
  ): Assertion =
    loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
      within,
      assertions.map { assertion => (entry: LogEntry) =>
        assertion(entry)
        entry.commandFailureMessage
        succeed
      } *,
    )

  def suppressPackageIdWarning[A](within: => A): A =
    loggerFactory.assertLogsSeq(SuppressionRule.Level(WARN))(
      within,
      entries =>
        forAtLeast(1, entries) { e =>
          e.warningMessage should (include regex "Received an identifier with package ID .*, but expected a package name.")
        },
    )

  /** Similar to [[com.digitalasset.canton.console.commands.ParticipantAdministration#ping]] But
    * unlike `ping`, this version mixes nicely with `eventually`.
    */
  def assertPingSucceeds(
      sender: ParticipantReference,
      receiver: ParticipantReference,
      timeoutMillis: Long = 20000,
      synchronizerId: Option[SynchronizerId] = None,
      id: String = "",
  ): Assertion =
    withClue(s"${sender.name} was unable to ping ${receiver.name} within ${timeoutMillis}ms:") {
      sender.health.maybe_ping(
        receiver.id,
        config.NonNegativeDuration.ofMillis(timeoutMillis),
        synchronizerId = synchronizerId,
        id = id,
      ) shouldBe defined
    }

  class TestWithSetup(test: OneArgTest) extends NoArgTest {
    override val configMap: ConfigMap = test.configMap
    override val name: String = test.name
    override val scopes: immutable.IndexedSeq[String] = test.scopes
    override val text: String = test.text
    override val tags: Set[String] = test.tags
    override val pos: Option[Position] = test.pos

    override def apply(): Outcome = {
      val environment = provideEnvironment
      val testOutcome =
        try test.toNoArgTest(environment)()
        finally testFinished(environment)
      testOutcome
    }
  }

  import com.daml.ledger.javaapi.data.{Command, CreateCommand, ExerciseCommand, Identifier}
  implicit class EnrichedCommands(commands: java.util.List[Command]) {
    def overridePackageId(packageIdOverride: String): java.util.List[Command] =
      commands.asScala
        .map {
          case cmd: CreateCommand =>
            new CreateCommand(
              identifierWithPackageIdOverride(packageIdOverride, cmd.getTemplateId),
              cmd.getCreateArguments,
            ): Command
          case cmd: ExerciseCommand =>
            new ExerciseCommand(
              identifierWithPackageIdOverride(packageIdOverride, cmd.getTemplateId),
              cmd.getContractId,
              cmd.getChoice,
              cmd.getChoiceArgument,
            )
          case other => fail(s"Unexpected command $other")
        }
        .toList
        .asJava

    private def identifierWithPackageIdOverride(packageIdOverride: String, templateId: Identifier) =
      new Identifier(
        packageIdOverride,
        templateId.getModuleName,
        templateId.getEntityName,
      )
  }

  implicit class EnrichedEvent(events: Event) {
    def asScalaProtoEvent: com.daml.ledger.api.v2.event.Event =
      com.daml.ledger.api.v2.event.Event.fromJavaProto(events.toProtoEvent)

    def asScalaProtoCreated: Option[com.daml.ledger.api.v2.event.CreatedEvent] =
      com.daml.ledger.api.v2.event.Event.fromJavaProto(events.toProtoEvent).event.created
  }

  implicit class EnrichedEvents(events: java.util.List[Event]) {
    def asScalaProtoEvents: List[com.daml.ledger.api.v2.event.Event] =
      events.asScala.iterator
        .map(_.asScalaProtoEvent)
        .toList

    def asScalaProtoCreatedContracts: List[com.daml.ledger.api.v2.event.CreatedEvent] =
      events.asScala.iterator
        .flatMap(_.asScalaProtoCreated)
        .toList
  }
}
