// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services.command

import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v2.commands.CreateCommand
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.{CantonConfig, ClockConfig, DbConfig}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{CantonFixture, CreatesParties}
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.MockMessages
import com.digitalasset.canton.ledger.api.util.DurationConversion
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.google.protobuf.duration.Duration as ProtoDuration
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.{Duration, Instant}
import java.util.UUID

sealed trait CommandServiceITBase extends CantonFixture with CreatesParties with TestCommands {

  private def findParty(participant: LocalParticipantReference, party: String) =
    eventually(retryOnTestFailuresOnly = false) {
      participant.parties.find(party).toProtoPrimitive
    }

  private def command(party: String) =
    CreateCommand(
      Some(templateIds.dummy),
      Some(
        Record(
          Some(templateIds.dummy),
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party))))),
        )
      ),
    ).wrap

  private def submitAndWaitRequest(party: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := UUID.randomUUID().toString,
        _.commands.actAs := Seq(party),
      )

  private def submitRequest(party: String) =
    MockMessages.submitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := UUID.randomUUID().toString,
        _.commands.actAs := Seq(party),
      )

  private[this] def assertExpectedDelay(
      start: Instant,
      end: Instant,
      minLedgerTimeRel: ProtoDuration,
  ): Assertion = {
    val avgLatency = Duration.ZERO
    val expectedDuration = DurationConversion.fromProto(minLedgerTimeRel).minus(avgLatency)
    val actualDuration = Duration.between(start, end)
    assert(
      actualDuration.compareTo(expectedDuration) != -1,
      s"Expected submission duration was $expectedDuration, actual duration was $actualDuration",
    )
  }

  private val partyHints = MockMessages.submitRequest.getCommands.actAs

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, partyHints.toList)(
      directExecutionContext
    )
  }

  "CommandSubmissionService" when {
    "receiving a command with minLedgerTimeRel" should {
      "delay the submission" in { env =>
        import env.*
        val party = findParty(participant1, partyHints.head)
        val submissionService = CommandSubmissionServiceGrpc.stub(channel)
        val minLedgerTimeRel = ProtoDuration.of(5, 0)
        val request =
          submitRequest(party).update(_.commands.minLedgerTimeRel := minLedgerTimeRel)

        val start = Instant.now
        (for {
          _ <- submissionService.submit(request)
          end = Instant.now
        } yield {
          assertExpectedDelay(start, end, minLedgerTimeRel)
        }).futureValue
      }
    }
  }

  "CommandService" when {
    "receiving a command with minLedgerTimeRel" should {
      "delay the submission" in { env =>
        import env.*
        val party = findParty(participant1, partyHints.head)
        val commandService = CommandServiceGrpc.stub(channel)
        val minLedgerTimeRel = ProtoDuration.of(5, 0)
        val request =
          submitAndWaitRequest(party).update(_.commands.minLedgerTimeRel := minLedgerTimeRel)

        val start = Instant.now
        (for {
          _ <- commandService.submitAndWait(request)
          end = Instant.now
        } yield {
          assertExpectedDelay(start, end, minLedgerTimeRel)
        }).futureValue
      }
    }
  }
}

//  plugin to override the configuration
final case class CommandServiceOverrideConfig(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms
      .updateParticipantConfig("participant1")(
        _.focus(_.testingTime)
          .replace(None)
          .focus(_.ledgerApi.authServices)
          .replace(Seq(Wildcard))
      )(config)
      .focus(_.parameters.clock)
      .replace(ClockConfig.WallClock())
}

class CommandServiceITPostgres extends CommandServiceITBase {
  registerPlugin(CommandServiceOverrideConfig(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
