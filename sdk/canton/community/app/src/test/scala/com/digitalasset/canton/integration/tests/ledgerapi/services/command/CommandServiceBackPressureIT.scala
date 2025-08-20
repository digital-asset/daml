// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services.command

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.commands.CreateCommand
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CantonConfig, DbConfig}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{CantonFixture, CreatesParties}
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.{IsStatusException, MockMessages}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

sealed trait CommandServiceBackPressureITBase
    extends CantonFixture
    with CreatesParties
    with TestCommands {

  private val commands = 100

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

  private def pushedBack(t: Throwable): Boolean = t match {
    case GrpcException(GrpcStatus.RESOURCE_EXHAUSTED(), _) => true
    case _ => false
  }

  private def testBackPressure[A](
      requests: Seq[Future[A]]
  )(implicit ec: ExecutionContext): Assertion =
    (Future
      .sequence(requests.map(_.map(Success(_)).recover { case ex =>
        Failure(ex)
      }))
      .map(_.collect { case Failure(ex) => ex }) map { errors =>
      info(s"${errors.size}/$commands requests failed")
      info(s"${errors.count(pushedBack)}/${errors.size} errors are push-backs")
      errors should not be empty
      forAll(errors)(IsStatusException(Status.ABORTED))
    }).futureValue

  private val partyHints = MockMessages.submitRequest.getCommands.actAs

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, partyHints.toList)(directExecutionContext)
  }

  "CommandService" when {
    "overloaded with commands" should {
      "reject requests with RESOURCE_EXHAUSTED" in { env =>
        import env.*
        val service = CommandServiceGrpc.stub(channel)
        val party = participant1.parties.find(partyHints.head).toProtoPrimitive
        testBackPressure(
          Seq.fill(commands)(submitAndWaitRequest(party)).map(service.submitAndWait)
        )
      }
    }
  }
}

//  plugin to override the configuration and cause backpressure
final case class BackpressureOverrideConfig(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms.updateParticipantConfig("participant1") {
      _.focus(_.ledgerApi.commandService.maxCommandsInFlight)
        .replace(2)
        .focus(_.parameters.ledgerApiServer.indexer.inputMappingParallelism)
        .replace(NonNegativeInt.tryCreate(2))
        .focus(_.ledgerApi.authServices)
        .replace(Seq(Wildcard))
    }(config)
}

class CommandServiceBackPressureITPostgres extends CommandServiceBackPressureITBase {
  registerPlugin(BackpressureOverrideConfig(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
