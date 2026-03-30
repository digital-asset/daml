// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine.script
package test

import java.nio.file.{Path, Paths}
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonConfig.TimeProviderType
import com.daml.integrationtest.CantonFixture
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  GrpcLedgerClient,
  ScriptLedgerClient,
}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.engine.ScriptEngine.{ExtendedValue, defaultCompilerConfig}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}
import scala.annotation.unused

// Fixture for a set of participants used in Daml Script tests
trait AbstractScriptTest extends CantonFixture with PekkoBeforeAndAfterAll {
  self: Suite =>

  def tuple(a: Value, b: Value): Value =
    Value.ValueRecord(
      Some(StablePackages.stablePackages.Tuple2),
      ImmArray(
        Some(Ref.Name.assertFromString("_1")) -> a,
        Some(Ref.Name.assertFromString("_2")) -> b,
      ),
    )

  override protected lazy val protocolVersion = "v35"

  lazy val darPath: Path = rlocation(
    Paths.get(s"daml-script/test/script-test-v2.3-staging.dar")
  )
  lazy val dar: CompiledDar = CompiledDar.read(darPath, defaultCompilerConfig)

  protected def timeMode: ScriptTimeMode
  override protected lazy val darFiles = List(darPath)

  final override protected lazy val timeProviderType = timeMode match {
    case ScriptTimeMode.Static => TimeProviderType.Static
    case ScriptTimeMode.WallClock => TimeProviderType.WallClock
  }

  final protected def run(
      clients: Participants[ScriptLedgerClient],
      name: Ref.QualifiedName,
      inputValue: Option[Value] = None,
      dar: CompiledDar,
  )(implicit ec: ExecutionContext): Future[ExtendedValue] = {
    val scriptId = Ref.Identifier(dar.mainPkg, name)
    def translate(v: Value, @unused ty: Ast.Type): Either[String, Value] = Right(v)
    Runner
      .run(
        dar.compiledPackages,
        scriptId,
        Some(translate(_, _)),
        inputValue,
        clients,
        timeMode,
      )
  }

  final protected def scriptClients(
      token: Option[String] = None,
      maxInboundMessageSize: Int = RunnerMainConfig.DefaultMaxInboundMessageSize,
      provideAdminPorts: Boolean = false,
  ): Future[Participants[GrpcLedgerClient]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val participants = ledgerPorts.zipWithIndex.map { case (ports, i) =>
      Participant(s"participant$i") -> ApiParameters(
        host = "localhost",
        port = ports.ledgerPort.value,
        access_token = token,
        user_id = None,
        adminPort = if (provideAdminPorts) Some(ports.adminPort.value) else None,
      )
    }
    val params = Participants(
      default_participant = participants.headOption.map(_._2),
      participants = participants.toMap,
      party_participants = Map.empty,
    )
    Runner.connect(
      participantParams = params,
      tlsConfig = config.tlsClientConfig,
      maxInboundMessageSize = maxInboundMessageSize,
    )
  }
}
