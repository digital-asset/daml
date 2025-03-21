// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine.script
package test

import java.nio.file.{Path, Paths}
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonConfig.TimeProviderType
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  GrpcLedgerClient,
  ScriptLedgerClient,
}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion}
import com.digitalasset.daml.lf.speedy.{ArrayList, SValue}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

// Fixture for a set of participants used in Daml Script tests
trait AbstractScriptTest extends CantonFixture with PekkoBeforeAndAfterAll {
  self: Suite =>

  val majorLanguageVersion: LanguageMajorVersion;

  def tuple(a: SValue, b: SValue) =
    SValue.SRecord(
      id = StablePackages(majorLanguageVersion).Tuple2,
      fields = ImmArray(Ref.Name.assertFromString("_1"), Ref.Name.assertFromString("_2")),
      values = ArrayList(a, b),
    )

  // TODO(https://github.com/digital-asset/daml/issues/18457): delete once test cases using keys
  //  are split
  override protected lazy val devMode = true

  lazy val darPath: Path = rlocation(
    // TODO(https://github.com/digital-asset/daml/issues/18457): split key test cases and revert to
    //  non-dev dar
    Paths.get(s"daml-script/test/script-test-v${majorLanguageVersion.pretty}.dev.dar")
  )
  lazy val dar: CompiledDar = CompiledDar.read(darPath, Runner.compilerConfig(majorLanguageVersion))

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
  )(implicit ec: ExecutionContext): Future[SValue] = {
    val scriptId = Ref.Identifier(dar.mainPkg, name)
    def converter(input: Value, typ: Ast.Type) =
      new com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator(
        dar.compiledPackages.pkgInterface,
        false,
      )
        .strictTranslateValue(typ, input)
        .left
        .map(_.message)
    Runner
      .run(
        dar.compiledPackages,
        scriptId,
        Some(converter(_, _)),
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
