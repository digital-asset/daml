// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles.rlocation

import java.io.File
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.DarDecoder
import com.daml.lf.data.{Ref, ImmArray}
import com.daml.lf.engine.script.ledgerinteraction.{ScriptLedgerClient, ScriptTimeMode}
import com.daml.lf.engine.script.{Runner, Participants}
import com.daml.lf.language.Ast
import com.daml.lf.language.StablePackage.DA
import com.daml.lf.speedy.{SValue, ArrayList}
import com.daml.lf.value.Value
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

case class CompiledDar(
    mainPkg: Ref.PackageId,
    compiledPackages: PureCompiledPackages,
)

object AbstractScriptTest {

  def readDar(file: File): CompiledDar = {
    val dar = DarDecoder.assertReadArchiveFromFile(file)
    val pkgs = PureCompiledPackages.assertBuild(dar.all.toMap, Runner.compilerConfig)
    CompiledDar(dar.main._1, pkgs)
  }

  def tuple(a: SValue, b: SValue) =
    SValue.SRecord(
      id = DA.Types.Tuple2,
      fields = ImmArray(Ref.Name.assertFromString("_1"), Ref.Name.assertFromString("_2")),
      values = ArrayList(a, b),
    )

  val stableDarPath = new File(rlocation("daml-script/test/script-test.dar"))
  val devDarPath = new File(rlocation("daml-script/test/script-test-1.dev.dar"))
  val stableDar = readDar(stableDarPath)
  val devDar = readDar(devDarPath)
}

// Fixture for a set of participants used in Daml Script tests
trait AbstractScriptTest extends AkkaBeforeAndAfterAll {
  self: Suite =>
  protected def timeMode: ScriptTimeMode

  protected def run(
      clients: Participants[ScriptLedgerClient],
      name: Ref.QualifiedName,
      inputValue: Option[Value] = None,
      dar: CompiledDar,
  )(implicit ec: ExecutionContext): Future[SValue] = {
    val scriptId = Ref.Identifier(dar.mainPkg, name)
    def converter(input: Value, typ: Ast.Type) =
      new com.daml.lf.engine.preprocessing.ValueTranslator(dar.compiledPackages.pkgInterface, false)
        .translateValue(typ, input)
        .left
        .map(_.message)
    Runner
      .run(dar.compiledPackages, scriptId, Some(converter(_, _)), inputValue, clients, timeMode)
      ._2
  }
}
