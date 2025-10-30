// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.script.{Runner, ScriptTimeMode}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{newTraceLog, newWarningLog}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}

class AttestationITV2 extends AttestationIT(LanguageVersion.Major.V2)

class AttestationIT(languageVersion: LanguageVersion.Major)
    extends AsyncWordSpec
    with PekkoBeforeAndAfterAll
    with Matchers {

  private val darPath: Path = rlocation(
    Paths.get(s"daml-script/test/attestation-test-v${languageVersion.pretty}.dev.dar")
  )
  private val dar = CompiledDar.read(darPath, Runner.compilerConfig)

  private def converter(input: Value, typ: Ast.Type) =
    new com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator(
      dar.compiledPackages.pkgInterface,
      false,
    )
      .translateValue(typ, input)
      .left
      .map(_.message)

  "Attestation test data can be successfully processed" in {
    val scriptEntryPoint =
      Ref.Identifier(dar.mainPkg, Ref.QualifiedName.assertFromString("AttestationTests:main"))

    for {
      clients <- Runner.ideLedgerClient(
        dar.compiledPackages,
        newTraceLog,
        newWarningLog,
      )
      _ <- Runner
        .run(
          dar.compiledPackages,
          scriptEntryPoint,
          Some(converter(_, _)),
          None,
          clients,
          ScriptTimeMode.Static,
        )
    } yield {
      succeed
    }
  }
}
