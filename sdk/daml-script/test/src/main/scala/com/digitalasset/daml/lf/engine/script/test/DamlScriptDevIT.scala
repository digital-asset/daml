// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.value.Value._

import java.nio.file.Paths
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class DamlScriptDevIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {
  final override protected lazy val devMode = true
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  override val majorLanguageVersion: LanguageVersion.Major = LanguageVersion.Major.V2

  lazy val trySubmitConcurrentlyTestDarPath =
    BazelRunfiles.rlocation(Paths.get("compiler/damlc/tests/try-submit-concurrently-test.dar"))
  lazy val trySubmitConcurrentlyTestDar: CompiledDar =
    CompiledDar.read(trySubmitConcurrentlyTestDarPath, Runner.compilerConfig)

  lazy val queryTestDarPath =
    BazelRunfiles.rlocation(Paths.get("compiler/damlc/tests/query-test.dar"))
  lazy val queryTestDar: CompiledDar =
    CompiledDar.read(queryTestDarPath, Runner.compilerConfig)

  override protected lazy val darFiles = List(
    trySubmitConcurrentlyTestDarPath,
    queryTestDarPath,
  )

  "trySubmitConcurrently" should {
    "return exactly one result per 'Commands' in the same order as the input" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("TrySubmitConcurrently:resultsMatchInputs"),
            dar = trySubmitConcurrentlyTestDar,
          )
      } yield r shouldBe ValueUnit
    }

    "return exactly one successful result and n-1 errors when attempting to exercise n consuming choices on the same contract" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("TrySubmitConcurrently:noDoubleSpend"),
            dar = trySubmitConcurrentlyTestDar,
          )
      } yield r shouldBe ValueUnit
    }
  }

  "query" should {
    "return contracts iff they are visible" in {
      for {
        clients <- scriptClients()
        r <-
          run(
            clients,
            QualifiedName.assertFromString("Query:main"),
            dar = queryTestDar,
          )
      } yield r shouldBe ValueUnit
    }
  }
}
