// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import java.nio.file.{Path, Paths}
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class MultiUpgradesIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock
  override val disableUpgradeValidation = true

  final override protected lazy val devMode = true

  lazy val testDarPath: Path = rlocation(
    Paths.get(s"daml-script/test/upgrades-test.dar")
  )
  lazy val testDar: CompiledDar =
    CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V1))

  override protected lazy val darFiles = List(
    rlocation(Paths.get(s"daml-script/test/upgrades-my-templates-v0.dar")),
    rlocation(Paths.get(s"daml-script/test/upgrades-my-templates-v1.dar")),
    rlocation(Paths.get(s"daml-script/test/upgrades-my-templates-v2.dar")),
  )

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    "run successfully" in {
      for {
        clients <- scriptClients(provideAdminPorts = true)
        _ <- run(
          clients,
          QualifiedName.assertFromString("UpgradesTest:main"),
          dar = testDar,
          enableContractUpgrading = true,
        )
      } yield succeed
    }
  }
}
