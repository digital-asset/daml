// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue._

import java.nio.file.Paths
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class DevIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {
  final override protected lazy val devMode = true
  // TODO: https://github.com/digital-asset/daml/issues/17082
  final override protected lazy val timeMode = ScriptTimeMode.WallClock
  override val disableUpgradeValidation = true

  val prettyLfVersion = "1.dev"

  lazy val devDarPath =
    BazelRunfiles.rlocation(Paths.get(s"daml-script/test/script-test-$prettyLfVersion.dar"))
  lazy val devDar = CompiledDar.read(devDarPath, Runner.compilerConfig(LanguageMajorVersion.V1))

  override protected lazy val darFiles = List(devDarPath)

  // TODO: https://github.com/digital-asset/daml/issues/15882
  // -- Enable this test when canton supports choice observers
  "ChoiceAuthority:test" should {
    "succeed" ignore {
      for {
        clients <- scriptClients()
        v <- run(
          clients,
          QualifiedName.assertFromString("TestChoiceAuthority:test"),
          dar = devDar,
        )
      } yield {
        v shouldBe (SUnit)
      }
    }
  }
}
