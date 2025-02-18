// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarDecoder
import org.scalatest.Suite
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class DamlScriptUtilityPackage extends AnyWordSpec with Matchers {
  self: Suite =>

  "Daml-script" should {
    "be a utility package in LF 2.1" in testUtility("daml-script/daml/daml-script-2.1.dar")
    "be a utility package in LF 2.dev" in testUtility("daml-script/daml/daml-script-2.dev.dar")
  }

  def testUtility(location: String): Assertion = {
    val darPath = Paths.get(BazelRunfiles.rlocation(location)).toFile
    val (pkgId, pkg) = DarDecoder.assertReadArchiveFromFile(darPath).main
    assert(!pkg.supportsUpgrades(pkgId))
  }
}
