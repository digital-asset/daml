// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarDecoder
import org.scalatest.Suite
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class DamlScriptUtilityPackage extends AnyWordSpec with Matchers {
  self: Suite =>

  // "Daml-script" should {
  //   "be a utility package in LF 1.14" in testUtility("daml-script/daml/daml-script-1.14.dar")
  //   "be a utility package in LF 1.15" in testUtility("daml-script/daml/daml-script-1.15.dar")
  //   "be a utility package in LF 1.17" in testUtility("daml-script/daml/daml-script-1.17.dar")
  //   "be a utility package in LF 1.dev" in testUtility("daml-script/daml/daml-script-1.dev.dar")
  // }

  "Daml3-script" should {
    "be a utility package in LF 1.14" in testUtility("daml-script/daml3/daml3-script-1.14.dar")
    "be a utility package in LF 1.15" in testUtility("daml-script/daml3/daml3-script-1.15.dar")
    "be a utility package in LF 1.17" in testUtility("daml-script/daml3/daml3-script-1.17.dar")
    "be a utility package in LF 1.dev" in testUtility("daml-script/daml3/daml3-script-1.dev.dar")
  }

  def testUtility(location: String): Assertion = {
    val darPath = Paths.get(BazelRunfiles.rlocation(location)).toFile
    val (_, pkg) = DarDecoder.assertReadArchiveFromFile(darPath).main
    assert(pkg.isUtilityPackage)
  }
}
