// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class DamlScriptLtsTestRunnerPreUpgrade extends DamlScriptLtsTestRunner {
  override val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/submit-test-1.15.dar"))
}

class DamlScriptLtsTestRunnerPostUpgrade extends DamlScriptLtsTestRunner {

  // TODO: Remomve as soon PV=6 is stable
  override lazy val devMode = true

  override val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("compiler/damlc/tests/submit-test.dar"))
}

abstract class DamlScriptLtsTestRunner extends GenericDamlScriptTestRunner {
  self: Suite =>

  def trySubmitTestDarPath: java.nio.file.Path

  override lazy val darFiles = List(trySubmitTestDarPath)

  val expectedContractNotActiveResponse =
    """FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "contractNotActive no additional info" })"""

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml-script-lts features" in
      assertDamlScriptRunnerResult(
        trySubmitTestDarPath,
        // scripts are executed in alphabetical order
        f"""DamlScriptLtsSubmit:authorizationError SUCCESS
           |DamlScriptLtsSubmit:contractKeyNotFound SUCCESS
           |DamlScriptLtsSubmit:contractNotActive ${expectedContractNotActiveResponse}
           |DamlScriptLtsSubmit:createEmptyContractKeyMaintainers SUCCESS
           |DamlScriptLtsSubmit:duplicateContractKey SUCCESS
           |DamlScriptLtsSubmit:fetchEmptyContractKeyMaintainers SUCCESS
           |DamlScriptLtsSubmit:prefetchContractKeys SUCCESS
           |DamlScriptLtsSubmit:truncatedError FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "EXPECTED_TRUNCATED_ERROR" })
           |DamlScriptLtsSubmit:unhandledException SUCCESS
           |DamlScriptLtsSubmit:wronglyTypedContract SUCCESS
           |""".stripMargin,
      )

  }
}
