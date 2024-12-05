// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class Daml3ScriptTestRunnerPreUpgrade extends Daml3ScriptTestRunner {
  override val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/try-submit-test-1.15.dar"))
}

class Daml3ScriptTestRunnerPostUpgrade extends Daml3ScriptTestRunner {

  // TODO: Remomve as soon PV=6 is stable
  override lazy val devMode = true

  override val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("compiler/damlc/tests/try-submit-test.dar"))
}

abstract class Daml3ScriptTestRunner extends DamlScriptTestRunner {
  self: Suite =>

  def trySubmitTestDarPath: java.nio.file.Path

  override lazy val darFiles = List(trySubmitTestDarPath)

  val expectedContractNotActiveResponse =
    """FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "contractNotActive no additional info" })"""

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml3-script features" in
      assertDamlScriptRunnerResult(
        trySubmitTestDarPath,
        f"""Daml3ScriptTrySubmit:authorizationError SUCCESS
           |Daml3ScriptTrySubmit:contractKeyNotFound SUCCESS
           |Daml3ScriptTrySubmit:contractNotActive ${expectedContractNotActiveResponse}
           |Daml3ScriptTrySubmit:createEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptTrySubmit:duplicateContractKey SUCCESS
           |Daml3ScriptTrySubmit:fetchEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptTrySubmit:truncatedError FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "EXPECTED_TRUNCATED_ERROR" })
           |Daml3ScriptTrySubmit:unhandledException SUCCESS
           |Daml3ScriptTrySubmit:wronglyTypedContract SUCCESS
           |""".stripMargin,
      )

  }
}
