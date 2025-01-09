// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class Daml3ScriptTestRunnerDev extends DamlScriptTestRunner {
  self: Suite =>

  override lazy val devMode = true

  val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("compiler/damlc/tests/submit-test.dar"))

  override lazy val darFiles = List(trySubmitTestDarPath)

  val expectedContractNotActiveResponse =
    """FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "contractNotActive no additional info" })"""

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml3-script features" in
      assertDamlScriptRunnerResult(
        trySubmitTestDarPath,
        f"""Daml3ScriptSubmit:authorizationError SUCCESS
           |Daml3ScriptSubmit:contractKeyNotFound SUCCESS
           |Daml3ScriptSubmit:contractNotActive ${expectedContractNotActiveResponse}
           |Daml3ScriptSubmit:createEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptSubmit:devError SUCCESS
           |Daml3ScriptSubmit:fetchEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptSubmit:prefetchContractKeys SUCCESS
           |Daml3ScriptSubmit:truncatedError FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "EXPECTED_TRUNCATED_ERROR" })
           |Daml3ScriptSubmit:unhandledException SUCCESS
           |Daml3ScriptSubmit:wronglyTypedContract SUCCESS
           |""".stripMargin,
      )

  }
}
