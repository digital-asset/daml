// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class Daml3ScriptTestRunner extends DamlScriptTestRunner {
  self: Suite =>

  val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/try-submit-test-1.15.dar"))

  override lazy val darFiles = List(trySubmitTestDarPath)

  val expectedContractNotActiveResponse =
    """FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "contractNotActive no additional info" })"""

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml3-script features" in
      assertDamlScriptRunnerResult(
        trySubmitTestDarPath,
        f"""Daml3ScriptTrySubmit15:authorizationError SUCCESS
           |Daml3ScriptTrySubmit15:contractKeyNotFound SUCCESS
           |Daml3ScriptTrySubmit15:contractNotActive ${expectedContractNotActiveResponse}
           |Daml3ScriptTrySubmit15:createEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptTrySubmit15:duplicateContractKey SUCCESS
           |Daml3ScriptTrySubmit15:fetchEmptyContractKeyMaintainers SUCCESS
           |Daml3ScriptTrySubmit15:truncatedError FAILURE (com.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "EXPECTED_TRUNCATED_ERROR" })
           |Daml3ScriptTrySubmit15:unhandledException SUCCESS
           |Daml3ScriptTrySubmit15:wronglyTypedContract SUCCESS
           |""".stripMargin,
      )

  }
}
