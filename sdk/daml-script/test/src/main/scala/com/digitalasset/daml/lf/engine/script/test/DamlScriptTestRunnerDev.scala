// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class DamlScriptTestRunnerDev extends DamlScriptTestRunner {
  self: Suite =>

  override lazy val devMode = true

  val trySubmitTestDarPath =
    Paths.get(BazelRunfiles.rlocation("compiler/damlc/tests/submit-test.dar"))

  override lazy val darFiles = List(trySubmitTestDarPath)

  val expectedContractNotActiveResponse =
    """FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "contractNotActive no additional info" })"""

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml-script features" in
      assertDamlScriptRunnerResult(
        trySubmitTestDarPath,
        f"""Submit:authorizationError SUCCESS
           |Submit:contractKeyNotFound SUCCESS
           |Submit:contractNotActive ${expectedContractNotActiveResponse}
           |Submit:createEmptyContractKeyMaintainers SUCCESS
           |Submit:devError SUCCESS
           |Submit:fetchEmptyContractKeyMaintainers SUCCESS
           |Submit:prefetchContractKeys SUCCESS
           |Submit:truncatedError FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "EXPECTED_TRUNCATED_ERROR" })
           |Submit:unhandledException SUCCESS
           |Submit:wronglyTypedContract SUCCESS
           |""".stripMargin,
      )

  }
}
