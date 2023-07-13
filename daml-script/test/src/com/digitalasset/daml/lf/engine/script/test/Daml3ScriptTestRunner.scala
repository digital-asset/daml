// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

class Daml3ScriptTestRunner extends DamlScriptTestRunner {
  self: Suite =>

  override lazy val devMode = true

  val darPath = BazelRunfiles.rlocation("daml-script/test/script-test.dar")
  val daml3DarPath = BazelRunfiles.rlocation("compiler/damlc/tests/try-submit-test.dar")

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs for daml3-script features" in
      assertDamlScriptRunnerResult(
        daml3DarPath,
        """Daml3ScriptTrySubmit:contractKeyNotFound SUCCESS
          |Daml3ScriptTrySubmit:authorizationError SUCCESS
          |Daml3ScriptTrySubmit:contractNotActive SUCCESS
          |Daml3ScriptTrySubmit:duplicateContractKey SUCCESS
          |Daml3ScriptTrySubmit:unhandledException SUCCESS
          |Daml3ScriptTrySubmit:createEmptyContractKeyMaintainers SUCCESS
          |Daml3ScriptTrySubmit:fetchEmptyContractKeyMaintainers SUCCESS
          |Daml3ScriptTrySubmit:wronglyTypedContract SUCCESS
          |Daml3ScriptTrySubmit:devError SUCCESS
          |""".stripMargin,
      )
    "pick up all scripts and returns somewhat sensible outputs" in
      assertDamlScriptRunnerResult(
        darPath,
        """MultiTest:listKnownPartiesTest SUCCESS
          |MultiTest:multiTest SUCCESS
          |MultiTest:partyIdHintTest SUCCESS
          |ScriptExample:allocateParties SUCCESS
          |ScriptExample:initializeFixed SUCCESS
          |ScriptExample:initializeUser SUCCESS
          |ScriptExample:test SUCCESS
          |ScriptTest:clearUsers SUCCESS
          |ScriptTest:failingTest FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@XXXXXXXX{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction: ...
          |ScriptTest:listKnownPartiesTest SUCCESS
          |ScriptTest:multiPartySubmission SUCCESS
          |ScriptTest:partyIdHintTest SUCCESS
          |ScriptTest:sleepTest SUCCESS
          |ScriptTest:stackTrace FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@XXXXXXXX{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction: ...
          |ScriptTest:test0 SUCCESS
          |ScriptTest:test1 SUCCESS
          |ScriptTest:test3 SUCCESS
          |ScriptTest:test4 SUCCESS
          |ScriptTest:testCreateAndExercise SUCCESS
          |ScriptTest:testGetTime SUCCESS
          |ScriptTest:testKey SUCCESS
          |ScriptTest:testMaxInboundMessageSize SUCCESS
          |ScriptTest:testMultiPartyQueries SUCCESS
          |ScriptTest:testQueryContractId SUCCESS
          |ScriptTest:testQueryContractKey SUCCESS
          |ScriptTest:testSetTime SUCCESS
          |ScriptTest:testStack SUCCESS
          |ScriptTest:testUserListPagination SUCCESS
          |ScriptTest:testUserManagement SUCCESS
          |ScriptTest:testUserRightManagement SUCCESS
          |ScriptTest:traceOrder SUCCESS
          |ScriptTest:tree SUCCESS
          |ScriptTest:tupleKey SUCCESS
          |TestContractId:testContractId SUCCESS
          |TestExceptions:test SUCCESS
          |TestExceptions:try_catch_recover SUCCESS
          |TestExceptions:try_catch_then_abort FAILURE (com.daml.lf.engine.script.Runner$InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "expected exception" })
          |TestExceptions:try_catch_then_error FAILURE (com.daml.lf.engine.script.Runner$InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "expected exception" })
          |TestExceptions:try_catch_then_fail FAILURE (com.daml.lf.engine.script.Runner$InterpretationError: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "expected exception" })
          |TestInterfaces:test SUCCESS
          |TestInterfaces:test_queryInterface SUCCESS
          |""".stripMargin,
      )
  }
}
