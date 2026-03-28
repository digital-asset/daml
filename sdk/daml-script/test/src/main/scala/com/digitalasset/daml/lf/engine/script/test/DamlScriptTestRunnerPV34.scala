// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class DamlScriptTestRunnerPV34 extends DamlScriptTestRunner {
  self: Suite =>

  override val legacyStateMachineMode = true

  val scriptTestDar = Paths.get(BazelRunfiles.rlocation("daml-script/test/script-test-v2.dev.dar"))

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs in PV34" in
      assertDamlScriptRunnerResult(
        scriptTestDar,
        """AuthFailure:t1_CreateMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract1) requires authorizers party, but only party were given
          |AuthFailure:t3_FetchMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(2) requires one of the stakeholders TreeSet(party) of the fetched contract to be an authorizer, but authorizers were TreeSet(party)
          |AuthFailure:t4_ExerciseMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract4) requires authorizers party, but only party were given
          |AuthorizedDivulgence:test_authorizedFetch SUCCESS
          |AuthorizedDivulgence:test_divulgeChoiceTargetContractId SUCCESS
          |AuthorizedDivulgence:test_noDivulgenceForFetch FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |AuthorizedDivulgence:test_noDivulgenceOfCreateArguments SUCCESS
          |DiscloseViaChoiceObserver:test FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |Divulgence:main FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:divulgence FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:handledArithmeticError SUCCESS
          |ExceptionSemantics:handledUserException SUCCESS
          |ExceptionSemantics:rollbackArchive SUCCESS
          |ExceptionSemantics:tryContext FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:uncaughtArithmeticError SUCCESS
          |ExceptionSemantics:uncaughtUserException SUCCESS
          |ExceptionSemantics:unhandledArithmeticError FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError (error category 9): ArithmeticError while evaluating (DIV_INT64 1 0).
          |    in choice XXXXXXXX:ExceptionSemantics:T:ThrowArithmeticError on contract XXXXXXXXXX (#1)
          |    in create-and-exercise command XXXXXXXX:ExceptionSemantics:T:ThrowArithmeticError.
          |ExceptionSemantics:unhandledUserException FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/ExceptionSemantics:E (error category 9): E
          |    in choice XXXXXXXX:ExceptionSemantics:T:Throw on contract XXXXXXXXXX (#1)
          |    in create-and-exercise command XXXXXXXX:ExceptionSemantics:T:Throw.
          |MoreChoiceObserverDivulgence:test FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |MultiTest:disclosuresTest SUCCESS
          |MultiTest:inactiveDisclosureDoesNotFailDuringSubmission FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): Here
          |    in choice XXXXXXXX:MultiTest:Helper:FailWith on contract XXXXXXXXXX (#1)
          |    in exercise command XXXXXXXX:MultiTest:Helper:FailWith on contract XXXXXXXXXX.
          |MultiTest:listKnownPartiesTest SUCCESS
          |MultiTest:multiTest SUCCESS
          |MultiTest:partyIdHintTest SUCCESS
          |ScriptExample:allocateParties SUCCESS
          |ScriptExample:initializeFixed SUCCESS
          |ScriptExample:initializeUser SUCCESS
          |ScriptExample:test SUCCESS
          |ScriptTest:clearUsers SUCCESS
          |ScriptTest:failingTest FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed (error category 9): Assertion failed
          |    in choice XXXXXXXX:ScriptTest:C:ShouldFail on contract XXXXXXXXXX (#0)
          |    in exercise command XXXXXXXX:ScriptTest:C:ShouldFail on contract XXXXXXXXXX.
          |ScriptTest:listKnownPartiesTest SUCCESS
          |ScriptTest:multiPartySubmission SUCCESS
          |ScriptTest:partyIdHintTest SUCCESS
          |ScriptTest:sleepTest SUCCESS
          |ScriptTest:stackTrace FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed (error category 9): Assertion failed
          |    in choice XXXXXXXX:ScriptTest:C:ShouldFail on contract XXXXXXXXXX (#1)
          |    in create-and-exercise command XXXXXXXX:ScriptTest:C:ShouldFail.
          |ScriptTest:test0 SUCCESS
          |ScriptTest:test1 SUCCESS
          |ScriptTest:test3 SUCCESS
          |ScriptTest:test4 SUCCESS
          |ScriptTest:testCreateAndExercise SUCCESS
          |ScriptTest:testGetTime SUCCESS
          |ScriptTest:testMaxInboundMessageSize SUCCESS
          |ScriptTest:testMultiPartyQueries SUCCESS
          |ScriptTest:testQueryContractId SUCCESS
          |ScriptTest:testSetTime SUCCESS
          |ScriptTest:testStack SUCCESS
          |ScriptTest:testUserListPagination SUCCESS
          |ScriptTest:testUserManagement SUCCESS
          |ScriptTest:testUserRightManagement SUCCESS
          |ScriptTest:traceOrder SUCCESS
          |ScriptTest:tree SUCCESS
          |TestContractId:testContractId SUCCESS
          |TestExceptions:test SUCCESS
          |TestExceptions:try_catch_recover SUCCESS
          |TestExceptions:try_catch_then_abort FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): expected exception)
          |TestExceptions:try_catch_then_error FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): expected exception)
          |TestExceptions:try_catch_then_fail FAILURE (com.digitalasset.daml.lf.engine.free.InterpretationError: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): expected exception)
          |TestFailWithStatus:attemptToOverwriteMetadata SUCCESS
          |TestFailWithStatus:cannotCatch SUCCESS
          |TestFailWithStatus:haltsExecution SUCCESS
          |TestFailWithStatus:pureFailEagerness SUCCESS
          |TestFailWithStatus:roundtrip SUCCESS
          |TestInterfaces:test SUCCESS
          |TestInterfaces:test_queryInterface SUCCESS
          |""".stripMargin,
        // Contract keys are not supported in PV34
        skipTestNames = List(
          "AuthFailureWithKey:t1_LookupByKeyMissingAuthorization",
          "AuthFailureWithKey:t2_MaintainersNotSubsetOfSignatories",
          "LFContractKeys:lookupTest",
          "MultiTest:disclosuresByKeyTest",
          "ScriptTest:testKey",
          "ScriptTest:testQueryContractKey",
          "ScriptTest:tupleKey",
        ),
      )
  }
}
