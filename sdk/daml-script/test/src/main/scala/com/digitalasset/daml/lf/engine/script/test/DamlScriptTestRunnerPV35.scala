// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonConfig.ProtocolVersion
import org.scalatest.Suite

import java.nio.file.Paths

// TODO (canton#31925) Change this to DamlScriptTestRunnerPVLatest, and update/remove protocolVersion to reflect that
// once PV35 is the default/out of alpha

class DamlScriptTestRunnerPV35 extends DamlScriptTestRunner {
  self: Suite =>

  override lazy val protocolVersion = ProtocolVersion.Explicit("v35")

  val scriptTestDar =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/script-test-v2.3-staging.dar"))
  val fakeScriptTestDar =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/legacy-script-test.dar"))
  val jsonScriptTestDar =
    Paths.get(BazelRunfiles.rlocation("daml-script/test/json-script-test.dar"))

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs" in
      assertDamlScriptRunnerResult(
        scriptTestDar,
        """AuthFailure:t1_CreateMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract1) requires authorizers party, but only party were given
          |AuthFailure:t3_FetchMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(2) requires one of the stakeholders TreeSet(party) of the fetched contract to be an authorizer, but authorizers were TreeSet(party)
          |AuthFailure:t4_ExerciseMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract4) requires authorizers party, but only party were given
          |AuthFailureWithKey:t1_LookupByKeyMissingAuthorization FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(2) (XXXXXXXX:AuthFailureWithKey:TheContract1) requires authorizers party for lookup by key, but it only has party
          |AuthFailureWithKey:t2_MaintainersNotSubsetOfSignatories FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailureWithKey:TheContract2) has maintainers party which are not a subset of the signatories TreeSet(party)
          |AuthorizedDivulgence:test_authorizedFetch SUCCESS
          |AuthorizedDivulgence:test_divulgeChoiceTargetContractId SUCCESS
          |AuthorizedDivulgence:test_noDivulgenceForFetch FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |AuthorizedDivulgence:test_noDivulgenceOfCreateArguments SUCCESS
          |DiscloseViaChoiceObserver:test FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |Divulgence:main FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |LFContractKeys:lookupTest FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: SErrorDamlException(UserError(Expected submit to fail but it succeeded)
          |MoreChoiceObserverDivulgence:test FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |MultiTestWithKeys:disclosuresByKeyTest SUCCESS
          |MultiTestWithKeys:disclosuresTest SUCCESS
          |MultiTestWithKeys:inactiveDisclosureDoesNotFailDuringSubmission FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.GeneralError:GeneralError (error category 9): Here
          |    in choice XXXXXXXX:MultiTestWithKeys:Helper:FailWith on contract XXXXXXXXXX (#1)
          |    in exercise command XXXXXXXX:MultiTestWithKeys:Helper:FailWith on contract XXXXXXXXXX.
          |MultiTestWithKeys:listKnownPartiesTest SUCCESS
          |MultiTestWithKeys:multiTest SUCCESS
          |MultiTestWithKeys:partyIdHintTest SUCCESS
          |ScriptExample:allocateParties SUCCESS
          |ScriptExample:initializeFixed SUCCESS
          |ScriptExample:initializeUser SUCCESS
          |ScriptExample:test SUCCESS
          |ScriptTestWithKeys:clearUsers SUCCESS
          |ScriptTestWithKeys:failingTest FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed (error category 9): Assertion failed
          |    in choice XXXXXXXX:ScriptTestWithKeys:C:ShouldFail on contract XXXXXXXXXX (#0)
          |    in exercise command XXXXXXXX:ScriptTestWithKeys:C:ShouldFail on contract XXXXXXXXXX.
          |ScriptTestWithKeys:listKnownPartiesTest SUCCESS
          |ScriptTestWithKeys:multiPartySubmission SUCCESS
          |ScriptTestWithKeys:partyIdHintTest SUCCESS
          |ScriptTestWithKeys:sleepTest SUCCESS
          |ScriptTestWithKeys:stackTrace FAILURE (com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command Submit failed: FAILED_PRECONDITION: DAML_FAILURE(9,XXXXXXXX): Interpretation error: Error: User failure: UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed (error category 9): Assertion failed
          |    in choice XXXXXXXX:ScriptTestWithKeys:C:ShouldFail on contract XXXXXXXXXX (#1)
          |    in create-and-exercise command XXXXXXXX:ScriptTestWithKeys:C:ShouldFail.
          |ScriptTestWithKeys:test0 SUCCESS
          |ScriptTestWithKeys:test1 SUCCESS
          |ScriptTestWithKeys:test3 SUCCESS
          |ScriptTestWithKeys:test4 SUCCESS
          |ScriptTestWithKeys:testCreateAndExercise SUCCESS
          |ScriptTestWithKeys:testGetTime SUCCESS
          |ScriptTestWithKeys:testKey SUCCESS
          |ScriptTestWithKeys:testMaxInboundMessageSize SUCCESS
          |ScriptTestWithKeys:testMultiPartyQueries SUCCESS
          |ScriptTestWithKeys:testQueryByKey SUCCESS
          |ScriptTestWithKeys:testQueryContractId SUCCESS
          |ScriptTestWithKeys:testSetTime SUCCESS
          |ScriptTestWithKeys:testStack SUCCESS
          |ScriptTestWithKeys:testUserListPagination SUCCESS
          |ScriptTestWithKeys:testUserManagement SUCCESS
          |ScriptTestWithKeys:testUserRightManagement SUCCESS
          |ScriptTestWithKeys:traceOrder SUCCESS
          |ScriptTestWithKeys:tree SUCCESS
          |ScriptTestWithKeys:tupleKey SUCCESS
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
        // PV35 does not support rollbacks, disable tests that use it
        skipTestNames = List(
          "ExceptionSemantics:divulgence",
          "ExceptionSemantics:handledArithmeticError",
          "ExceptionSemantics:handledUserException",
          "ExceptionSemantics:rollbackArchive",
          "ExceptionSemantics:tryContext",
          "ExceptionSemantics:uncaughtArithmeticError",
          "ExceptionSemantics:uncaughtUserException",
          "ExceptionSemantics:unhandledArithmeticError",
          "ExceptionSemantics:unhandledUserException",
          "ExceptionSemanticsWithKeys:duplicateKey",
        ),
      )
    "Reject legacy daml scripts correctly" in
      assertDamlScriptRunnerResult(
        fakeScriptTestDar,
        """FakeDamlScriptTest:myScript FAILURE (com.digitalasset.daml.lf.script.converter.ConverterException: Legacy daml-script is not supported in daml 3.3, please recompile your script using a daml 3.3+ SDK)
          |""".stripMargin,
        false,
      )
    "Json output correctly" in
      assertDamlScriptRunnerResult(
        jsonScriptTestDar,
        """{
          |  "JsonDamlScriptTest:failingTestData": {
          |    "error": "com.digitalasset.daml.lf.engine.script.Script$FailedCmd: Command AllocateParty failed: INVALID_ARGUMENT: INVALID_ARGUMENT(8,XXXXXXXX): The submitted request has invalid arguments: Party already exists: party party... is already allocated on this node\nDaml stacktrace:\nallocatePartyByHint at XXXXXXXX:JsonDamlScriptTest:18"
          |  },
          |  "JsonDamlScriptTest:succeedingTestData": {
          |    "result": {
          |      "i": 10
          |    }
          |  },
          |  "JsonDamlScriptTest:succeedingTestUnit": {
          |    "result": {
          |
          |    }
          |  }
          |}
          |""".stripMargin,
        shouldUpload = false,
        jsonOutput = true,
        // Explicitly not running JsonDamlScriptTestWithKeys:testWeDontRun and asserting it doesnt show in output
        explicitTestNames = Some(
          List(
            "JsonDamlScriptTest:succeedingTestUnit",
            "JsonDamlScriptTest:succeedingTestData",
            "JsonDamlScriptTest:failingTestData",
          )
        ),
      )
  }
}
