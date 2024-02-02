// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite

import java.nio.file.Paths

class Daml2ScriptTestRunner extends DamlScriptTestRunner {
  self: Suite =>

  val darPath = Paths.get(BazelRunfiles.rlocation("daml-script/test/script-test-v2.dar"))

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs" in
      assertDamlScriptRunnerResult(
        darPath,
        """AuthFailure:t1_CreateMissingAuthorization FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract1) requires authorizers party, but only party were given
          |AuthFailure:t2_MaintainersNotSubsetOfSignatories FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract2) has maintainers TreeSet(party) which are not a subset of the signatories TreeSet(party)
          |AuthFailure:t3_FetchMissingAuthorization FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(2) requires one of the stakeholders TreeSet(party) of the fetched contract to be an authorizer, but authorizers were TreeSet(party)
          |AuthFailure:t4_LookupByKeyMissingAuthorization FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(2) (XXXXXXXX:AuthFailure:TheContract4) requires authorizers TreeSet(party) for lookup by key, but it only has TreeSet(party)
          |AuthFailure:t5_ExerciseMissingAuthorization FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: INVALID_ARGUMENT: DAML_AUTHORIZATION_ERROR(8,XXXXXXXX): Interpretation error: Error: node NodeId(0) (XXXXXXXX:AuthFailure:TheContract5) requires authorizers party, but only party were given
          |AuthorizedDivulgence:test_authorizedFetch SUCCESS
          |AuthorizedDivulgence:test_divulgeChoiceTargetContractId SUCCESS
          |AuthorizedDivulgence:test_noDivulgenceForFetch FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |AuthorizedDivulgence:test_noDivulgenceOfCreateArguments SUCCESS
          |DiscloseViaChoiceObserver:test FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |Divulgence:main FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:divulgence FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:duplicateKey FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submitMustFail failed: null
          |ExceptionSemantics:handledArithmeticError SUCCESS
          |ExceptionSemantics:handledUserException SUCCESS
          |ExceptionSemantics:rollbackArchive SUCCESS
          |ExceptionSemantics:tryContext FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |ExceptionSemantics:uncaughtArithmeticError SUCCESS
          |ExceptionSemantics:uncaughtUserException SUCCESS
          |ExceptionSemantics:unhandledArithmeticError FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.ArithmeticError:ArithmeticError@XXXXXXXX{ message = "ArithmeticError while evaluating (DIV_INT64 1 0)." }. Details: Last location: [DA.Internal.Template.Functions:242], partial transaction: ...
          |ExceptionSemantics:unhandledUserException FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: ExceptionSemantics:E@XXXXXXXX{  }. Details: Last location: [DA.Internal.Exception:176], partial transaction: ...
          |LFContractKeys:lookupTest FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submitMustFail failed: null
          |MoreChoiceObserverDivulgence:test FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: NOT_FOUND: CONTRACT_NOT_FOUND(11,XXXXXXXX): Contract could not be found with id XXXXXXXX
          |MultiTest:disclosuresByKeyTest SUCCESS
          |MultiTest:disclosuresTest SUCCESS
          |MultiTest:inactiveDisclosureDoesNotFailDuringSubmission FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@XXXXXXXX{ message = "Here" }. Details: Last location: [GHC.Err:25], partial transaction: ...
          |MultiTest:listKnownPartiesTest SUCCESS
          |MultiTest:multiTest SUCCESS
          |MultiTest:partyIdHintTest SUCCESS
          |ScriptExample:allocateParties SUCCESS
          |ScriptExample:initializeFixed SUCCESS
          |ScriptExample:initializeUser SUCCESS
          |ScriptExample:test SUCCESS
          |ScriptTest:clearUsers SUCCESS
          |ScriptTest:failingTest FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@XXXXXXXX{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:176], partial transaction: ...
          |ScriptTest:listKnownPartiesTest SUCCESS
          |ScriptTest:multiPartySubmission SUCCESS
          |ScriptTest:partyIdHintTest SUCCESS
          |ScriptTest:sleepTest SUCCESS
          |ScriptTest:stackTrace FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@XXXXXXXX{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:176], partial transaction: ...
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
