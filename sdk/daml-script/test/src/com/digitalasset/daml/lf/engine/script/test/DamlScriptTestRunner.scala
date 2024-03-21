// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files

class DamlScriptTestRunner extends AnyWordSpec with CantonFixture with Matchers {
  self: Suite =>

  final override protected lazy val timeProviderType = TimeProviderType.Static

  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  val scriptPath = BazelRunfiles.rlocation("daml-script/runner/daml-script-binary" + exe)
  val darPath = BazelRunfiles.rlocation("daml-script/test/script-test.dar")

  "daml-script command line" should {
    "pick up all scripts and returns somewhat sensible outputs" in {
      val expected =
        """MultiTest:listKnownPartiesTest SUCCESS
          |MultiTest:multiTest SUCCESS
          |MultiTest:partyIdHintTest SUCCESS
          |ScriptExample:allocateParties SUCCESS
          |ScriptExample:initializeFixed SUCCESS
          |ScriptExample:initializeUser SUCCESS
          |ScriptExample:test SUCCESS
          |ScriptTest:clearUsers SUCCESS
          |ScriptTest:failingTest FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction: ...
          |ScriptTest:listKnownPartiesTest SUCCESS
          |ScriptTest:multiPartySubmission SUCCESS
          |ScriptTest:partyIdHintTest SUCCESS
          |ScriptTest:sleepTest SUCCESS
          |ScriptTest:stackTrace FAILURE (com.daml.lf.engine.script.Script$FailedCmd: Command submit failed: FAILED_PRECONDITION: UNHANDLED_EXCEPTION(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction: ...
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
          |""".stripMargin

      val port = ports.head.value

      import scala.sys.process._
      val builder = new StringBuilder
      def log(s: String) = discard(builder.append(s).append('\n'))

      val cmd = Seq(
        scriptPath,
        "--all",
        "--static-time",
        "--dar",
        darPath,
        "--max-inbound-message-size",
        "41943040",
        "--ledger-host",
        "localhost",
        "--ledger-port",
        port.toString,
      )

      cmd ! ProcessLogger(log, log)

      val actual = builder
        .toString()
        .linesIterator
        .filter(s => List("SUCCESS", "FAILURE").exists(s.contains))
        .mkString("", f"%n", f"%n")
        // ignore partial transactions as parties, cids, and package Ids are pretty unpredictable
        .replaceAll("partial transaction: .*", "partial transaction: ...")
        .replaceAll(
          """UNHANDLED_EXCEPTION\((\d+),\w{8}\)""",
          "UNHANDLED_EXCEPTION($1,XXXXXXXX)",
        )
        .replaceAll(
          """DA.Exception.GeneralError:GeneralError@\w{8}""",
          "DA.Exception.GeneralError:GeneralError@XXXXXXXX",
        )

      if (cantonFixtureDebugMode) {
        Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".expected"), expected)
        Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".actual"), actual)
      }

      actual shouldBe expected
    }
  }
}
