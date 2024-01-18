// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import org.scalatest.{Assertion, Suite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files
import java.nio.file.Path

trait DamlScriptTestRunner extends AnyWordSpec with CantonFixture with Matchers {
  self: Suite =>

  final override protected lazy val timeProviderType = TimeProviderType.Static

  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  val scriptPath = BazelRunfiles.rlocation("daml-script/runner/daml-script-binary" + exe)

  def assertDamlScriptRunnerResult(darPath: Path, expected: String): Assertion = {
    val port = ports.head.value

    import scala.sys.process._
    val builder = new StringBuilder
    def log(s: String) = discard(builder.append(s).append('\n'))

    val cmd = Seq(
      scriptPath,
      "--all",
      "--static-time",
      "--dar",
      darPath.toString,
      "--max-inbound-message-size",
      "41943040",
      "--ledger-host",
      "localhost",
      "--ledger-port",
      port.toString,
    )

    discard(cmd ! ProcessLogger(log, log))

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
        """DA.Exception.(\w+):(\w+)@\w{8}""",
        "DA.Exception.$1:$2@XXXXXXXX",
      )

    if (cantonFixtureDebugMode) {
      discard(
        Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".expected"), expected)
      )
      discard(Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".actual"), actual))
    }

    actual shouldBe expected
  }
}
