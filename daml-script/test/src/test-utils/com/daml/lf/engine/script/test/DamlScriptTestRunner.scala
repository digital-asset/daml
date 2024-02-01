// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonConfig.TimeProviderType
import com.daml.integrationtest.CantonFixture
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
      // rename ledger errors
      .replaceAll("""([A-Z_]+)\((\d+),[a-f0-9]{8}\)""", "$1($2,XXXXXXXX)")
      // rename exceptions
      .replaceAll("""([\w\.]+):([\w\.]+)@[a-f0-9]{8}""", "$1:$2@XXXXXXXX")
      // rename template IDs
      .replaceAll("""[a-f0-9]{64}(:\w+:\w+)""", "XXXXXXXX$1")
      // rename contract IDs
      .replaceAll("id [a-f0-9]{138}", "id XXXXXXXX")
      // rename parties
      .replaceAll("""party-[a-f0-9\-:]+""", "party")

    if (cantonFixtureDebugMode) {
      discard(
        Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".expected"), expected)
      )
      discard(Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".actual"), actual))
    }

    actual shouldBe expected
  }
}
