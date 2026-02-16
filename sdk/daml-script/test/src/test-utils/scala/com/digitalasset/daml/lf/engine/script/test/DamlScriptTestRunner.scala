// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine.script.test

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonConfig.TimeProviderType
import com.daml.integrationtest.CantonFixture
import com.daml.scalautil.Statement.discard
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, Suite}

import java.nio.file.{Files, Path}
import scala.io.Source

trait DamlScriptTestRunner extends AnyWordSpec with CantonFixture with Matchers {
  self: Suite =>

  final override protected lazy val timeProviderType = TimeProviderType.Static

  import BazelRunfiles.exe
  val scriptPath = BazelRunfiles.rlocation("daml-script/runner/daml-script-binary" + exe)

  def assertDamlScriptRunnerResult(
      darPath: Path,
      expected: String,
      shouldUpload: Boolean = true,
      jsonOutput: Boolean = false,
      explicitTestNames: Option[List[String]] = None,
  ): Assertion = {
    val port = ports.head.value

    import scala.sys.process._
    val builder = new StringBuilder
    def log(s: String) = discard(builder.append(s).append('\n'))

    val mainCmd = Seq(
      scriptPath,
      "--static-time",
      "--dar",
      darPath.toString,
      "--max-inbound-message-size",
      "41943040",
      "--ledger-host",
      "localhost",
      "--ledger-port",
      port.toString,
      "--upload-dar",
      if (shouldUpload) "yes" else "no",
    )

    val testSummaryPath = Files.createTempFile("test-summary", ".json")

    val jsonArg = if (jsonOutput) Seq(s"--json-test-summary=$testSummaryPath") else Seq()
    val testNameArgs = explicitTestNames match {
      case Some(names) => names.map(name => s"--script-name=$name").toSeq
      case None => Seq("--all")
    }

    val cmd = mainCmd ++ jsonArg ++ testNameArgs

    discard(cmd ! ProcessLogger(log, log))

    val iter =
      if (jsonOutput) {
        val source = Source.fromFile(testSummaryPath.toFile)
        try source.mkString.linesIterator
        finally source.close()
      } else
        builder
          .toString()
          .linesIterator
          .filter(s => s.endsWith("SUCCESS") || s.contains("FAILURE") || s.startsWith("    in "))

    val actual = iter
      .mkString("", f"%n", f"%n")
      // Normalize escaped Windows line endings in JSON strings
      .replace("\\r\\n", "\\n")
      // rename package Ids and contractId
      .replaceAll(raw"in choice [0-9a-f]{8}:([\w_\.:]+)+", "in choice XXXXXXXX:$1")
      .replaceAll(raw"in ([\w\-]+) command [0-9a-f]{8}:([\w_\.:]+)+", "in $1 command XXXXXXXX:$2")
      .replaceAll(raw"on contract [0-9a-f]{10}", "on contract XXXXXXXXXX")
      // rename ledger errors
      .replaceAll(raw"([A-Z_]+)\((\d+),[a-f0-9]{8}\)", "$1($2,XXXXXXXX)")
      // rename exceptions
      .replaceAll(raw"([\w\.]+):([\w\.]+)@[a-f0-9]{8}", "$1:$2@XXXXXXXX")
      // rename template IDs
      .replaceAll(raw"[a-f0-9]{64}(:\w+:\w+)", "XXXXXXXX$1")
      // rename contract IDs
      .replaceAll("id [a-f0-9]{138}", "id XXXXXXXX")
      // rename parties
      .replaceAll(raw"party-[a-f0-9\-:]+", "party")
      .replaceAll(raw"(Alice|Bob|Charlie|Ivy|Mach)(-[a-z0-9]+)?::[a-z0-9]+", "party")

    cantonFixtureDebugMode match {
      case CantonFixtureDebugKeepTmpFiles | CantonFixtureDebugRemoveTmpFiles => {
        discard(
          Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".expected"), expected)
        )
        discard(Files.writeString(cantonTmpDir.resolve(getClass.getSimpleName + ".actual"), actual))
      }
      case _ => {}
    }

    Files.delete(testSummaryPath)

    actual shouldBe expected
  }
}
