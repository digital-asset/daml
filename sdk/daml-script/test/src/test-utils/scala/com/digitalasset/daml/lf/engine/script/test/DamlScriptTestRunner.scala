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
      "--upload-dar",
      "yes",
    )

    discard(cmd ! ProcessLogger(log, log))

    val actual = builder
      .toString()
      .linesIterator
      .filter(s => s.endsWith("SUCCESS") || s.contains("FAILURE") || s.startsWith("    in "))
      .mkString("", f"%n", f"%n")
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

    actual shouldBe expected
  }
}
