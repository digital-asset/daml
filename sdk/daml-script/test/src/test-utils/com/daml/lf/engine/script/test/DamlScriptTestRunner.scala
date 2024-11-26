// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.platform.services.time.TimeProviderType
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
      .filter(s => s.endsWith("SUCCESS") || s.contains("FAILURE") || s.startsWith("    in "))
      .mkString("", f"%n", f"%n")
      // ignore partial transactions as parties, cids, and package Ids are pretty unpredictable
      .replaceAll(
        raw"in choice [0-9a-f]{8}:([\w_\.:]+)+ on contract 00[0-9a-f]{8}",
        "in choice XXXXXXXX:$1 on contract 00XXXXXXXX",
      )
      .replaceAll(
        raw"in ([\w\-]+) command [0-9a-f]{8}:([\w_\.:]+)+ on contract 00[0-9a-f]{8}",
        "in $1 command XXXXXXXX:$2 on contract 00XXXXXXXX",
      )
      .replaceAll(
        raw"UNHANDLED_EXCEPTION\((\d+),\w{8}\)",
        "UNHANDLED_EXCEPTION($1,XXXXXXXX)",
      )
      .replaceAll(
        raw"DA.Exception.(\w+):(\w+)@\w{8}",
        "DA.Exception.$1:$2@XXXXXXXX",
      )

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
