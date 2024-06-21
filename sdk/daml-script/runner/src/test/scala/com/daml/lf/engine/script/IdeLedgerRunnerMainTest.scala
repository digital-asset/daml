// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import org.scalatest.Suite
import org.scalatest.freespec.AsyncFreeSpec

final class IdeLedgerRunnerMainTest extends AsyncFreeSpec with RunnerMainTestBase {
  self: Suite =>

  "IDE-Ledger" - {
    "Succeeds with single run" in
      testDamlScript(
        dars(0),
        Seq(
          "--ide-ledger",
          "--script-name",
          "TestScript:myScript",
        ),
        Right(Seq("Ran myScript")),
      )
    "Succeeds with all run" in
      testDamlScript(
        dars(0),
        Seq(
          "--ide-ledger",
          "--all",
        ),
        Right(
          Seq(
            "TestScript:myOtherScript SUCCESS",
            "TestScript:myScript SUCCESS",
          )
        ),
      )
    "Fails trying to upload dar" in
      testDamlScript(
        dars(0),
        Seq(
          "--ide-ledger",
          "--script-name",
          "TestScript:myScript",
          "--upload-dar=yes",
        ),
        Left(Seq("Cannot upload dar to IDELedger")),
      )
    "Do not warn users to add --upload-dar when using --all" in
      testDamlScriptPred(
        dars(0),
        Seq(
          "--ide-ledger",
          "--all",
        ),
      ) { res =>
        res match {
          case Left(actual) =>
            fail(s"Expected daml-script to succeed but it failed with $actual")
          case Right(actual) =>
            if (actual contains "Please use the explicit `--upload-dar yes` option") {
              fail("Did not expect `--upload-dar` warning")
            } else {
              succeed
            }
        }
      }
    "Fails trying to upload dar with --all" in
      testDamlScript(
        dars(0),
        Seq(
          "--ide-ledger",
          "--all",
          "--upload-dar=yes",
        ),
        Left(Seq("Cannot upload dar to IDELedger")),
      )
  }
}
