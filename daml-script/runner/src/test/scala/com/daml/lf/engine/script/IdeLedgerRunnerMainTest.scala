// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

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
  }
}
