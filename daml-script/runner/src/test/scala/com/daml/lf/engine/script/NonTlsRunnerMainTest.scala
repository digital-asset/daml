// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import org.scalatest.Suite
import org.scalatest.freespec.AsyncFreeSpec

final class NonTlsRunnerMainTest extends AsyncFreeSpec with RunnerMainTestBase {
  self: Suite =>

  "No TLS" - {
    "GRPC" - {
      "Succeeds with single run, no-upload" in
        testDamlScript(
          dars(0),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--script-name",
            "TestScript:myScript",
          ),
          Right(Seq("Ran myScript")),
        )
      // Checks we upload following the legacy behaviour, and throw our warning
      "Succeeds with all run, no-upload-flag, default uploading behaviour" in
        testDamlScript(
          dars(1),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--all",
          ),
          Right(
            Seq(
              "WARNING: Implicitly using the legacy behaviour",
              "TestScript:myOtherScript SUCCESS",
              "TestScript:myScript SUCCESS",
            )
          ),
          true,
        )
      "Succeeds with all run, explicit no-upload" in
        testDamlScript(
          dars(2),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--all",
            "--upload-dar=no",
          ),
          Right(
            Seq(
              "TestScript:myOtherScript SUCCESS",
              "TestScript:myScript SUCCESS",
            )
          ),
          false,
        )
      "Succeeds with single run, explicit upload" in
        testDamlScript(
          dars(3),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--script-name",
            "TestScript:myScript",
            "--upload-dar=yes",
          ),
          Right(Seq("Ran myScript")),
          true,
        )
    }
    "JSON-API" - {
      "Succeeds with single run" in
        testDamlScript(
          dars(4),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            jsonApiPort.toString,
            "--access-token-file",
            jwt.toString,
            "--json-api",
            "--script-name",
            "TestScript:myScript",
          ),
          Right(Seq("Ran myScript")),
        )
      "Succeeds with all run" in
        testDamlScript(
          dars(5),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            jsonApiPort.toString,
            "--access-token-file",
            jwt.toString,
            "--json-api",
            "--all",
          ),
          Right(
            Seq(
              "TestScript:myOtherScript SUCCESS",
              "TestScript:myScript SUCCESS",
            )
          ),
        )
      "Fails when attempting to upload dar" in
        testDamlScript(
          dars(6),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            jsonApiPort.toString,
            "--access-token-file",
            jwt.toString,
            "--json-api",
            "--upload-dar=yes",
            "--script-name",
            "TestScript:myScript",
          ),
          Left(Seq("Cannot upload dar via JSON API")),
        )
    }
  }
}
