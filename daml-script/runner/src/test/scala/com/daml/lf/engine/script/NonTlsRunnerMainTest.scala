// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.Suite

final class NonTlsRunnerMainTest extends AsyncFreeSpec with RunnerMainTestBaseCanton {
  self: Suite =>

  "No TLS" - {
    "GRPC" - {
      "Succeeds with single run, no-upload" in
        testDamlScriptCanton(
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
          Some(false),
        )
      // Checks we upload following the legacy behaviour, and throw our warning
      "Succeeds with all run, no-upload-flag, default uploading behaviour" in
        testDamlScriptCanton(
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
          Some(true),
        )
      "Succeeds with all run, explicit no-upload" in
        testDamlScriptCanton(
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
          Some(false),
        )
      "Succeeds with single run, explicit upload" in
        testDamlScriptCanton(
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
          Some(true),
        )
      "Succeeds with single run, passing argument" in
        testDamlScriptCanton(
          dars(4),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--script-name",
            "TestScript:inputScript",
            "--input-file",
            inputFile,
          ),
          Right(Seq("Got 5")),
        )
      "Succeeds using --participant-config" in
        withGrpcParticipantConfig { path =>
          testDamlScriptCanton(
            dars(4),
            Seq(
              "--participant-config",
              path.toString,
              "--script-name",
              "TestScript:myScript",
            ),
            Right(Seq("Ran myScript")),
          )
        }
      "Fails when running a single failing script" in
        testDamlScriptCanton(
          failingDar,
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--script-name",
            "FailingTestScript:failingScript",
          ),
          Left(Seq("Failed!")),
        )
      "Fails when any script fails with --all" in
        testDamlScriptCanton(
          failingDar,
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--all",
            "--upload-dar=no",
          ),
          Left(Seq("Failed!")),
        )
    }
  }
}
