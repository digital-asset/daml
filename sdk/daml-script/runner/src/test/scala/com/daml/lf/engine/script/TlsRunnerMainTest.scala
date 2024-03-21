// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.Suite
import org.scalatest.freespec.AsyncFreeSpec

final class TlsRunnerMainTest extends AsyncFreeSpec with RunnerMainTestBaseCanton {
  self: Suite =>

  override protected lazy val tlsEnable: Boolean = true

  private val tlsArgs: Seq[String] = Seq(
    "--crt",
    BazelRunfiles.rlocation("test-common/test-certificates/server.crt"),
    "--pem",
    BazelRunfiles.rlocation("test-common/test-certificates/server.pem"),
    "--cacrt",
    BazelRunfiles.rlocation("test-common/test-certificates/ca.crt"),
    "--tls",
  )

  "TLS" - {
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
          ) ++ tlsArgs,
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
          ) ++ tlsArgs,
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
          ) ++ tlsArgs,
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
          ) ++ tlsArgs,
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
          ) ++ tlsArgs,
          Right(Seq("Got 5")),
        )
      "Fails without TLS args" in
        testDamlScriptCanton(
          dars(4),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            ports.head.toString,
            "--script-name",
            "TestScript:myScript",
            "--upload-dar=yes",
          ),
          // On linux, we throw "UNAVAILABLE: Network closed for unknown reason"
          // On macOS, simply "UNAVAILABLE: io exception"
          // and on windows, ???
          // TODO: Make a consistent error for this with useful information.
          Left(Seq("UNAVAILABLE")),
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
            ) ++ tlsArgs,
            Right(Seq("Ran myScript")),
          )
        }
    }
    "JSON-API" - {
      "Succeeds with single run" in
        testDamlScriptCanton(
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
          ) ++ tlsArgs,
          Right(Seq("Ran myScript")),
        )
      "Succeeds with all run" in
        testDamlScriptCanton(
          dars(4),
          Seq(
            "--ledger-host",
            "localhost",
            "--ledger-port",
            jsonApiPort.toString,
            "--access-token-file",
            jwt.toString,
            "--json-api",
            "--all",
          ) ++ tlsArgs,
          Right(
            Seq(
              "TestScript:myOtherScript SUCCESS",
              "TestScript:myScript SUCCESS",
            )
          ),
        )
      "Fails when attempting to upload dar" in
        testDamlScriptCanton(
          dars(4),
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
          ) ++ tlsArgs,
          Left(Seq("Cannot upload dar via JSON API")),
        )
      "Succeeds using --participant-config" in
        withGrpcParticipantConfig { path =>
          testDamlScriptCanton(
            dars(4),
            Seq(
              "--participant-config",
              path.toString,
              "--access-token-file",
              jwt.toString,
              "--json-api",
              "--script-name",
              "TestScript:myScript",
            ),
            Right(Seq("Ran myScript")),
          )
        }
    }
  }
}
