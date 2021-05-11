// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}

class ConfigSpec extends AnyFreeSpec with Matchers with OptionValues {
  private val outputTypeArgs = Array("script")
  private val outputArgs = Array("--output", "/tmp/out")
  private val sdkVersionArgs = Array("--sdk-version", "0.0.0")
  private val ledgerArgs = Array("--host", "localhost", "--port", "6865")
  private val partyArgs = Array("--party", "Alice")
  private val defaultRequiredArgs =
    outputTypeArgs ++ outputArgs ++ sdkVersionArgs ++ ledgerArgs ++ partyArgs
  "command-line options" - {
    "--start" - {
      "--start begin" in {
        val args = defaultRequiredArgs ++ Array("--start", "begin")
        val optConfig = Config.parse(args)
        optConfig.value.start shouldBe LedgerOffset().withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_BEGIN
        )
      }
      "--start end" in {
        val args = defaultRequiredArgs ++ Array("--start", "end")
        val optConfig = Config.parse(args)
        optConfig.value.start shouldBe LedgerOffset().withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_END
        )
      }
      "--start offset" in {
        val args = defaultRequiredArgs ++ Array("--start", "00100")
        val optConfig = Config.parse(args)
        optConfig.value.start shouldBe LedgerOffset().withAbsolute("00100")
      }
    }
    "--end" - {
      "--end begin" in {
        val args = defaultRequiredArgs ++ Array("--end", "begin")
        val optConfig = Config.parse(args)
        optConfig.value.end shouldBe LedgerOffset().withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_BEGIN
        )
      }
      "--end end" in {
        val args = defaultRequiredArgs ++ Array("--end", "end")
        val optConfig = Config.parse(args)
        optConfig.value.end shouldBe LedgerOffset().withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_END
        )
      }
      "--end offset" in {
        val args = defaultRequiredArgs ++ Array("--end", "00100")
        val optConfig = Config.parse(args)
        optConfig.value.end shouldBe LedgerOffset().withAbsolute("00100")
      }
    }
    "--party" - {
      val defaultRequiredArgs = outputTypeArgs ++ outputArgs ++ sdkVersionArgs ++ ledgerArgs
      "--party Alice" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice")
        val optConfig = Config.parse(args)
        optConfig.value.parties should contain only ("Alice")
      }
      "--party Alice --party Bob" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice", "--party", "Bob")
        val optConfig = Config.parse(args)
        optConfig.value.parties should contain only ("Alice", "Bob")
      }
      "--party Alice,Bob" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice,Bob")
        val optConfig = Config.parse(args)
        optConfig.value.parties should contain only ("Alice", "Bob")
      }
    }
    "TLS" - {
      "--pem PEM --crt CRT" in {
        val pemPath = rlocation("ledger/test-common/test-certificates/client.pem")
        val crtPath = rlocation("ledger/test-common/test-certificates/client.crt")
        val args = defaultRequiredArgs ++ Array("--pem", pemPath, "--crt", crtPath)
        val optConfig = Config.parse(args)
        assert(Files.isSameFile(optConfig.value.tlsConfig.keyFile.value.toPath, Paths.get(pemPath)))
        assert(
          Files.isSameFile(
            optConfig.value.tlsConfig.keyCertChainFile.value.toPath,
            Paths.get(crtPath),
          )
        )
      }
    }
    "Auth" - {
      val token = "test-token"
      def withTokenFile(f: Path => Assertion): Assertion = {
        val tokenFile: Path = Files.createTempFile("token", ".jwt")
        try {
          val writer = new PrintWriter(tokenFile.toFile)
          try {
            writer.print(token)
          } finally {
            writer.close
          }
          f(tokenFile)
        } finally {
          Files.delete(tokenFile)
        }
      }
      "--access-token-file" in withTokenFile { tokenFile =>
        val args = defaultRequiredArgs ++ Array("--access-token-file", tokenFile.toString)
        val optConfig = Config.parse(args)
        optConfig.value.accessToken.value.token.value shouldBe token
      }
    }
  }
}
