// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    "--party or --all-parties" - {
      val defaultRequiredArgs = outputTypeArgs ++ outputArgs ++ sdkVersionArgs ++ ledgerArgs
      "--party Alice" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice")
        val optConfig = Config.parse(args)
        optConfig.value.partyConfig.parties should contain only ("Alice")
        optConfig.value.partyConfig.allParties shouldBe false
      }
      "--party Alice --party Bob" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice", "--party", "Bob")
        val optConfig = Config.parse(args)
        optConfig.value.partyConfig.parties should contain only ("Alice", "Bob")
        optConfig.value.partyConfig.allParties shouldBe false
      }
      "--party Alice,Bob" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice,Bob")
        val optConfig = Config.parse(args)
        optConfig.value.partyConfig.parties should contain only ("Alice", "Bob")
        optConfig.value.partyConfig.allParties shouldBe false
      }
      "--all-parties" in {
        val args = defaultRequiredArgs ++ Array("--all-parties")
        val optConfig = Config.parse(args)
        optConfig.value.partyConfig.parties shouldBe empty
        optConfig.value.partyConfig.allParties shouldBe true
      }
      "missing" in {
        val args = defaultRequiredArgs
        val optConfig = Config.parse(args)
        optConfig shouldBe empty
      }
      "--party and --all-parties" in {
        val args = defaultRequiredArgs ++ Array("--party", "Alice", "--all-parties")
        val optConfig = Config.parse(args)
        optConfig shouldBe empty
      }
    }
    "TLS" - {
      "--pem PEM --crt CRT" in {
        val pemPath = rlocation("ledger/test-common/test-certificates/client.pem")
        val crtPath = rlocation("ledger/test-common/test-certificates/client.crt")
        val args = defaultRequiredArgs ++ Array("--pem", pemPath, "--crt", crtPath)
        val optConfig = Config.parse(args)
        assert(
          Files.isSameFile(
            optConfig.value.tlsConfig.privateKeyFile.value.toPath,
            Paths.get(pemPath),
          )
        )
        assert(
          Files.isSameFile(
            optConfig.value.tlsConfig.certChainFile.value.toPath,
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
    "--max-inbound-message-size" - {
      "unset" in {
        val args = defaultRequiredArgs
        val optConfig = Config.parse(args)
        optConfig.value.maxInboundMessageSize shouldBe Config.DefaultMaxInboundMessageSize
      }
      "--max-inbound-message-size 9388608" in {
        val args = defaultRequiredArgs ++ Array("--max-inbound-message-size", "9388608")
        val optConfig = Config.parse(args)
        optConfig.value.maxInboundMessageSize shouldBe 9388608
      }
    }
    "Output type" - {
      "missing" in {
        val args = ledgerArgs ++ partyArgs
        val optConfig = Config.parse(args)
        optConfig shouldBe empty
      }
      "script" in {
        val args = outputTypeArgs ++ outputArgs ++ sdkVersionArgs ++ ledgerArgs ++ partyArgs
        val optConfig = Config.parse(args)
        optConfig.value.exportType.value shouldBe an[ExportScript]
      }
    }
  }
}
