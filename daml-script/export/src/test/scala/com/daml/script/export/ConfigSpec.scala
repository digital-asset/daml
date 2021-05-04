// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

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
  }
}
