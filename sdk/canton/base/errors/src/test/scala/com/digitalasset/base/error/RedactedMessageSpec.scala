// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import BaseError.RedactedMessage

class RedactedMessageSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with Inside {
  private val traceIdGen =
    Gen.option(Gen.asciiPrintableStr).filterNot(_.exists(v => v.isEmpty || v == "<no-tid>"))
  private val correlationIdGen = Gen
    .option(Gen.asciiPrintableStr)
    .filterNot(_.exists(v => v.isEmpty || v == "<no-correlation-id>"))

  RedactedMessage.getClass.getSimpleName should "correctly construct and extract the security message fields" in {
    forAll(correlationIdGen, traceIdGen) { (corrIdO, tIdO) =>
      inside(RedactedMessage(corrIdO, tIdO)) { case RedactedMessage(`corrIdO`, `tIdO`) =>
        succeed
      }
    }
  }
}
