// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.BaseError.SecuritySensitiveMessage
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SecuritySensitiveMessageSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with Inside {
  private val traceIdGen =
    Gen.option(Gen.asciiPrintableStr).filterNot(_.exists(v => v.isEmpty || v == "<no-tid>"))
  private val correlationIdGen = Gen
    .option(Gen.asciiPrintableStr)
    .filterNot(_.exists(v => v.isEmpty || v == "<no-correlation-id>"))

  SecuritySensitiveMessage.getClass.getSimpleName should "correctly construct and extract the security message fields" in {
    forAll(correlationIdGen, traceIdGen) { (corrIdO, tIdO) =>
      inside(SecuritySensitiveMessage(corrIdO, tIdO)) {
        case SecuritySensitiveMessage(`corrIdO`, `tIdO`) => succeed
      }
    }
  }
}
