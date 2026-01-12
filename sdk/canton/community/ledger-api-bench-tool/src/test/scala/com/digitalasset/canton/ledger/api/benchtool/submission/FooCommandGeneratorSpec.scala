// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.digitalasset.canton.ledger.api.benchtool.submission.{
  FooCommandGenerator,
  RandomnessProvider,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class FooCommandGeneratorSpec extends AnyFlatSpec with Matchers {

  it should "generate random payload of a given size" in {
    FooCommandGenerator
      .randomPayload(RandomnessProvider.Default, sizeBytes = 100)
      .getBytes(StandardCharsets.UTF_8)
      .length shouldBe 100
  }

}
