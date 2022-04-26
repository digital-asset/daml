// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommandGeneratorSpec extends AnyFlatSpec with Matchers {

  it should "generate random payload of a given size" in {
    CommandGenerator.randomPayload(RandomnessProvider.Default, 10).length shouldBe 10
  }

}
