// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NamesSpec extends AnyFlatSpec with Matchers with OptionValues {

  it should "make left pad party set party names" in {
    val tested = new Names
    tested.partySetPartyName(prefix = "Party", numberOfParties = 2, uniqueParties = false) shouldBe
      Seq(
        "Party-0",
        "Party-1",
      )
    tested.partySetPartyName(prefix = "Party", numberOfParties = 12, uniqueParties = false) shouldBe
      Seq(
        "Party-00",
        "Party-01",
        "Party-02",
        "Party-03",
        "Party-04",
        "Party-05",
        "Party-06",
        "Party-07",
        "Party-08",
        "Party-09",
        "Party-10",
        "Party-11",
      )
    val thousandParties =
      tested.partySetPartyName(prefix = "Party", numberOfParties = 1000, uniqueParties = false)
    thousandParties.headOption.value shouldBe "Party-000"
    thousandParties.lastOption.value shouldBe "Party-999"
    thousandParties should have length 1000
  }

}
