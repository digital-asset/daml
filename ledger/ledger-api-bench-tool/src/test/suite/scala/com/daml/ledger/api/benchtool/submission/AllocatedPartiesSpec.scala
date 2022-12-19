// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.client.binding.Primitive
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AllocatedPartiesSpec extends AnyFlatSpec with Matchers {

  it should "apportion parties appropriately" in {
    AllocatedParties.forExistingParties(
      parties = List(
        "signatory-123",
        "Obs-0",
        "Obs-1",
        "Div-0",
        "Sub-0",
        "FooParty-0",
        "FooParty-1",
        "BarParty-100",
        "MyOtherParty-0",
      ),
      partySetPrefixes = List("FooParty", "BarParty"),
    ) shouldBe AllocatedParties(
      signatoryO = Some(Primitive.Party("signatory-123")),
      observers = List(
        Primitive.Party("Obs-0"),
        Primitive.Party("Obs-1"),
      ),
      divulgees = List(Primitive.Party("Div-0")),
      extraSubmitters = List(Primitive.Party("Sub-0")),
      observerPartySets = List(
        AllocatedPartySet(
          partyNamePrefix = "FooParty",
          parties = List(Primitive.Party("FooParty-0"), Primitive.Party("FooParty-1")),
        ),
        AllocatedPartySet(
          partyNamePrefix = "BarParty",
          parties = List(Primitive.Party("BarParty-100")),
        ),
      ),
    )
  }

  it should "apportion parties appropriately - minimal" in {
    AllocatedParties.forExistingParties(
      parties = List(
        "signatory-123"
      ),
      partySetPrefixes = List.empty,
    ) shouldBe AllocatedParties(
      signatoryO = Some(Primitive.Party("signatory-123")),
      observers = List.empty,
      divulgees = List.empty,
      extraSubmitters = List.empty,
      observerPartySets = List.empty,
    )
  }

}
