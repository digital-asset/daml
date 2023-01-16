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
      partyPrefixesForPartySets = List("FooParty", "BarParty"),
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
          mainPartyNamePrefix = "FooParty",
          parties = List(Primitive.Party("FooParty-0"), Primitive.Party("FooParty-1")),
        ),
        AllocatedPartySet(
          mainPartyNamePrefix = "BarParty",
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
      partyPrefixesForPartySets = List.empty,
    ) shouldBe AllocatedParties(
      signatoryO = Some(Primitive.Party("signatory-123")),
      observers = List.empty,
      divulgees = List.empty,
      extraSubmitters = List.empty,
      observerPartySets = List.empty,
    )
  }

  it should "find party sets for any party prefix" in {
    AllocatedParties.forExistingParties(
      parties = List(
        "Party-01",
        "Party-02",
        "Party-10",
        "Foo-01",
        "Bar-02",
        "Baz-03",
      ),
      partyPrefixesForPartySets = List("Party-0", "Foo-", "Bar"),
    ) shouldBe AllocatedParties(
      signatoryO = None,
      observers = List.empty,
      divulgees = List.empty,
      extraSubmitters = List.empty,
      observerPartySets = List(
        AllocatedPartySet(
          "Party",
          parties = List(
            Primitive.Party("Party-01"),
            Primitive.Party("Party-02"),
            Primitive.Party("Party-10"),
          ),
        ),
        AllocatedPartySet(
          "Foo",
          parties = List(Primitive.Party("Foo-01")),
        ),
        AllocatedPartySet(
          "Bar",
          parties = List(Primitive.Party("Bar-02")),
        ),
      ),
    )
  }

}
