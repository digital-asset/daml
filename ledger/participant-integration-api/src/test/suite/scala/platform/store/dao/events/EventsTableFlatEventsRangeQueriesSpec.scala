// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.dao.events.EventsTableFlatEventsRangeQueries.filterParams
import com.daml.platform.store.dao.events.EventsTableFlatEventsRangeQueriesSpec.Scope
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventsTableFlatEventsRangeQueriesSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks {

  behavior of EventsTableFlatEventsRangeQueries.getClass.getSimpleName

  it should "give empty filter for empty input" in new Scope {
    filterParams(Map()) shouldBe FilterParams(
      wildCardParties = Set.empty,
      partiesAndTemplates = Set.empty,
    )
  }

  it should "translate to wildcard" in new Scope {
    filterParams(Map(party -> Set.empty)) shouldBe FilterParams(
      wildCardParties = Set(party),
      partiesAndTemplates = Set.empty,
    )
  }

  it should "translate to parties and templates" in new Scope {
    filterParams(Map(party -> Set(template1), party2 -> Set(template2))) shouldBe FilterParams(
      wildCardParties = Set.empty,
      partiesAndTemplates = Set((Set(party), Set(template1)), (Set(party2), Set(template2))),
    )
  }

  it should "support translation of wildcard parties and non-wildcard at the same time" in new Scope {
    filterParams(
      Map(party -> Set(template1), party2 -> Set(template2), party3 -> Set.empty)
    ) shouldBe FilterParams(
      wildCardParties = Set(party3),
      partiesAndTemplates = Set((Set(party), Set(template1)), (Set(party2), Set(template2))),
    )
  }

  it should "optimize if all parties request the same templates" in new Scope {
    filterParams(
      Map(party -> Set(template1), party2 -> Set(template1))
    ) shouldBe FilterParams(
      wildCardParties = Set.empty,
      partiesAndTemplates = Set((Set(party, party2), Set(template1))),
    )
  }

}

object EventsTableFlatEventsRangeQueriesSpec {
  trait Scope {
    val party = Ref.Party.assertFromString("party")
    val party2 = Ref.Party.assertFromString("party2")
    val party3 = Ref.Party.assertFromString("party3")
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val template3 = Ref.Identifier.assertFromString("PackageName:ModuleName:template3")
  }
}
