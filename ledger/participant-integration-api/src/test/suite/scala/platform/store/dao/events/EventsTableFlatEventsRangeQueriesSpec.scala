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
    filterParams(Map(), Set.empty) shouldBe FilterParams(
      wildCardParties = Set.empty,
      partiesAndTemplates = Set.empty,
    )
  }

  it should "propagate wildcard parties" in new Scope {
    filterParams(Map(), Set(party)) shouldBe FilterParams(
      wildCardParties = Set(party),
      partiesAndTemplates = Set.empty,
    )

    filterParams(Map(), Set(party2, party3)) shouldBe FilterParams(
      wildCardParties = Set(party2, party3),
      partiesAndTemplates = Set.empty,
    )
  }

  it should "convert simple filter" in new Scope {
    filterParams(Map(template1 -> Set(party)), Set.empty) shouldBe FilterParams(
      Set.empty,
      Set((Set(party), Set(template1))),
    )

    filterParams(Map(template1 -> Set(party, party2)), Set.empty) shouldBe FilterParams(
      Set.empty,
      Set((Set(party, party2), Set(template1))),
    )
  }

  it should "convert sophisticated filter with multiple parties and templates" in new Scope {
    filterParams(
      Map(
        template1 -> Set(party, party2),
        template2 -> Set.empty,
        template3 -> Set(party2, party3),
      ),
      Set.empty,
    ) shouldBe FilterParams(
      Set.empty,
      Set(
        (Set.empty, Set(template2)),
        (Set(party, party2), Set(template1)),
        (Set(party2, party3), Set(template3)),
      ),
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
