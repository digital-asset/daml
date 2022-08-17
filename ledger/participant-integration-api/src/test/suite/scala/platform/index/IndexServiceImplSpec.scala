// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.domain.{Filters, InclusiveFilters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.daml.platform.index.IndexServiceImpl.templateFilter
import com.daml.platform.index.IndexServiceImplSpec.Scope
import com.daml.platform.packagemeta.PackageMetadata
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexServiceImplSpec extends AnyFlatSpec with Matchers {

  behavior of "IndexServiceImpl.templateFilter"

  it should "give empty result for the empty input" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map.empty),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for wildcard filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(None))),
    ) shouldBe Map(party -> Set.empty)
  }

  it should "support multiple wildcard filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(None),
        )
      ),
    ) shouldBe Map(
      party -> Set.empty,
      party2 -> Set.empty,
    )

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(
            Some(InclusiveFilters(templateIds = Set(template1), interfaceFilters = Set()))
          ),
        )
      ),
    ) shouldBe Map(
      party -> Set.empty,
      party2 -> Set(template1),
    )
  }

  it should "be treated as wildcard filter if templateIds and interfaceIds are empty" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(), Set())))),
    ) shouldBe Map(party -> Set())
  }

  it should "provide a template filter for a specific template filter" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(template1), Set())))),
    ) shouldBe Map(party -> Set(template1))
  }

  it should "provide an empty template filter if no template implementing this interface" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(InterfaceFilter(iface1, true)))))
      ),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for related interface filter" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))),
      TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(InterfaceFilter(iface1, true)))))
      ),
    ) shouldBe Map(party -> Set(template1))
  }

  it should "merge template filter and interface filter together" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template2))),
      TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(template1), Set(InterfaceFilter(iface1, true)))))
      ),
    ) shouldBe Map(party -> Set(template1, template2))
  }

  it should "merge multiple interface filters into union of templates" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy =
        Map(iface1 -> Set(template1), iface2 -> Set(template2))
      ),
      TransactionFilter(
        Map(
          party -> Filters(
            InclusiveFilters(
              templateIds = Set(template3),
              interfaceFilters = Set(
                InterfaceFilter(iface1, true),
                InterfaceFilter(iface2, true),
              ),
            )
          )
        )
      ),
    ) shouldBe Map(party -> Set(template1, template2, template3))
  }

}

object IndexServiceImplSpec {
  trait Scope {
    val party = Ref.Party.assertFromString("party")
    val party2 = Ref.Party.assertFromString("party2")
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val template3 = Ref.Identifier.assertFromString("PackageName:ModuleName:template3")
    val iface1 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface2")
  }
}
