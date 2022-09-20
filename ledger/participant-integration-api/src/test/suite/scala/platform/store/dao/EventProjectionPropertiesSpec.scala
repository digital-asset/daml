// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.domain.{Filters, InclusiveFilters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
import com.daml.platform.store.dao.EventProjectionProperties.RenderResult
import com.daml.platform.store.dao.EventProjectionPropertiesSpec.Scope
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventProjectionPropertiesSpec extends AnyFlatSpec with Matchers {
  behavior of "EventProjectionProperties"

  it should "propagate verbose flag" in new Scope {
    EventProjectionProperties(noFilter, true, noInterface).verbose shouldBe true
    EventProjectionProperties(noFilter, false, noInterface).verbose shouldBe false
  }

  it should "project nothing in case of empty filters" in new Scope {
    EventProjectionProperties(noFilter, true, noInterface)
      .render(Set.empty, id) shouldBe RenderResult(false, false, Set.empty)
  }

  it should "project nothing in case of irrelevant filters" in new Scope {
    EventProjectionProperties(wildcardFilter, true, interfaceImpl)
      .render(Set.empty, id) shouldBe RenderResult(false, false, Set.empty)
  }

  it should "project contract arguments in case of match by template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(party -> Filters(templateFilterFor(template1)))
    )
    EventProjectionProperties(transactionFilter, true, noInterface).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(false, true, Set.empty)
  }

  it should "project contract arguments in case of wildcard match" in new Scope {
    EventProjectionProperties(wildcardFilter, true, noInterface).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(false, true, Set.empty)
  }

  it should "project contract arguments in case of empty InclusiveFilters" in new Scope {
    EventProjectionProperties(emptyInclusiveFilters, true, noInterface).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(false, true, Set.empty)
  }

  it should "project contract arguments with wildcard and another filter" in new Scope {
    EventProjectionProperties(
      new TransactionFilter(
        Map(
          party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))),
          party2 -> Filters(Some(InclusiveFilters(Set(template1), Set.empty))),
        )
      ),
      true,
      noInterface,
    ).render(
      Set(party, party2),
      template2,
    ) shouldBe RenderResult(false, true, Set.empty)
  }

  it should "project interface in case of match by interface id and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(InterfaceFilter(iface1, includeView = true, includeCreateArgumentsBlob = false)),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))
    EventProjectionProperties(transactionFilter, true, interfaceImpl)
      .render(Set(party), template1) shouldBe RenderResult(false, false, Set(iface1))
  }

  it should "not project interface in case of match by interface id and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(InterfaceFilter(iface1, includeView = false, includeCreateArgumentsBlob = false)),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))

    EventProjectionProperties(transactionFilter, true, interfaceImpl)
      .render(Set(party), template1) shouldBe RenderResult(false, false, Set.empty)
  }

  it should "project an interface and template in case of match by interface id, template and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set(template1),
          Set(InterfaceFilter(iface1, includeView = true, includeCreateArgumentsBlob = false)),
        )
      )
    )
    val transactionFilter = new TransactionFilter(
      Map(
        party -> filter
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl)
      .render(Set(party), template1) shouldBe RenderResult(false, true, Set(iface1))
  }

  it should "project multiple interfaces in case of match by multiple interface ids and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(iface1, includeView = true, includeCreateArgumentsBlob = false),
            InterfaceFilter(iface2, includeView = true, includeCreateArgumentsBlob = false),
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))
    EventProjectionProperties(transactionFilter, true, interfaceImpl)
      .render(Set(party), template1) shouldBe RenderResult(false, false, Set(iface1, iface2))
  }

  it should "deduplicate projected interfaces and include the view" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          Some(
            InclusiveFilters(
              Set.empty,
              Set(
                InterfaceFilter(iface1, includeView = false, includeCreateArgumentsBlob = false),
                InterfaceFilter(iface2, includeView = true, includeCreateArgumentsBlob = false),
              ),
            )
          )
        ),
        party2 -> Filters(
          Some(
            InclusiveFilters(
              Set.empty,
              Set(
                InterfaceFilter(iface1, includeView = true, includeCreateArgumentsBlob = false),
                InterfaceFilter(iface2, includeView = true, includeCreateArgumentsBlob = false),
              ),
            )
          )
        ),
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl)
      .render(Set(party, party2), template1) shouldBe RenderResult(
      false,
      false,
      Set(iface2, iface1),
    )
  }

  it should "project contract arguments blob in case of match by interface" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(party -> Filters(InclusiveFilters(Set.empty, Set(InterfaceFilter(iface1, false, true)))))
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(true, false, Set.empty)
  }

  it should "project contract arguments blob in case of match by interface and template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(Set(template1), Set(InterfaceFilter(iface1, false, true)))
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(true, true, Set.empty)
  }

  it should "project contract arguments blob in case of match by interface and template with include the view" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(Set(template1), Set(InterfaceFilter(iface1, true, true)))
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(true, true, Set(iface1))
  }

  it should "project contract arguments blob in case of at least a single interface requesting it" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(iface1, false, includeCreateArgumentsBlob = true),
              InterfaceFilter(iface2, false, includeCreateArgumentsBlob = false),
            ),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl).render(
      Set(party),
      template1,
    ) shouldBe RenderResult(true, false, Set.empty)
  }

  it should "not project contract arguments blob in case of no match by interface" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(iface1, false, includeCreateArgumentsBlob = true)
            ),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl).render(
      Set(party),
      template2,
    ) shouldBe RenderResult(false, false, Set.empty)
  }
}

object EventProjectionPropertiesSpec {
  trait Scope {
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val id = Ref.Identifier.assertFromString("PackageName:ModuleName:id")
    val iface1 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface2")

    val noInterface: Identifier => Set[Ref.Identifier] = _ => Set.empty[Ref.Identifier]
    val interfaceImpl: Identifier => Set[Ref.Identifier] = {
      case `iface1` => Set(template1)
      case `iface2` => Set(template1, template2)
      case _ => Set.empty
    }
    val party = Ref.Party.assertFromString("party")
    val party2 = Ref.Party.assertFromString("party2")
    val noFilter = new TransactionFilter(Map())
    val wildcardFilter = new TransactionFilter(Map(party -> Filters(None)))
    val emptyInclusiveFilters = new TransactionFilter(
      Map(party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))))
    )
    def templateFilterFor(templateId: Ref.Identifier): Option[InclusiveFilters] = Some(
      InclusiveFilters(Set(templateId), Set.empty)
    )
  }
}
