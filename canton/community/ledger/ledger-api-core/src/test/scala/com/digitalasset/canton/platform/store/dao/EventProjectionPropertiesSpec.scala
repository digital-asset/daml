// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.canton.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.dao.EventProjectionPropertiesSpec.Scope
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventProjectionPropertiesSpec extends AnyFlatSpec with Matchers {
  behavior of "EventProjectionProperties"

  it should "propagate verbose flag" in new Scope {
    EventProjectionProperties(noFilter, true, noInterface, false).verbose shouldBe true
    EventProjectionProperties(noFilter, false, noInterface, false).verbose shouldBe false
  }

  it should "project nothing in case of empty filters" in new Scope {
    EventProjectionProperties(noFilter, true, noInterface, false)
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  it should "project nothing in case of irrelevant filters" in new Scope {
    EventProjectionProperties(wildcardFilter, true, interfaceImpl, false)
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  behavior of "projecting contract arguments"

  it should "project contract arguments in case of match by template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(party -> Filters(templateFilterFor(template1)))
    )
    EventProjectionProperties(transactionFilter, true, noInterface, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project contract arguments in case of wildcard match" in new Scope {
    EventProjectionProperties(wildcardFilter, true, noInterface, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project contract arguments in case of empty InclusiveFilters" in new Scope {
    EventProjectionProperties(emptyInclusiveFilters, true, noInterface, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project contract arguments with wildcard and another filter" in new Scope {
    EventProjectionProperties(
      new TransactionFilter(
        Map(
          party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))),
          party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
        )
      ),
      true,
      noInterface,
      false,
    ).render(
      Set(party, party2),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "do not project contract arguments with wildcard and another filter, if queried non wildcard party/template combination" in new Scope {
    EventProjectionProperties(
      new TransactionFilter(
        Map(
          party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))),
          party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
        )
      ),
      true,
      noInterface,
      false,
    ).render(
      Set(party2),
      template2,
    ) shouldBe Projection(Set.empty, false, false)
  }

  it should "project contract arguments with wildcard and another filter with alwaysPopulateArguments, if queried non wildcard party/template combination" in new Scope {
    EventProjectionProperties(
      new TransactionFilter(
        Map(
          party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))),
          party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
        )
      ),
      true,
      noInterface,
      true,
    ).render(
      Set(party2),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }

  behavior of "projecting interfaces"

  it should "project interface in case of match by interface id and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              iface1,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false)
      .render(Set(party), template1) shouldBe Projection(Set(iface1), false, false)
  }

  it should "project interface in case of match by interface id and witness with alwaysPopulateArguments" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              iface1,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true)
      .render(Set(party), template1) shouldBe Projection(Set(iface1), false, true)
  }

  it should "not project interface in case of match by interface id and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              iface1,
              includeView = false,
              includeCreatedEventBlob = false,
            )
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))

    EventProjectionProperties(transactionFilter, true, interfaceImpl, false)
      .render(Set(party), template1) shouldBe Projection(Set.empty, false, false)
  }

  it should "project an interface and template in case of match by interface id, template and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set(template1Filter),
          Set(
            InterfaceFilter(
              iface1,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(
      Map(
        party -> filter
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false)
      .render(Set(party), template1) shouldBe Projection(Set(iface1), false, true)
  }

  it should "project an interface and template in case of match by interface id, template and witness with alwaysPopulateArguments" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set(template1Filter),
          Set(
            InterfaceFilter(
              iface1,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(
      Map(
        party -> filter
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true)
      .render(Set(party), template1) shouldBe Projection(Set(iface1), false, true)
  }

  it should "project multiple interfaces in case of match by multiple interface ids and witness" in new Scope {
    val filter = Filters(
      Some(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              iface1,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
            InterfaceFilter(
              iface2,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
          ),
        )
      )
    )
    val transactionFilter = new TransactionFilter(Map(party -> filter))
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false)
      .render(Set(party), template1) shouldBe Projection(Set(iface1, iface2), false, false)
  }

  it should "deduplicate projected interfaces and include the view" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          Some(
            InclusiveFilters(
              Set.empty,
              Set(
                InterfaceFilter(
                  iface1,
                  includeView = false,
                  includeCreatedEventBlob = false,
                ),
                InterfaceFilter(
                  iface2,
                  includeView = true,
                  includeCreatedEventBlob = false,
                ),
              ),
            )
          )
        ),
        party2 -> Filters(
          Some(
            InclusiveFilters(
              Set.empty,
              Set(
                InterfaceFilter(
                  iface1,
                  includeView = true,
                  includeCreatedEventBlob = false,
                ),
                InterfaceFilter(
                  iface2,
                  includeView = true,
                  includeCreatedEventBlob = false,
                ),
              ),
            )
          )
        ),
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false)
      .render(Set(party, party2), template1) shouldBe Projection(
      Set(iface2, iface1),
      false,
      false,
    )
  }

  behavior of "projecting created_event_blob"

  it should "project created_event_blob in case of match by interface" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(Set.empty, Set(InterfaceFilter(iface1, false, true)))
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, false)
  }

  it should "project created_event_blob in case of match by interface and template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(template1Filter.copy(includeCreatedEventBlob = true)),
            Set(InterfaceFilter(iface1, false, true)),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)
  }

  it should "project created_event_blob in case of match by interface and template with include the view" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(template1Filter.copy(includeCreatedEventBlob = true)),
            Set(InterfaceFilter(iface1, true, true)),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set(iface1), true, true)
  }

  it should "project created_event_blob in case of at least a single interface requesting it" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(
                iface1,
                false,
                includeCreatedEventBlob = true,
              ),
              InterfaceFilter(
                iface2,
                false,
                includeCreatedEventBlob = false,
              ),
            ),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, false)
  }

  it should "not project created_event_blob in case of no match by interface" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(
                iface1,
                false,
                includeCreatedEventBlob = true,
              )
            ),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, false).render(
      Set(party),
      template2,
    ) shouldBe Projection(Set.empty, false, false)
  }

  it should "project created_event_blob for wildcard templates, if it is specified explcitly via interface filter" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(
                iface1,
                false,
                includeCreatedEventBlob = true,
              )
            ),
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true).render(
      Set(party),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project created_event_blob for wildcard templates, if it is specified explicitly via template filter" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(
              TemplateFilter(template1, true)
            ),
            Set.empty,
          )
        )
      )
    )
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)
    EventProjectionProperties(transactionFilter, true, interfaceImpl, true).render(
      Set(party),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }
}

object EventProjectionPropertiesSpec {
  trait Scope {
    val template1: Identifier = Identifier.assertFromString("PackageName:ModuleName:template1")
    val template1Filter: TemplateFilter =
      TemplateFilter(templateId = template1, includeCreatedEventBlob = false)
    val template2: Identifier = Identifier.assertFromString("PackageName:ModuleName:template2")
    val id: Identifier = Identifier.assertFromString("PackageName:ModuleName:id")
    val iface1: Identifier = Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2: Identifier = Identifier.assertFromString("PackageName:ModuleName:iface2")

    val noInterface: Identifier => Set[Identifier] = _ => Set.empty[Identifier]
    val interfaceImpl: Identifier => Set[Identifier] = {
      case `iface1` => Set(template1)
      case `iface2` => Set(template1, template2)
      case _ => Set.empty
    }
    val party: Party = Party.assertFromString("party")
    val party2: Party = Party.assertFromString("party2")
    val noFilter = new TransactionFilter(Map())
    val wildcardFilter = new TransactionFilter(Map(party -> Filters(None)))
    val emptyInclusiveFilters = new TransactionFilter(
      Map(party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))))
    )
    def templateFilterFor(templateId: Identifier): Option[InclusiveFilters] = Some(
      InclusiveFilters(Set(TemplateFilter(templateId, false)), Set.empty)
    )
  }
}
