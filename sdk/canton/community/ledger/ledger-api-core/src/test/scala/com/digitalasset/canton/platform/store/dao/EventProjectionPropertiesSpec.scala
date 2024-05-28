// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party, TypeConRef}
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
    EventProjectionProperties(
      transactionFilter = noFilter,
      verbose = true,
      interfaceImplementedBy = noInterface,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = false,
    ).verbose shouldBe true
    EventProjectionProperties(
      transactionFilter = noFilter,
      verbose = false,
      interfaceImplementedBy = noInterface,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = false,
    ).verbose shouldBe false
  }

  it should "project nothing in case of empty filters" in new Scope {
    EventProjectionProperties(
      transactionFilter = noFilter,
      verbose = true,
      interfaceImplementedBy = noInterface,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = false,
    )
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  it should "project nothing in case of empty witnesses" in new Scope {
    EventProjectionProperties(
      transactionFilter = templateWildcardFilter,
      verbose = true,
      interfaceImplementedBy = interfaceImpl,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = false,
    )
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)

    EventProjectionProperties(
      transactionFilter = templateWildcardPartyWildcardFilter,
      verbose = true,
      interfaceImplementedBy = interfaceImpl,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = false,
    )
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  behavior of "projecting contract arguments"

  projectingContractArgumentsTests(withPartyWildcard = false)
  projectingContractArgumentsTests(withPartyWildcard = true)

  behavior of "projecting interfaces"

  projectingInterfacesTests(withPartyWildcard = false)
  projectingInterfacesTests(withPartyWildcard = true)

  behavior of "projecting created_event_blob"

  projectingBlobTests(withPartyWildcard = false)
  projectingBlobTests(withPartyWildcard = true)

  it should "project created_event_blob for everything if set as default" in new Scope {
    private val transactionFilter = TransactionFilter(
      filtersByParty = Map(
        party -> Filters(
          InclusiveFilters(
            Set(TemplateFilter(template1, false)),
            Set.empty,
          )
        ),
        party2 -> Filters(
          InclusiveFilters(
            Set.empty,
            Set(
              InterfaceFilter(
                iface1,
                false,
                includeCreatedEventBlob = false,
              )
            ),
          )
        ),
        party3 -> Filters.noFilter,
      ),
      filtersForAnyParty = Some(Filters.noFilter),
      alwaysPopulateCreatedEventBlob = true,
    )
    val testee = EventProjectionProperties(
      transactionFilter = transactionFilter,
      verbose = true,
      interfaceImplementedBy = interfaceImpl,
      resolveTemplateIds = noTemplatesForPackageName,
      alwaysPopulateArguments = true,
    )
    testee.render(Set(party), template1).createdEventBlob shouldBe true
    testee.render(Set(party2), template1).createdEventBlob shouldBe true
    testee.render(Set(party3), template1).createdEventBlob shouldBe true
  }

  behavior of "combining projections"

  it should "project created_event_blob and contractArguments in case of match by interface, template-id and" +
    "package-name-scoped template when interface filters are defined with party-wildcard and template filters by party" in new Scope {
      private val templateFilters = Filters(
        InclusiveFilters(
          Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          Set.empty,
        )
      )
      private val interfaceFilters = Filters(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              interfaceId = iface1,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
        )
      )

      private val transactionFilter =
        TransactionFilter(
          filtersByParty = Map(party -> templateFilters),
          filtersForAnyParty = Some(interfaceFilters),
        )

      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
        contractArguments = true,
      )

      // createdEventBlob not enabled as it's only matched by the package-name scoped template filter with createdEventBlob = false
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
    }

  it should "project created_event_blob and contractArguments in case of match by interface, template-id and" +
    "package-name-scoped template when template filters are defined with party-wildcard and interface filters by party" in new Scope {
      private val templateFilters = Filters(
        InclusiveFilters(
          Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          Set.empty,
        )
      )
      private val interfaceFilters = Filters(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              interfaceId = iface1,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
        )
      )
      private val transactionFilter =
        TransactionFilter(
          filtersByParty = Map(party -> interfaceFilters),
          filtersForAnyParty = Some(templateFilters),
        )

      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
        contractArguments = true,
      )

      // createdEventBlob not enabled as it's only matched by the package-name scoped template filter with createdEventBlob = false
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
    }

  it should "project created_event_blob and interface in case of match by interface and template when filters exist in party-wildcard and by party" in new Scope {
    private val templateFilters = Filters(
      InclusiveFilters(
        Set(
          template1Filter.copy(includeCreatedEventBlob = true)
        ),
        Set.empty,
      )
    )
    private val interfaceFilters = Filters(
      InclusiveFilters(
        Set.empty,
        Set(
          InterfaceFilter(
            interfaceId = iface1,
            includeView = true,
            includeCreatedEventBlob = false,
          )
        ),
      )
    )

    private val transactionFilter =
      TransactionFilter(
        filtersByParty = Map(party -> templateFilters),
        filtersForAnyParty = Some(interfaceFilters),
      )
    private val transactionFilterSwapped =
      TransactionFilter(
        filtersByParty = Map(party -> interfaceFilters),
        filtersForAnyParty = Some(templateFilters),
      )

    private val eventProjectionProperties = EventProjectionProperties(
      transactionFilter = transactionFilter,
      verbose = true,
      interfaceImplementedBy = interfaceImpl,
      templatesForPackageName,
      alwaysPopulateArguments = false,
    )
    private val eventProjectionPropertiesSwapped = EventProjectionProperties(
      transactionFilter = transactionFilterSwapped,
      verbose = true,
      interfaceImplementedBy = interfaceImpl,
      templatesForPackageName,
      alwaysPopulateArguments = false,
    )

    eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
      interfaces = Set(iface1),
      createdEventBlob = true,
      contractArguments = true,
    )
    eventProjectionPropertiesSwapped.render(Set(party), template1) shouldBe Projection(
      interfaces = Set(iface1),
      createdEventBlob = true,
      contractArguments = true,
    )
  }

  def projectingContractArgumentsTests(withPartyWildcard: Boolean) = {
    val details =
      if (withPartyWildcard) " (with party-wildcard filters)" else " (with filters by party)"

    it should "project contract arguments in case of match by template" ++ details in new Scope {
      private val filters = Filters(templateFilterFor(template1Ref))
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = withPartyWildcard,
      )
    }

    it should "not project contract arguments in case of mismatch by template" ++ details in new Scope {
      private val filters = Filters(templateFilterFor(template1Ref))
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      ).render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, false, false)
    }

    it should "project contract arguments in case of match by package-name" ++ details in new Scope {
      private val filters = Filters(templateFilterFor(packageNameScopedTemplate))
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = templatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
      eventProjectionProperties.render(Set(party2), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = withPartyWildcard,
      )
      eventProjectionProperties.render(Set(party), template3) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = false,
      )
    }

    it should "project contract arguments in case of template-wildcard match" ++ details in new Scope {
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter =
          if (withPartyWildcard) templateWildcardPartyWildcardFilter else templateWildcardFilter,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, false, true)
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(Set.empty, false, withPartyWildcard)
    }

    it should "project contract arguments in case of empty InclusiveFilters" ++ details in new Scope {
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter =
          if (withPartyWildcard) emptyInclusivePartyWildcardFilters else emptyInclusiveFilters,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, false, true)
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(Set.empty, false, withPartyWildcard)
    }

    it should "project contract arguments with template-wildcard and another filter" ++ details in new Scope {
      private val filters = Filters(Some(InclusiveFilters(Set.empty, Set.empty)))
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            Map(
              party -> filters,
              party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
            )
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map(
              party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty)))
            ),
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = noInterface,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party, party2),
        template2,
      ) shouldBe Projection(Set.empty, false, true)
      eventProjectionProperties.render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, false, true)
      eventProjectionProperties.render(
        Set(party2),
        template2,
      ) shouldBe Projection(Set.empty, false, withPartyWildcard)
    }

    it should "project contract arguments if interface filter and package-name scope template filter" ++ details in new Scope {
      private val filters: Filters = Filters(
        Some(
          InclusiveFilters(
            Set(TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false)),
            Set(InterfaceFilter(iface1, includeView = true, includeCreatedEventBlob = false)),
          )
        )
      )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = Map(iface1 -> Set(template1)),
        resolveTemplateIds = templatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
        contractArguments = true,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
        contractArguments = withPartyWildcard,
      )
    }

    it should "project contract arguments with template-wildcard and another filter with alwaysPopulateArguments, " +
      "if queried non template-wildcard party/template combination" ++ details in new Scope {
        private val filters: Filters = Filters(Some(InclusiveFilters(Set.empty, Set.empty)))
        private val transactionFilter = withPartyWildcard match {
          case false =>
            TransactionFilter(filtersByParty =
              Map(
                party -> filters,
                party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
              )
            )
          case true =>
            TransactionFilter(
              filtersByParty = Map(
                party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty)))
              ),
              filtersForAnyParty = Some(filters),
            )
        }

        private val eventProjectionProperties = EventProjectionProperties(
          transactionFilter = transactionFilter,
          verbose = true,
          interfaceImplementedBy = noInterface,
          resolveTemplateIds = noTemplatesForPackageName,
          alwaysPopulateArguments = true,
        )
        eventProjectionProperties.render(
          Set(party2),
          template2,
        ) shouldBe Projection(
          interfaces = Set.empty,
          createdEventBlob = false,
          contractArguments = true,
        )
        eventProjectionProperties.render(
          Set(party3),
          template2,
        ) shouldBe Projection(
          interfaces = Set.empty,
          createdEventBlob = false,
          contractArguments = withPartyWildcard,
        )
      }

  }

  def projectingInterfacesTests(withPartyWildcard: Boolean) = {
    val details =
      if (withPartyWildcard) " (with party-wildcard filters)" else " (with filters by party)"

    it should "project interface in case of match by interface id and witness" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
        contractArguments = false,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
        contractArguments = false,
      )
    }

    it should "project interface in case of match by interface id and witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = true,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
        contractArguments = true,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
        contractArguments = withPartyWildcard,
      )
    }

    it should "not project interface in case of match by interface id and witness" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
        .render(Set(party), template1) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = false,
      )

    }

    it should "not project interface in case of match by interface id but not witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = true,
      )
        .render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
        contractArguments = withPartyWildcard,
      )
    }

    it should "project an interface and template in case of match by interface id, template and witness" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
        .render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
        contractArguments = true,
      )
    }

    it should "project an interface and template in case of match by interface id, template and witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = true,
      )
        .render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
        contractArguments = true,
      )
    }

    it should "project multiple interfaces in case of match by multiple interface ids and witness" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            filtersByParty = Map(party -> filters)
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1, iface2),
        createdEventBlob = false,
        contractArguments = false,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1, iface2),
        createdEventBlob = false,
        contractArguments = false,
      )
    }

    if (withPartyWildcard) {
      it should "project multiple interfaces in case of match by multiple interface ids and witness when combined with party-wildcard" in new Scope {
        private val filter1 = Filters(
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
        private val filter2 = Filters(
          Some(
            InclusiveFilters(
              Set.empty,
              Set(
                InterfaceFilter(
                  iface2,
                  includeView = true,
                  includeCreatedEventBlob = false,
                )
              ),
            )
          )
        )
        private val transactionFilter = TransactionFilter(Map(party -> filter1), Some(filter2))
        private val eventProjectionProperties = EventProjectionProperties(
          transactionFilter = transactionFilter,
          verbose = true,
          interfaceImplementedBy = interfaceImpl,
          resolveTemplateIds = noTemplatesForPackageName,
          alwaysPopulateArguments = false,
        )
        eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
          Set(iface1, iface2),
          false,
          false,
        )
        eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
          Set(iface2),
          false,
          false,
        )
      }
    }

    it should "deduplicate projected interfaces and include the view" ++ details in new Scope {
      private val filter1 = Filters(
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
      )
      private val filter2 = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(
            Map(
              party -> filter1,
              party2 -> filter2,
            )
          )
        case true =>
          TransactionFilter(
            filtersByParty = Map(
              party2 -> filter2
            ),
            filtersForAnyParty = Some(filter1),
          )
      }

      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )
        .render(Set(party, party2), template1) shouldBe Projection(
        Set(iface2, iface1),
        false,
        false,
      )
    }

  }

  def projectingBlobTests(withPartyWildcard: Boolean) = {
    val details =
      if (withPartyWildcard) " (with party-wildcard filters)" else " (with filters by party)"

    it should "project created_event_blob in case of match by interface" ++ details in new Scope {
      private val filters = Filters(
        InclusiveFilters(
          Set.empty,
          Set(
            InterfaceFilter(
              interfaceId = iface1,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
        )
      )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
        contractArguments = false,
      )
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = withPartyWildcard,
        contractArguments = false,
      )
    }

    it should "project created_event_blob in case of match by interface, template-id and package-name-scoped template" ++ details in new Scope {
      private val filters = Filters(
        InclusiveFilters(
          Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          Set(
            InterfaceFilter(
              interfaceId = iface1,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
        )
      )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
        contractArguments = true,
      )

      // createdEventBlob not enabled as it's only matched by the package-name scoped template filter with createdEventBlob = false
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = false,
        contractArguments = true,
      )
    }

    it should "project created_event_blob in case of match by interface and template with include the view" ++ details in new Scope {
      private val filters = Filters(
        InclusiveFilters(
          Set(template1Filter.copy(includeCreatedEventBlob = true)),
          Set(InterfaceFilter(iface1, true, true)),
        )
      )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = true,
        contractArguments = true,
      )
      eventProjectionProperties.render(
        witnesses = Set(party2),
        templateId = template1,
      ) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = withPartyWildcard,
        contractArguments = withPartyWildcard,
      )

    }

    it should "project created_event_blob in case of at least a single interface requesting it" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      ).render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true, false)

    }

    it should "project created_event_blob in case of match by interface, template-id (but both without flag enabled) and " +
      "package-name-scoped template (flag enabled)" ++ details in new Scope {
        val template2Filter: TemplateFilter =
          TemplateFilter(templateId = template2, includeCreatedEventBlob = false)
        private val filters = Filters(
          InclusiveFilters(
            Set(
              template1Filter,
              template2Filter,
              TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = true),
            ),
            Set(InterfaceFilter(iface1, false, false), InterfaceFilter(iface2, false, false)),
          )
        )
        private val transactionFilter = withPartyWildcard match {
          case false =>
            TransactionFilter(filtersByParty = Map(party -> filters))
          case true =>
            TransactionFilter(
              filtersByParty = Map.empty,
              filtersForAnyParty = Some(filters),
            )
        }

        private val eventProjectionProperties: EventProjectionProperties =
          EventProjectionProperties(
            transactionFilter = transactionFilter,
            verbose = true,
            interfaceImplementedBy = interfaceImpl,
            resolveTemplateIds = Map(
              template1Ref -> Set(template1),
              template2Ref -> Set(template2),
              packageNameScopedTemplate -> Set(template2),
            ),
            alwaysPopulateArguments = false,
          )
        eventProjectionProperties.render(
          Set(party),
          template1,
        ) shouldBe Projection(Set.empty, false, true)

        // createdEventBlob enabled as it's matched by the package-name scoped template filter with createdEventBlob = true
        eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
          interfaces = Set.empty,
          createdEventBlob = true,
          contractArguments = true,
        )
      }

    it should "not project created_event_blob in case of no match by interface" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = false,
      ).render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, false, false)

    }

    it should "project created_event_blob for wildcard templates, if it is specified explicitly via interface filter" ++ details in new Scope {
      private val filters = Filters(
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
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }

      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = true,
      )
      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true, true)
      eventProjectionProperties.render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, false, true)
    }

    it should "project created_event_blob for wildcard templates, if it is specified explicitly via template filter" ++ details in new Scope {
      private val filters = Filters(
        InclusiveFilters(
          Set(TemplateFilter(template1, true)),
          Set.empty,
        )
      )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          TransactionFilter(filtersByParty = Map(party -> filters))
        case true =>
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        transactionFilter = transactionFilter,
        verbose = true,
        interfaceImplementedBy = interfaceImpl,
        resolveTemplateIds = noTemplatesForPackageName,
        alwaysPopulateArguments = true,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true, true)
      eventProjectionProperties.render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, false, true)
    }

  }

}
object EventProjectionPropertiesSpec {
  trait Scope {
    val packageName: Ref.PackageName = Ref.PackageName.assertFromString("PackageName")
    val qualifiedName: Ref.QualifiedName = Ref.QualifiedName.assertFromString("ModuleName:template")
    val packageNameScopedTemplate: Ref.TypeConRef =
      Ref.TypeConRef(Ref.PackageRef.Name(packageName), qualifiedName)

    val template1: Identifier = Identifier.assertFromString("PackageId2:ModuleName:template")
    val template1Ref: Ref.TypeConRef = TypeConRef.fromIdentifier(template1)
    val template1Filter: TemplateFilter =
      TemplateFilter(templateId = template1, includeCreatedEventBlob = false)
    val template2: Identifier = Identifier.assertFromString("PackageId1:ModuleName:template")
    val template2Ref: Ref.TypeConRef = TypeConRef.fromIdentifier(template2)
    val template3: Identifier = Identifier.assertFromString("PackageId3:ModuleName:template")
    val id: Identifier = Identifier.assertFromString("PackageId:ModuleName:id")
    val iface1: Identifier = Identifier.assertFromString("PackageId:ModuleName:iface1")
    val iface2: Identifier = Identifier.assertFromString("PackageId:ModuleName:iface2")

    val noInterface: Identifier => Set[Identifier] = _ => Set.empty[Identifier]
    val noTemplatesForPackageName: TypeConRef => Set[Identifier] =
      Map(
        template1Ref -> Set(template1),
        template2Ref -> Set(template2),
      )
    val templatesForPackageName: TypeConRef => Set[Identifier] =
      Map(
        template1Ref -> Set(template1),
        template2Ref -> Set(template2),
        packageNameScopedTemplate -> Set(template1, template2),
      )

    val interfaceImpl: Identifier => Set[Identifier] = {
      case `iface1` => Set(template1)
      case `iface2` => Set(template1, template2)
      case _ => Set.empty
    }
    val party: Party = Party.assertFromString("party")
    val party2: Party = Party.assertFromString("party2")
    val party3: Party = Party.assertFromString("party3")
    val noFilter = TransactionFilter(Map())
    val templateWildcardFilter = TransactionFilter(Map(party -> Filters(None)))
    val templateWildcardPartyWildcardFilter = TransactionFilter(
      filtersByParty = Map.empty,
      filtersForAnyParty = Some(Filters(None)),
    )
    val emptyInclusiveFilters = TransactionFilter(
      Map(party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))))
    )
    val emptyInclusivePartyWildcardFilters = TransactionFilter(
      filtersByParty = Map.empty,
      filtersForAnyParty = Some(Filters(Some(InclusiveFilters(Set.empty, Set.empty)))),
    )
    def templateFilterFor(templateTypeRef: Ref.TypeConRef): Option[InclusiveFilters] = Some(
      InclusiveFilters(Set(TemplateFilter(templateTypeRef, false)), Set.empty)
    )
  }
}
