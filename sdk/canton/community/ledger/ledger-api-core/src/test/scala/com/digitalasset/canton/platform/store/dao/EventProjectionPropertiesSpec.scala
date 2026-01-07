// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  InterfaceFilter,
  TemplateFilter,
  TemplateWildcardFilter,
}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.dao.EventProjectionPropertiesSpec.Scope
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  FullIdentifier,
  Identifier,
  IdentifierConverter,
  NameTypeConRef,
  Party,
  TypeConRef,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventProjectionPropertiesSpec extends AnyFlatSpec with Matchers {
  behavior of "EventProjectionProperties"

  it should "propagate verbose flag" in new Scope {
    EventProjectionProperties(
      eventFormat = noFilter.copy(verbose = true),
      interfaceImplementedBy = noInterface,
      resolveTypeConRef = noTemplatesForPackageName,
    ).verbose shouldBe true
    EventProjectionProperties(
      eventFormat = noFilter.copy(verbose = false),
      interfaceImplementedBy = noInterface,
      resolveTypeConRef = noTemplatesForPackageName,
    ).verbose shouldBe false
  }

  it should "project nothing in case of empty filters" in new Scope {
    EventProjectionProperties(
      eventFormat = noFilter.copy(verbose = true),
      interfaceImplementedBy = noInterface,
      resolveTypeConRef = noTemplatesForPackageName,
    )
      .render(Set(party), id) shouldBe Projection(Set.empty, false)
  }

  it should "project nothing in case of not matching template filter" in new Scope {
    EventProjectionProperties(
      eventFormat = templateWildcardFilter(),
      interfaceImplementedBy = interfaceImpl,
      resolveTypeConRef = noTemplatesForPackageName,
    )
      .render(Set(party), id) shouldBe Projection(Set.empty, false)

    EventProjectionProperties(
      eventFormat = templateWildcardPartyWildcardFilter(),
      interfaceImplementedBy = interfaceImpl,
      resolveTypeConRef = noTemplatesForPackageName,
    )
      .render(Set(party), id) shouldBe Projection(Set.empty, false)
  }

  behavior of "projecting interfaces"

  projectingInterfacesTests(withPartyWildcard = false)
  projectingInterfacesTests(withPartyWildcard = true)

  behavior of "projecting created_event_blob"

  projectingBlobTests(withPartyWildcard = false)
  projectingBlobTests(withPartyWildcard = true)

  it should "project created_event_blob for everything if set as default" in new Scope {
    private val transactionFilter = EventFormat(
      filtersByParty = Map(
        party ->
          CumulativeFilter(
            templateFilters = Set(TemplateFilter(template1Full.toIdentifier.toRef, false)),
            interfaceFilters = Set.empty,
            templateWildcardFilter = None,
          ),
        party2 ->
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              InterfaceFilter(
                iface1Ref,
                false,
                includeCreatedEventBlob = false,
              )
            ),
            templateWildcardFilter = None,
          ),
        party3 -> CumulativeFilter.templateWildcardFilter(),
      ),
      filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter(true)),
      verbose = true,
    )
    val testee = EventProjectionProperties(
      eventFormat = transactionFilter,
      interfaceImplementedBy = interfaceImpl,
      resolveTypeConRef = noTemplatesForPackageName,
    )
    testee.render(Set(party), template1).createdEventBlob shouldBe true
    testee.render(Set(party2), template1).createdEventBlob shouldBe true
    testee.render(Set(party3), template1).createdEventBlob shouldBe true
  }

  behavior of "combining projections"

  it should "project created_event_blob and contractArguments in case of match by interface, template-id and" +
    "package-name-scoped template when interface filters are defined with party-wildcard and template filters by party" in new Scope {
      private val templateFilters =
        CumulativeFilter(
          templateFilters = Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          interfaceFilters = Set.empty,
          templateWildcardFilter = None,
        )
      private val interfaceFilters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = iface1Ref,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )

      private val transactionFilter =
        EventFormat(
          filtersByParty = Map(party -> templateFilters),
          filtersForAnyParty = Some(interfaceFilters),
          verbose = true,
        )

      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )

      // createdEventBlob enabled as it's matched by the package-id scoped template filter for template1 with createdEventBlob = true
      // which internally translates to package-name which is the same with that of template2
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )
    }

  it should "project created_event_blob and contractArguments in case of match by interface, template-id and" +
    "package-name-scoped template when template filters are defined with party-wildcard and interface filters by party" in new Scope {
      private val templateFilters =
        CumulativeFilter(
          templateFilters = Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          interfaceFilters = Set.empty,
          templateWildcardFilter = None,
        )
      private val interfaceFilters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = iface1Ref,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter =
        EventFormat(
          filtersByParty = Map(party -> interfaceFilters),
          filtersForAnyParty = Some(templateFilters),
          verbose = true,
        )

      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )

      // createdEventBlob enabled as it's matched by the package-id scoped template filter for template1 with createdEventBlob = true
      // which internally translates to package-name which is the same with that of template2
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )
    }

  it should "project created_event_blob and interface in case of match by interface and template when filters exist in party-wildcard and by party" in new Scope {
    private val templateFilters =
      CumulativeFilter(
        templateFilters = Set(
          template1Filter.copy(includeCreatedEventBlob = true)
        ),
        interfaceFilters = Set.empty,
        templateWildcardFilter = None,
      )
    private val interfaceFilters =
      CumulativeFilter(
        templateFilters = Set.empty,
        interfaceFilters = Set(
          InterfaceFilter(
            interfaceTypeRef = iface1Ref,
            includeView = true,
            includeCreatedEventBlob = false,
          )
        ),
        templateWildcardFilter = None,
      )

    private val transactionFilter =
      EventFormat(
        filtersByParty = Map(party -> templateFilters),
        filtersForAnyParty = Some(interfaceFilters),
        verbose = true,
      )
    private val transactionFilterSwapped =
      EventFormat(
        filtersByParty = Map(party -> interfaceFilters),
        filtersForAnyParty = Some(templateFilters),
        verbose = true,
      )

    private val eventProjectionProperties = EventProjectionProperties(
      eventFormat = transactionFilter,
      interfaceImplementedBy = interfaceImpl,
      templatesForPackageName,
    )
    private val eventProjectionPropertiesSwapped = EventProjectionProperties(
      eventFormat = transactionFilterSwapped,
      interfaceImplementedBy = interfaceImpl,
      templatesForPackageName,
    )

    eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
      interfaces = Set(iface1),
      createdEventBlob = true,
    )
    eventProjectionPropertiesSwapped.render(Set(party), template1) shouldBe Projection(
      interfaces = Set(iface1),
      createdEventBlob = true,
    )
  }

  def projectingInterfacesTests(withPartyWildcard: Boolean) = {
    val details =
      if (withPartyWildcard) " (with party-wildcard filters)" else " (with filters by party)"

    it should "project interface in case of match by interface id and witness" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
      )
    }

    it should "project interface in case of match by interface id and witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
      )
    }

    it should "not project interface in case of match by interface id but not witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
        .render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = false,
      )
    }

    it should "project an interface and template in case of match by interface id, template and witness" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set(template1Filter),
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
        .render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
      )
    }

    it should "project an interface and template in case of match by interface id, template and witness with alwaysPopulateArguments" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set(template1Filter),
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
        .render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = false,
      )
    }

    it should "project multiple interfaces in case of match by multiple interface ids and witness" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
            InterfaceFilter(
              iface2Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }

      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
      eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
        interfaces = Set(iface1, iface2),
        createdEventBlob = false,
      )
      eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1, iface2),
        createdEventBlob = false,
      )
    }

    if (withPartyWildcard) {
      it should "project multiple interfaces in case of match by multiple interface ids and witness when combined with party-wildcard" in new Scope {
        private val filter1 =
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              InterfaceFilter(
                iface1Ref,
                includeView = true,
                includeCreatedEventBlob = false,
              )
            ),
            templateWildcardFilter = None,
          )
        private val filter2 =
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              InterfaceFilter(
                iface2Ref,
                includeView = true,
                includeCreatedEventBlob = false,
              )
            ),
            templateWildcardFilter = None,
          )
        private val transactionFilter =
          EventFormat(Map(party -> filter1), Some(filter2), verbose = true)
        private val eventProjectionProperties = EventProjectionProperties(
          eventFormat = transactionFilter,
          interfaceImplementedBy = interfaceImpl,
          resolveTypeConRef = noTemplatesForPackageName,
        )
        eventProjectionProperties.render(Set(party), template1) shouldBe Projection(
          Set(iface1, iface2),
          false,
        )
        eventProjectionProperties.render(Set(party2), template1) shouldBe Projection(
          Set(iface2),
          false,
        )
      }
    }

    it should "deduplicate projected interfaces and include the view" ++ details in new Scope {
      private val filter1 =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = false,
              includeCreatedEventBlob = false,
            ),
            InterfaceFilter(
              iface2Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
          ),
          templateWildcardFilter = None,
        )
      private val filter2 =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
            InterfaceFilter(
              iface2Ref,
              includeView = true,
              includeCreatedEventBlob = false,
            ),
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(
              party -> filter1,
              party2 -> filter2,
            ),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map(
              party2 -> filter2
            ),
            filtersForAnyParty = Some(filter1),
            verbose = true,
          )
      }

      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
        .render(Set(party, party2), template1) shouldBe Projection(
        Set(iface1, iface2),
        false,
      )
    }

  }

  def projectingBlobTests(withPartyWildcard: Boolean) = {
    val details =
      if (withPartyWildcard) " (with party-wildcard filters)" else " (with filters by party)"

    it should "project created_event_blob in case of match by interface" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = iface1Ref,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = withPartyWildcard,
      )
    }

    it should "project created_event_blob in case of match by template-wildcard" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = iface1Ref,
              includeView = false,
              includeCreatedEventBlob = false,
            )
          ),
          templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = true)),
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )
      eventProjectionProperties.render(
        Set(party2),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = withPartyWildcard,
      )
    }

    it should "project created_event_blob in case of match by interface, template-id and package-name-scoped template" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set(
            template1Filter.copy(includeCreatedEventBlob = true),
            TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
          ),
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = iface1Ref,
              includeView = false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        templatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )

      // createdEventBlob enabled as it's matched by the package-id scoped template filter with createdEventBlob = true
      // (it internally translates to package-name which is the same with that of template2)
      eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
        interfaces = Set.empty,
        createdEventBlob = true,
      )
    }

    it should "project created_event_blob in case of match by interface and template with include the view" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set(template1Filter.copy(includeCreatedEventBlob = true)),
          interfaceFilters = Set(InterfaceFilter(iface1Ref, true, true)),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(
        interfaces = Set(iface1),
        createdEventBlob = true,
      )
      eventProjectionProperties.render(
        witnesses = Set(party2),
        templateId = template1,
      ) shouldBe Projection(
        interfaces = if (!withPartyWildcard) Set.empty else Set(iface1),
        createdEventBlob = withPartyWildcard,
      )

    }

    it should "project created_event_blob in case of at least a single interface requesting it" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              false,
              includeCreatedEventBlob = true,
            ),
            InterfaceFilter(
              iface2Ref,
              false,
              includeCreatedEventBlob = false,
            ),
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }

      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      ).render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true)

    }

    it should "project created_event_blob in case of match by interface, template-id (but both without flag enabled) and " +
      "package-name-scoped template (flag enabled)" ++ details in new Scope {
        val template2Filter: TemplateFilter =
          TemplateFilter(template2Full.toIdentifier.toRef, includeCreatedEventBlob = false)
        private val filters =
          CumulativeFilter(
            templateFilters = Set(
              template1Filter,
              template2Filter,
              TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = true),
            ),
            interfaceFilters = Set(
              InterfaceFilter(iface1Ref, false, false),
              InterfaceFilter(iface2Ref, false, false),
            ),
            templateWildcardFilter = None,
          )
        private val transactionFilter = withPartyWildcard match {
          case false =>
            EventFormat(
              filtersByParty = Map(party -> filters),
              filtersForAnyParty = None,
              verbose = true,
            )
          case true =>
            EventFormat(
              filtersByParty = Map.empty,
              filtersForAnyParty = Some(filters),
              verbose = true,
            )
        }

        private val eventProjectionProperties: EventProjectionProperties =
          EventProjectionProperties(
            eventFormat = transactionFilter,
            interfaceImplementedBy = interfaceImpl,
            resolveTypeConRef = Map(
              template1Ref -> Set(template1Full),
              template2Ref -> Set(template2Full),
              iface1Ref -> Set(iface1),
              iface2Ref -> Set(iface2),
              packageNameScopedTemplate -> Set(template2Full),
            ),
          )

        // createdEventBlob enabled as it's matched by the package-named scoped template filter with createdEventBlob = true
        // (template1 internally translates to package-name which is the same with that of template2)
        eventProjectionProperties.render(
          Set(party),
          template1,
        ) shouldBe Projection(Set.empty, true)

        eventProjectionProperties.render(
          Set(party),
          template3,
        ) shouldBe Projection(Set.empty, false)

        // createdEventBlob enabled as it's matched by the package-name scoped template filter with createdEventBlob = true
        eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
          interfaces = Set.empty,
          createdEventBlob = true,
        )
      }

    it should "not project created_event_blob in case of no match by interface" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      ).render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, true)

      EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      ).render(
        Set(party),
        template3,
      ) shouldBe Projection(Set.empty, false)

    }

    it should "project created_event_blob for wildcard templates, if it is specified explicitly via interface filter" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set(
            InterfaceFilter(
              iface1Ref,
              false,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }

      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )
      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true)
      eventProjectionProperties.render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, true)
      eventProjectionProperties.render(
        Set(party),
        template3,
      ) shouldBe Projection(Set.empty, false)
    }

    it should "project created_event_blob for wildcard templates, if it is specified explicitly via template filter" ++ details in new Scope {
      private val filters =
        CumulativeFilter(
          templateFilters = Set(TemplateFilter(template1Full.toIdentifier.toRef, true)),
          interfaceFilters = Set.empty,
          templateWildcardFilter = None,
        )
      private val transactionFilter = withPartyWildcard match {
        case false =>
          EventFormat(
            filtersByParty = Map(party -> filters),
            filtersForAnyParty = None,
            verbose = true,
          )
        case true =>
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(filters),
            verbose = true,
          )
      }
      private val eventProjectionProperties = EventProjectionProperties(
        eventFormat = transactionFilter,
        interfaceImplementedBy = interfaceImpl,
        resolveTypeConRef = noTemplatesForPackageName,
      )

      eventProjectionProperties.render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, true)
      eventProjectionProperties.render(
        Set(party),
        template2,
      ) shouldBe Projection(Set.empty, true)
      eventProjectionProperties.render(
        Set(party),
        template3,
      ) shouldBe Projection(Set.empty, false)
    }

  }

}
object EventProjectionPropertiesSpec {
  trait Scope {
    val packageRefName = Ref.PackageRef.Name(Ref.PackageName.assertFromString("PackageName"))
    val qualifiedName: Ref.QualifiedName = Ref.QualifiedName.assertFromString("ModuleName:template")
    val packageNameScopedTemplate: Ref.TypeConRef = Ref.TypeConRef(packageRefName, qualifiedName)

    val template1Full: FullIdentifier = Identifier
      .assertFromString("PackageId2:ModuleName:template")
      .toFullIdentifier(packageRefName.name)
    val template1: NameTypeConRef = template1Full.toNameTypeConRef
    val template1Ref: Ref.TypeConRef = template1Full.toIdentifier.toRef
    val template1Filter: TemplateFilter =
      TemplateFilter(template1Full.toIdentifier.toRef, includeCreatedEventBlob = false)
    val template2Full: FullIdentifier = Identifier
      .assertFromString("PackageId1:ModuleName:template")
      .toFullIdentifier(packageRefName.name)
    val template2: NameTypeConRef = template2Full.toNameTypeConRef
    val template2Ref: Ref.TypeConRef = template2Full.toIdentifier.toRef
    val template3Full: FullIdentifier = Identifier
      .assertFromString("PackageId1:ModuleName:template3")
      .toFullIdentifier(packageRefName.name)
    val template3: NameTypeConRef = template3Full.toNameTypeConRef
    val template3Ref: Ref.TypeConRef = template3Full.toIdentifier.toRef
    val id: NameTypeConRef =
      NameTypeConRef.assertFromString(s"$packageRefName:ModuleName:id")
    val iface1: FullIdentifier = Identifier
      .assertFromString("PackageId:ModuleName:iface1")
      .toFullIdentifier(packageRefName.name)
    val iface1Ref: TypeConRef = iface1.toIdentifier.toRef
    val iface2: FullIdentifier = Identifier
      .assertFromString("PackageId:ModuleName:iface2")
      .toFullIdentifier(packageRefName.name)
    val iface2Ref: TypeConRef = iface2.toIdentifier.toRef
    val packageNameScopedIface1 = Ref.TypeConRef(packageRefName, iface1.qualifiedName)

    val noInterface: FullIdentifier => Set[FullIdentifier] = _ => Set.empty[FullIdentifier]
    val noTemplatesForPackageName: TypeConRef => Set[FullIdentifier] =
      Map(
        template1Ref -> Set(template1Full),
        template2Ref -> Set(template2Full),
        template3Ref -> Set(template3Full),
        iface1Ref -> Set(iface1),
        iface2Ref -> Set(iface2),
      )
    val templatesForPackageName: TypeConRef => Set[FullIdentifier] =
      Map(
        template1Ref -> Set(template1Full),
        template2Ref -> Set(template2Full),
        iface1Ref -> Set(iface1),
        iface2Ref -> Set(iface2),
        packageNameScopedTemplate -> Set(template1Full, template2Full),
        packageNameScopedIface1 -> Set(iface1),
      )

    val interfaceImpl: FullIdentifier => Set[FullIdentifier] = {
      case `iface1` => Set(template1Full)
      case `iface2` => Set(template1Full, template2Full, template3Full)
      case _ => Set.empty
    }
    val party: Party = Party.assertFromString("party")
    val party2: Party = Party.assertFromString("party2")
    val party3: Party = Party.assertFromString("party3")
    val noFilter = EventFormat(
      filtersByParty = Map(),
      filtersForAnyParty = None,
      verbose = true,
    )
    def templateWildcardFilter(includeCreatedEventBlob: Boolean = false) = EventFormat(
      filtersByParty = Map(
        party ->
          CumulativeFilter(
            Set.empty,
            Set.empty,
            Some(TemplateWildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)),
          )
      ),
      filtersForAnyParty = None,
      verbose = true,
    )
    def templateWildcardPartyWildcardFilter(includeCreatedEventBlob: Boolean = false) =
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set.empty,
            templateWildcardFilter =
              Some(TemplateWildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)),
          )
        ),
        verbose = true,
      )
    val emptyCumulativeFilters = EventFormat(
      filtersByParty = Map(
        party ->
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set.empty,
            templateWildcardFilter = None,
          )
      ),
      filtersForAnyParty = None,
      verbose = true,
    )
    val emptyCumulativePartyWildcardFilters = EventFormat(
      filtersByParty = Map.empty,
      filtersForAnyParty = Some(
        CumulativeFilter(
          templateFilters = Set.empty,
          interfaceFilters = Set.empty,
          templateWildcardFilter = None,
        )
      ),
      verbose = true,
    )
    def templateFilterFor(templateTypeRef: Ref.TypeConRef): CumulativeFilter =
      CumulativeFilter(
        Set(TemplateFilter(templateTypeRef = templateTypeRef, includeCreatedEventBlob = false)),
        Set.empty,
        None,
      )
  }
}
