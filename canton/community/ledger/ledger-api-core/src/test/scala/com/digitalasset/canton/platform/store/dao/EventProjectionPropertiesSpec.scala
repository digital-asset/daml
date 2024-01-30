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
      noFilter,
      true,
      noInterface,
      noTemplatesForPackageName,
      false,
    ).verbose shouldBe true
    EventProjectionProperties(
      noFilter,
      false,
      noInterface,
      noTemplatesForPackageName,
      false,
    ).verbose shouldBe false
  }

  it should "project nothing in case of empty filters" in new Scope {
    EventProjectionProperties(noFilter, true, noInterface, noTemplatesForPackageName, false)
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  it should "project nothing in case of empty witnesses" in new Scope {
    EventProjectionProperties(wildcardFilter, true, interfaceImpl, noTemplatesForPackageName, false)
      .render(Set.empty, id) shouldBe Projection(Set.empty, false, false)
  }

  behavior of "projecting contract arguments"

  it should "project contract arguments in case of match by template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(party -> Filters(templateFilterFor(template1Ref)))
    )
    EventProjectionProperties(
      transactionFilter,
      true,
      noInterface,
      noTemplatesForPackageName,
      false,
    ).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project contract arguments in case of match by package-name" in new Scope {
    val transactionFilter: TransactionFilter =
      TransactionFilter(Map(party -> Filters(templateFilterFor(packageNameScopedTemplate))))

    private val eventProjectionProperties: EventProjectionProperties = EventProjectionProperties(
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
  }

  it should "project contract arguments in case of wildcard match" in new Scope {
    EventProjectionProperties(wildcardFilter, true, noInterface, noTemplatesForPackageName, false)
      .render(
        Set(party),
        template1,
      ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project contract arguments in case of empty InclusiveFilters" in new Scope {
    EventProjectionProperties(
      emptyInclusiveFilters,
      true,
      noInterface,
      noTemplatesForPackageName,
      false,
    ).render(
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
      noTemplatesForPackageName,
      false,
    ).render(
      Set(party, party2),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "not project contract arguments with wildcard and another filter, if queried non wildcard party/template combination" in new Scope {
    EventProjectionProperties(
      new TransactionFilter(
        Map(
          party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))),
          party2 -> Filters(Some(InclusiveFilters(Set(template1Filter), Set.empty))),
        )
      ),
      true,
      noInterface,
      noTemplatesForPackageName,
      false,
    ).render(
      Set(party2),
      template2,
    ) shouldBe Projection(Set.empty, false, false)
  }

  it should "project contract arguments if interface filter and package-name scope template filter" in new Scope {
    private val filters: Filters = Filters(
      Some(
        InclusiveFilters(
          Set(TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false)),
          Set(InterfaceFilter(iface1, includeView = true, includeCreatedEventBlob = false)),
        )
      )
    )

    EventProjectionProperties(
      transactionFilter = TransactionFilter(Map(party -> filters)),
      verbose = true,
      interfaceImplementedBy = Map(iface1 -> Set(template1)),
      resolveTemplateIds = templatesForPackageName,
      alwaysPopulateArguments = false,
    ).render(Set(party), template1) shouldBe Projection(
      interfaces = Set(iface1),
      createdEventBlob = false,
      contractArguments = true,
    )
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
      noTemplatesForPackageName,
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    )
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

    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    ).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, false)
  }

  it should "project created_event_blob in case of match by interface, template-id and package-name-scoped template" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(
              template1Filter.copy(includeCreatedEventBlob = true),
              TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = false),
            ),
            Set(InterfaceFilter(iface1, false, true)),
          )
        )
      )
    )
    private val eventProjectionProperties: EventProjectionProperties = EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      templatesForPackageName,
      false,
    )
    eventProjectionProperties.render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)

    // createdEventBlob not enabled as it's only matched by the package-name scoped template filter with createdEventBlob = false
    eventProjectionProperties.render(Set(party), template2) shouldBe Projection(
      Set.empty,
      false,
      true,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    ).render(
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    ).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, false)
  }

  it should "project created_event_blob in case of match by interface, template-id (but both without flag enabled) and package-name-scoped template (flag enabled)" in new Scope {
    val template2Filter: TemplateFilter =
      TemplateFilter(templateId = template2, includeCreatedEventBlob = false)

    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(
              template1Filter,
              template2Filter,
              TemplateFilter(packageNameScopedTemplate, includeCreatedEventBlob = true),
            ),
            Set(InterfaceFilter(iface1, false, false), InterfaceFilter(iface2, false, false)),
          )
        )
      )
    )
    private val eventProjectionProperties: EventProjectionProperties = EventProjectionProperties(
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
      Set.empty,
      true,
      true,
    )
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      false,
    ).render(
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
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    ).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    ).render(
      Set(party),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
  }

  it should "project created_event_blob for wildcard templates, if it is specified explicitly via template filter" in new Scope {
    val transactionFilter = new TransactionFilter(
      Map(
        party -> Filters(
          InclusiveFilters(
            Set(TemplateFilter(template1, true)),
            Set.empty,
          )
        )
      )
    )
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    ).render(
      Set(party),
      template1,
    ) shouldBe Projection(Set.empty, true, true)
    EventProjectionProperties(
      transactionFilter,
      true,
      interfaceImpl,
      noTemplatesForPackageName,
      true,
    ).render(
      Set(party),
      template2,
    ) shouldBe Projection(Set.empty, false, true)
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
    val noFilter = TransactionFilter(Map())
    val wildcardFilter = TransactionFilter(Map(party -> Filters(None)))
    val emptyInclusiveFilters = TransactionFilter(
      Map(party -> Filters(Some(InclusiveFilters(Set.empty, Set.empty))))
    )
    def templateFilterFor(templateTypeRef: Ref.TypeConRef): Option[InclusiveFilters] = Some(
      InclusiveFilters(Set(TemplateFilter(templateTypeRef, false)), Set.empty)
    )
  }
}
