// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.domain.{
  CumulativeFilter,
  InterfaceFilter,
  TemplateFilter,
  TemplateWildcardFilter,
  TransactionFilter,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.index.IndexServiceImpl.*
import com.digitalasset.canton.platform.index.IndexServiceImplSpec.Scope
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party, QualifiedName, TypeConRef}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

class IndexServiceImplSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with EitherValues
    with OptionValues {

  behavior of "IndexServiceImpl.memoizedTransactionFilterProjection"

  it should "give an empty result if no packages" in new Scope {
    currentPackageMetadata = PackageMetadata()
    val memoFunc =
      memoizedTransactionFilterProjection(
        getPackageMetadataSnapshot = getPackageMetadata,
        transactionFilter = TransactionFilter(filtersByParty = Map.empty),
        verbose = true,
        alwaysPopulateArguments = false,
      )
    memoFunc() shouldBe None
  }

  it should "change the result in case of new package arrived" in new Scope {
    currentPackageMetadata = PackageMetadata()
    // subscribing to iface1
    val memoFunc = memoizedTransactionFilterProjection(
      getPackageMetadataSnapshot = getPackageMetadata,
      transactionFilter = TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(
            templateFilters = Set(),
            interfaceFilters = Set(iface1Filter),
            templateWildcardFilter = None,
          )
        )
      ),
      verbose = true,
      alwaysPopulateArguments = false,
    )
    memoFunc() shouldBe None // no template implementing iface1
    // template1 implements iface1
    currentPackageMetadata =
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1)))

    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(Map(template1 -> Some(Set(party))), Some(Set())),
        EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(Set.empty),
          templateWildcardCreatedEventBlobParties = Some(Set.empty),
          witnessTemplateProjections =
            Map(Some(party.toString) -> Map(template1 -> Projection(Set(iface1), false, false))),
        ),
      )
    ) // filter gets complicated, filters template1 for iface1, projects iface1

    // template2 also implements iface1 as template1
    currentPackageMetadata =
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1, template2)))

    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(
          relation = Map(
            template1 -> Some(Set(party)),
            template2 -> Some(Set(party)),
          ),
          templateWildcardParties = Some(Set()),
        ),
        EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(Set.empty),
          witnessTemplateProjections = Map(
            Some(party.toString) -> Map(
              template1 -> Projection(Set(iface1), false, false),
              template2 -> Projection(Set(iface1), false, false),
            )
          ),
          templateWildcardCreatedEventBlobParties = Some(Set.empty),
        ),
      )
    ) // filter gets even more complicated, filters template1 and template2 for iface1, projects iface1 for both templates
  }

  it should "populate all contract arguments correctly" in new Scope {
    currentPackageMetadata =
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1)))
    // subscribing to iface1
    val memoFunc = memoizedTransactionFilterProjection(
      getPackageMetadataSnapshot = getPackageMetadata,
      transactionFilter = TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(
            templateFilters = Set(),
            interfaceFilters = Set(iface1Filter),
            templateWildcardFilter = None,
          )
        )
      ),
      verbose = true,
      alwaysPopulateArguments = true,
    )
    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(Map(template1 -> Some(Set(party))), Some(Set())),
        EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(Set(party)),
          witnessTemplateProjections =
            Map(Some(party.toString) -> Map(template1 -> Projection(Set(iface1), false, false))),
          templateWildcardCreatedEventBlobParties = Some(Set.empty),
        ),
      )
    )
  }

  behavior of "IndexServiceImpl.wildcardFilter"

  it should "give empty result for the empty input" in new Scope {
    wildcardFilter(
      TransactionFilter(filtersByParty = Map.empty)
    ) shouldBe Some(Set.empty)
  }

  it should "give empty result for filter without template-wildcards" in new Scope {
    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map(
          party2 -> CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          )
        )
      )
    ) shouldBe Some(Set.empty)

    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          )
        ),
      )
    ) shouldBe Some(Set.empty)

  }

  it should "provide a party filter for template-wildcard filter" in new Scope {
    wildcardFilter(
      TransactionFilter(filtersByParty = Map(party -> CumulativeFilter.templateWildcardFilter()))
    ) shouldBe Some(Set(party))

    wildcardFilter(
      TransactionFilter(filtersByParty =
        Map(
          party -> CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set.empty,
            templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
          )
        )
      )
    ) shouldBe Some(Set(party))

    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      )
    ) shouldBe None

    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set.empty,
            templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
          )
        ),
      )
    ) shouldBe None
  }

  it should "support multiple template-wildcard filters" in new Scope {
    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter.templateWildcardFilter(),
        )
      )
    ) shouldBe Some(
      Set(
        party,
        party2,
      )
    )

    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          ),
        )
      )
    ) shouldBe Some(Set(party))
  }

  it should "support combining party-wildcard with template-wildcard filters" in new Scope {
    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter.templateWildcardFilter(),
        ),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      )
    ) shouldBe None

    wildcardFilter(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          ),
        ),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      )
    ) shouldBe None
  }

  it should "be treated as wildcard filter if templateIds and interfaceIds are empty" in new Scope {
    a[RuntimeException] should be thrownBy
      wildcardFilter(
        TransactionFilter(filtersByParty = Map(party -> CumulativeFilter(Set(), Set(), None)))
      )

    a[RuntimeException] should be thrownBy wildcardFilter(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter(Set(), Set(), None)),
      )
    )
  }

  behavior of "IndexServiceImpl.templateFilter"

  it should "give empty result for the empty input" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(filtersByParty = Map.empty),
    ) shouldBe Map.empty
  }

  it should "provide an empty template filter for template-wildcard filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(filtersByParty = Map(party -> CumulativeFilter.templateWildcardFilter())),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      ),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map(party -> CumulativeFilter.templateWildcardFilter()),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      ),
    ) shouldBe Map.empty
  }

  it should "ignore template-wildcard filters and only include template filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter.templateWildcardFilter(),
        ),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      ),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter.templateWildcardFilter(),
          party2 -> CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          ),
        )
      ),
    ) shouldBe Map(
      template1 -> Some(Set(party2))
    )

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map(
          party2 -> CumulativeFilter(
            templateFilters = Set(template1Filter),
            interfaceFilters = Set(),
            templateWildcardFilter = None,
          )
        ),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter()),
      ),
    ) shouldBe Map(
      template1 -> Some(Set(party2))
    )
  }

  it should "ignore template-wildcard filter of the shape where templateIds and interfaceIds are empty" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(filtersByParty = Map(party -> CumulativeFilter(Set(), Set(), None))),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter(Set(), Set(), None)),
      ),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for a simple template filter" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(filtersByParty =
        Map(party -> CumulativeFilter(Set(template1Filter), Set(), None))
      ),
    ) shouldBe Map(template1 -> Some(Set(party)))

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter(Set(template1Filter), Set(), None)),
      ),
    ) shouldBe Map(template1 -> None)
  }

  it should "provide an empty template filter if no template implementing this interface" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map(party -> CumulativeFilter(Set(), Set(iface1Filter), None))
      ),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter(Set(), Set(iface1Filter), None)),
      ),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for related interface filter" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))),
      TransactionFilter(
        filtersByParty = Map(party -> CumulativeFilter(Set(), Set(iface1Filter), None))
      ),
    ) shouldBe Map(template1 -> Some(Set(party)))

    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(CumulativeFilter(Set(), Set(iface1Filter), None)),
      ),
    ) shouldBe Map(template1 -> None)
  }

  it should "merge template filter and interface filter together" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template2))),
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(Set(template1Filter), Set(iface1Filter), None)
        )
      ),
    ) shouldBe Map(template1 -> Some(Set(party)), template2 -> Some(Set(party)))

    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template2))),
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          CumulativeFilter(Set(template1Filter), Set(iface1Filter), None)
        ),
      ),
    ) shouldBe Map(template1 -> None, template2 -> None)

  }

  it should "merge multiple interface filters" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy =
        Map(iface1 -> Set(template1), iface2 -> Set(template2))
      ),
      TransactionFilter(
        filtersByParty = Map(
          party ->
            CumulativeFilter(
              templateFilters = Set(TemplateFilter(template3, false)),
              interfaceFilters = Set(
                iface1Filter,
                iface2Filter,
              ),
              templateWildcardFilter = None,
            )
        )
      ),
    ) shouldBe Map(
      template1 -> Some(Set(party)),
      template2 -> Some(Set(party)),
      template3 -> Some(Set(party)),
    )
  }

  it should "merge interface filters present in both filter by party and filter for any party" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy =
        Map(iface1 -> Set(template1), iface2 -> Set(template2))
      ),
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              iface1Filter
            ),
            templateWildcardFilter = None,
          )
        ),
        filtersForAnyParty = Some(
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              iface2Filter
            ),
            templateWildcardFilter = None,
          )
        ),
      ),
    ) shouldBe Map(
      template1 -> Some(Set(party)),
      template2 -> None,
    )
  }

  it should "merge the same interface filter present in both filter by party and filter for any party" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))),
      TransactionFilter(
        filtersByParty = Map(
          party ->
            CumulativeFilter(
              templateFilters = Set.empty,
              interfaceFilters = Set(
                iface1Filter
              ),
              templateWildcardFilter = None,
            )
        ),
        filtersForAnyParty = Some(
          CumulativeFilter(
            templateFilters = Set.empty,
            interfaceFilters = Set(
              iface1Filter
            ),
            templateWildcardFilter = None,
          )
        ),
      ),
    ) shouldBe Map(
      template1 -> None
    )
  }

  behavior of "IndexServiceImpl.unknownTemplatesOrInterfaces"

  it should "provide an empty list in case of empty filter and package metadata" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(filtersByParty = Map.empty),
      PackageMetadata(),
    ) shouldBe Right(())
  }

  it should "return an unknown template for not known template" in new Scope {
    val filters = CumulativeFilter(Set(template1Filter), Set(), None)

    checkUnknownIdentifiers(
      TransactionFilter(filtersByParty = Map(party -> filters)),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Left(template1))
    )

    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(filters),
      ),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Left(template1))
    )
  }

  it should "return an unknown interface for not known interface" in new Scope {
    val filters = CumulativeFilter(Set(), Set(iface1Filter), None)

    checkUnknownIdentifiers(
      TransactionFilter(filtersByParty = Map(party -> filters)),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Right(iface1))
    )

    checkUnknownIdentifiers(
      TransactionFilter(filtersByParty = Map.empty, filtersForAnyParty = Some(filters)),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Right(iface1))
    )
  }

  it should "return a package name on unknown package name" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map(
          party ->
            CumulativeFilter(
              templateFilters = Set(template1Filter, packageNameScopedTemplateFilter),
              interfaceFilters = Set(iface1Filter),
              templateWildcardFilter = None,
            )
        )
      ),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.PackageNamesNotFound.Reject(
      unknownPackageNames = Set(packageName1)
    )
  }

  it should "return an unknown type reference for a package name/template qualified name with no known template-ids" in new Scope {
    val unknownTemplateRefFilter = TemplateFilter(
      templateTypeRef = TypeConRef.assertFromString(
        s"${Ref.PackageRef.Name(packageName1).toString}:unknownModule:unknownEntity"
      ),
      includeCreatedEventBlob = false,
    )

    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(
            templateFilters = Set(template1Filter, unknownTemplateRefFilter),
            interfaceFilters = Set(iface1Filter),
            templateWildcardFilter = None,
          )
        )
      ),
      PackageMetadata(
        interfaces = Set(iface1),
        templates = Set.empty,
        packageNameMap = Map(packageName1 -> packageResolutionForTemplate1),
      ),
    ).left.value shouldBe RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName
      .Reject(
        noKnownReferences =
          Set(packageName1 -> Ref.QualifiedName.assertFromString("unknownModule:unknownEntity"))
      )
  }

  it should "succeed for all query filter identifiers known" in new Scope {
    val filters = CumulativeFilter(
      templateFilters = Set(template1Filter, packageNameScopedTemplateFilter),
      interfaceFilters = Set(iface1Filter),
      templateWildcardFilter = None,
    )

    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map(party -> filters)
      ),
      PackageMetadata(
        interfaces = Set(iface1),
        templates = Set(template1),
        packageNameMap = Map(packageName1 -> packageResolutionForTemplate1),
      ),
    ) shouldBe Right(())

    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(filters),
      ),
      PackageMetadata(
        interfaces = Set(iface1),
        templates = Set(template1),
        packageNameMap = Map(packageName1 -> packageResolutionForTemplate1),
      ),
    ) shouldBe Right(())
  }

  it should "only return unknown templates and interfaces" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(
        filtersByParty = Map(
          party -> CumulativeFilter(Set(template1Filter), Set(iface1Filter), None),
          party2 -> CumulativeFilter(Set(template2Filter, template3Filter), Set(iface2Filter), None),
        )
      ),
      PackageMetadata(templates = Set(template1), interfaces = Set(iface1)),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Left(template2), Left(template3), Right(iface2))
    )
  }

  behavior of "IndexServiceImpl.invalidTemplateOrInterfaceMessage"

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging
  it should "provide no message if the list of invalid templates or interfaces is empty" in new Scope {
    RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(List.empty)
      .cause shouldBe ""
  }

  it should "combine a message containing invalid interfaces and templates together" in new Scope {
    RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(List(Right(iface2), Left(template2), Left(template3)))
      .cause shouldBe "Templates do not exist: [PackageId:ModuleName:template2, PackageId:ModuleName:template3]. Interfaces do not exist: [PackageId:ModuleName:iface2]."
  }

  it should "provide a message for invalid templates" in new Scope {
    RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(List(Left(template2), Left(template3)))
      .cause shouldBe "Templates do not exist: [PackageId:ModuleName:template2, PackageId:ModuleName:template3]."
  }

  it should "provide a message for invalid interfaces" in new Scope {
    RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(
        List(Right(iface1), Right(iface2))
      )
      .cause shouldBe "Interfaces do not exist: [PackageId:ModuleName:iface1, PackageId:ModuleName:iface2]."
  }

  behavior of "IndexServiceImpl.resolveUpgradableTemplates"

  it should "resolve all known upgradable template-ids for a (package-name, qualified-name) tuple" in new Scope {
    val packageResolution: PackageResolution = {
      val preferredPackageId = Ref.PackageId.assertFromString("PackageId")
      PackageResolution(
        preference =
          LocalPackagePreference(Ref.PackageVersion.assertFromString("0.1"), preferredPackageId),
        allPackageIdsForName =
          NonEmpty(Set, Ref.PackageId.assertFromString("PackageId0"), preferredPackageId),
      )
    }
    private val packageMetadata: PackageMetadata = PackageMetadata(
      templates = Set(template2),
      packageNameMap = Map(packageName1 -> packageResolution),
    )
    resolveUpgradableTemplates(
      packageMetadata,
      packageName1,
      template2.qualifiedName,
    ) shouldBe Set(template2)
  }

  it should "return an empty set if any of the resolution sets in PackageMetadata are empty" in new Scope {
    val packageResolution: PackageResolution = {
      val preferredPackageId = Ref.PackageId.assertFromString("PackageId")
      PackageResolution(
        preference =
          LocalPackagePreference(Ref.PackageVersion.assertFromString("0.1"), preferredPackageId),
        allPackageIdsForName =
          NonEmpty(Set, Ref.PackageId.assertFromString("PackageId0"), preferredPackageId),
      )
    }

    resolveUpgradableTemplates(
      PackageMetadata(
        templates = Set.empty,
        packageNameMap = Map(packageName1 -> packageResolution),
      ),
      packageName1,
      template2.qualifiedName,
    ) shouldBe Set.empty

    resolveUpgradableTemplates(
      PackageMetadata(
        templates = Set(template2),
        packageNameMap = Map.empty,
      ),
      packageName1,
      template2.qualifiedName,
    ) shouldBe Set.empty
  }
}

object IndexServiceImplSpec {
  trait Scope extends MockitoSugar {
    val party: Party = Party.assertFromString("party")
    val party2: Party = Party.assertFromString("party2")
    val templateQualifiedName1: QualifiedName =
      QualifiedName.assertFromString("ModuleName:template1")

    val packageName1: Ref.PackageName = Ref.PackageName.assertFromString("PackageName1")
    val packageName2: Ref.PackageName = Ref.PackageName.assertFromString("PackageName2")
    val template1: Identifier = Identifier.assertFromString("PackageId:ModuleName:template1")
    val template1Filter: TemplateFilter =
      TemplateFilter(templateId = template1, includeCreatedEventBlob = false)

    val packageNameScopedTemplateFilter: TemplateFilter =
      TemplateFilter(
        templateTypeRef = TypeConRef.assertFromString(
          s"${Ref.PackageRef.Name(packageName1).toString}:ModuleName:template1"
        ),
        includeCreatedEventBlob = false,
      )
    val template2: Identifier = Identifier.assertFromString("PackageId:ModuleName:template2")
    val template2Filter: TemplateFilter =
      TemplateFilter(templateId = template2, includeCreatedEventBlob = false)
    val template3: Identifier = Identifier.assertFromString("PackageId:ModuleName:template3")
    val template3Filter: TemplateFilter =
      TemplateFilter(templateId = template3, includeCreatedEventBlob = false)
    val iface1: Identifier = Identifier.assertFromString("PackageId:ModuleName:iface1")
    val iface1Filter: InterfaceFilter = InterfaceFilter(
      iface1,
      includeView = true,
      includeCreatedEventBlob = false,
    )
    val iface2: Identifier = Identifier.assertFromString("PackageId:ModuleName:iface2")
    val iface2Filter: InterfaceFilter = InterfaceFilter(
      iface2,
      includeView = true,
      includeCreatedEventBlob = false,
    )
    @volatile var currentPackageMetadata = PackageMetadata()
    val getPackageMetadata: ContextualizedErrorLogger => PackageMetadata = _ =>
      currentPackageMetadata
    val packageResolutionForTemplate1: PackageResolution = PackageResolution(
      preference = LocalPackagePreference(
        Ref.PackageVersion.assertFromString("0.1"),
        template1.packageId,
      ),
      allPackageIdsForName = NonEmpty(Set, template1.packageId),
    )
  }
}
