// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.Offset
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
import com.digitalasset.canton.platform.store.cache.OffsetCheckpoint
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party, QualifiedName, TypeConRef}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.MockitoSugar
import org.scalatest.Inspectors.forAll
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

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

  behavior of "IndexServiceImpl.injectCheckpoint"
  val end = 10L
  def createSource(elements: Seq[Long]): Source[(Offset, Carrier[Unit]), NotUsed] = {
    val elementsSource = Source(elements).map(Offset.fromLong).map((_, ()))
    elementsSource.via(
      rangeDecorator(
        startExclusive = Offset.fromLong(elements.head - 1),
        endInclusive = Offset.fromLong(elements.last),
      )
    )

  }

  implicit val system: ActorSystem = ActorSystem("IndexServiceImplSpec")

  def fetchOffsetCheckpoint: Long => () => Option[OffsetCheckpoint] =
    off => () => Some(OffsetCheckpoint(offset = Offset.fromLong(off), domainTimes = Map.empty))

  it should "add a checkpoint at the right position of the stream" in new Scope {

    forAll(Seq(1L to end, Seq(1L, 5L, 10L))) { elements =>
      forAll(Seq(1L, 4L, 5L, 6L, 10L)) { checkpoint =>
        val out: Seq[Long] =
          createSource(elements)
            .via(
              injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => ())
            )
            .runWith(Sink.seq)
            .futureValue
            .map(_._1)
            .map(_.toLong)
        out shouldBe elements.appended(checkpoint).sorted
      }
    }
  }

  it should "not add a checkpoint that it is out of range" in new Scope {
    val elements = 1L to end
    val checkpoint = 11L

    val out: Seq[Long] =
      createSource(elements)
        .via(
          injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => ())
        )
        .runWith(Sink.seq)
        .futureValue
        .map(_._1)
        .map(_.toLong)
    out shouldBe elements
  }

  it should "add a checkpoint after the element if they have the same offset" in new Scope {
    val elements = 1L to end
    val source: Source[(Offset, Carrier[Option[Long]]), NotUsed] =
      Source(elements)
        .map(x => (Offset.fromLong(x), Some(x)))
        .via(
          rangeDecorator(
            startExclusive = Offset.fromLong(elements.head - 1),
            endInclusive = Offset.fromLong(elements.last),
          )
        )

    forAll(Seq(1L, 5L, 10L)) { checkpoint =>
      val out: Seq[Option[Long]] =
        source
          .via(
            injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => None)
          )
          .runWith(Sink.seq)
          .futureValue
          .map(_._2)

      out shouldBe
        (1L to checkpoint).map(Some(_)) ++ Seq(None) ++ (checkpoint + 1 to end).map(Some(_))
    }
  }

  it should "add a checkpoint invoked from timeout when its offset is at the last streamed element" in new Scope {
    val elements = 1L to end
    val checkpoint = 10L

    val out: Seq[Long] =
      createSource(elements)
        .concat(Source.single((Offset.beforeBegin, Timeout)))
        .via(
          injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => ())
        )
        .runWith(Sink.seq)
        .futureValue
        .map(_._1)
        .map(_.toLong)
    out shouldBe elements :+ checkpoint
  }

  it should "not add a checkpoint invoked from timeout when its offset is less or equal to the last streamed checkpoint" in new Scope {
    val elements = 1L to end
    val checkpoint = 10L

    val out: Seq[Long] =
      createSource(elements)
        .concat(Source.single((Offset.beforeBegin, Timeout)))
        .via(
          injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => ())
        )
        .runWith(Sink.seq)
        .futureValue
        .map(_._1)
        .map(_.toLong)
    out shouldBe elements :+ checkpoint
  }

  it should "not add the same checkpoint invoked from timeout" in new Scope {
    val elements = 1L to end
    val checkpoint = 10L

    val out: Seq[Long] =
      createSource(elements)
        .concat(Source(Seq((Offset.beforeBegin, Timeout), (Offset.beforeBegin, Timeout))))
        .via(
          injectCheckpoints(fetchOffsetCheckpoint(checkpoint), _ => ())
        )
        .runWith(Sink.seq)
        .futureValue
        .map(_._1)
        .map(_.toLong)
    out shouldBe elements :+ checkpoint
  }

  // checkpoint for element at offset #xx is denoted by Cxx,
  // (xx, RB) is the RangeBegin indicator at offset #xx
  // (xx, RE) is the RangeEnd indicator at offset #xx
  // TO is the Timeout indicator
  // NC means no checkpoint is there
  // e.g. (0,RB), C3, 1, 2, (2,RE), (2,RB), C3, 3, (3,RE) -shouldBe> 1, 2, 3, C3
  private val u: Option[Unit] = Some(())
  private val e: Carrier[Option[Unit]] = Element(u)
  private val RB: Carrier[Option[Unit]] = RangeBegin
  private val RE: Carrier[Option[Unit]] = RangeEnd
  private val TO: (Int, Carrier[Option[Unit]]) = (0, Timeout)
  private def fetchOffsetCheckpoints(
      checkpoints: mutable.Queue[Option[Int]]
  ): () => Option[OffsetCheckpoint] =
    () =>
      checkpoints
        .dequeue()
        .map(x => OffsetCheckpoint(offset = Offset.fromLong(x.toLong), domainTimes = Map.empty))

  it should "add a checkpoint if checkpoint arrived faster than the elements" in new Scope {
    // (0,RB), C3, 1, 2, (2,RE), (2,RB), C3, 3, (3,RE) -shouldBe> 1, 2, 3, C3

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (2, RE), (2, RB), (3, e), (3, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(Some(3), Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "add a checkpoint if checkpoint arrived exactly after the elements" in new Scope {
    // (0,RB), NC, 1, 2, (2,RE), (2,RB), C2, 3, (3,RE) -shouldBe> 1, 2, C2, 3

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (2, RE), (2, RB), (3, e), (3, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(None, Some(2))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (2, None), (3, u)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "not add a checkpoint if checkpoint arrived later than the elements" in new Scope {
    // (0,RB), NC, 1, 2, (2,RE), (2,RB), C1, 3, (3,RE) -shouldBe> 1, 2, 3

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (2, RE), (2, RB), (3, e), (3, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(None, Some(1))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "add multiple checkpoints" in new Scope {
    // (0,RB), NC, 1, 2, (2,RE), (2,RB), C2, 3, (3,RE), (3,RB), C3, (4,RE), (4,RB), C4, (5,RE), (7,RB), C4, (9,RE), (9,RB), C9, (10,RE), (10,RB), C12, 11, 13, 14, (15, RE)
    // -shouldBe> 1, 2, C2, 3, C3, C4, C9, 11, C12, 13, 14

    private val source = Source(
      Seq(
        (0, RB), // no checkpoint
        (1, e),
        (2, e),
        (2, RE),
        (2, RB), // C2
        (3, e),
        (3, RE),
        (3, RB), // C3
        (4, RE),
        (4, RB), // C4
        (5, RE),
        (7, RB), // C4
        (9, RE),
        (9, RB), // C9
        (10, RE),
        (10, RB), // C12
        (11, e),
        (13, e),
        (14, e),
        (15, RE),
      )
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] =
      mutable.Queue(None, Some(2), Some(3), Some(4), Some(4), Some(9), Some(12))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    // -shouldBe> 1, 2, C2, 3, C3, C4, C9, 11, C12, 13, 14
    out shouldBe
      Seq(
        (1, u),
        (2, u),
        (2, None),
        (3, u),
        (3, None),
        (4, None),
        (9, None),
        (11, u),
        (12, None),
        (13, u),
        (14, u),
      ).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "add checkpoints for dormant streams" in new Scope {
    // (0,RB), NC, 1, 2, 3, (3,RE), (TO), C4 -shouldBe> 1, 2, 3, C4

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (3, e), (3, RE), TO)
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(None, Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "add checkpoints at the right spot when streaming far from history" in new Scope {
    // (0,RB), NC, 1, 2, 3, (3,RE), (TO), C5, (3, RB), (4, RE), (4, RB), 5, (5, RE) -shouldBe> 1, 2, 3, 5, C5

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (3, e), (3, RE), TO, (3, RB), (4, RE), (4, RB), (5, e), (5, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] =
      mutable.Queue(None, Some(5), Some(5), Some(5))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (5, u), (5, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "add checkpoints at the right spot when streaming far from history when ranges empty" in new Scope {
    // (0,RB), NC, 1, 2, 3, (3,RE), (TO), C5, (3, RB), (4, RE), (4, RB), (5, RE) -shouldBe> 1, 2, 3, C5

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (3, e), (3, RE), TO, (3, RB), (4, RE), (4, RB), (5, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] =
      mutable.Queue(None, Some(5), Some(5), Some(5))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (5, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "not repeat checkpoints after regular checkpoint" in new Scope {
    // e.g. (0,RB), C3, 1, 2, 3, (3,RE), (TO), C3 -shouldBe> 1, 2, 3, C3

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (3, e), (3, RE), TO)
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(Some(3), Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "not repeat checkpoints after timeout checkpoint" in new Scope {
    // e.g. (0,RB), NC, 1, 2, 3, (3,RE), (TO), C3, (TO), C3 -shouldBe> 1, 2, 3, C3

    private val source = Source(
      Seq((0, RB), (1, e), (2, e), (3, e), (3, RE), TO, TO)
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(None, Some(3), Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "not emit checkpoints when timeout is the first" in new Scope {
    // e.g. NC, (TO), (0,RB), 1, 2, (2,RE), (2, RB), 3, (3, RE) -shouldBe> 1, 2, 3, C3

    private val source = Source(
      Seq(TO, (0, RB), (1, e), (2, e), (2, RE), (2, RB), (3, e), (3, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(Some(3), Some(3), Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue(timeout = PatienceConfiguration.Timeout(1.second))

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "not add checkpoints in the middle of a range" in new Scope {
    // (0,RB), NC, 1, (TO), C3, 2, 3, (3,RE) -shouldBe> 1, 2, 3

    private val source = Source(
      Seq((0, RB), (1, e), TO, (2, e), (3, e), (3, RE))
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] = mutable.Queue(None, Some(3))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
  }

  it should "continue regularly after idleness period" in new Scope {
    // e.g. (0,RB), NC, 1, 2, 3, (3,RE), (TO), C3, (TO), C3, (TO), C3, (3, RB), C3, 4, (4,RE), (4,RB), C4, (5,RE) -shouldBe> 1, 2, 3, C3, 4, C4

    private val source = Source(
      Seq(
        (0, RB), // no checkpoint
        (1, e),
        (2, e),
        (3, e),
        (3, RE),
        TO, // C3
        TO, // C3
        TO, // C3
        (3, RB), // C3
        (4, e),
        (4, RE),
        (4, RB), // C4
        (5, RE),
      )
    ).map { case (o, elem) => (Offset.fromLong(o.toLong), elem) }

    private val checkpoints: mutable.Queue[Option[Int]] =
      mutable.Queue(None, Some(3), Some(3), Some(3), Some(3), Some(4))

    val out: Seq[(Offset, Option[Unit])] =
      source
        .via(
          injectCheckpoints(fetchOffsetCheckpoints(checkpoints), _ => None)
        )
        .runWith(Sink.seq)
        .futureValue

    out shouldBe
      Seq((1, u), (2, u), (3, u), (3, None), (4, u), (4, None)).map { case (o, elem) =>
        (Offset.fromLong(o.toLong), elem)
      }
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
