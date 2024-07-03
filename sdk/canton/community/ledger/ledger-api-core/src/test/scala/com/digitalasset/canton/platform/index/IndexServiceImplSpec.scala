// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party, QualifiedName, TypeConRef}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.index.IndexServiceImpl.*
import com.digitalasset.canton.platform.index.IndexServiceImplSpec.Scope
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.canton.platform.store.packagemeta.{PackageMetadata, PackageMetadataView}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{HasExecutorServiceGeneric, TestEssentials}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.MockitoSugar
import org.scalatest.Assertions.fail
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.duration.DurationInt

class IndexServiceImplSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with EitherValues
    with OptionValues {

  behavior of "IndexServiceImpl.memoizedTransactionFilterProjection"

  it should "give an empty result if no packages" in new Scope {
    when(view.current()).thenReturn(PackageMetadata())
    val memoFunc =
      memoizedTransactionFilterProjection(
        packageMetadataView = view,
        transactionFilter = TransactionFilter(Map.empty),
        verbose = true,
        alwaysPopulateArguments = false,
      )
    memoFunc() shouldBe None
  }

  it should "change the result in case of new package arrived" in new Scope {
    when(view.current()).thenReturn(PackageMetadata())
    // subscribing to iface1
    val memoFunc = memoizedTransactionFilterProjection(
      packageMetadataView = view,
      transactionFilter = TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(iface1Filter))))
      ),
      verbose = true,
      alwaysPopulateArguments = false,
    )
    memoFunc() shouldBe None // no template implementing iface1
    when(view.current())
      .thenReturn(
        PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1)))
      ) // template1 implements iface1

    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(Map(template1 -> Set(party)), Set()),
        EventProjectionProperties(
          true,
          Set.empty,
          Map(party.toString -> Map(template1 -> Projection(Set(iface1), false, false))),
        ),
      )
    ) // filter gets complicated, filters template1 for iface1, projects iface1

    when(view.current())
      .thenReturn(
        PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1, template2)))
      ) // template2 also implements iface1 as template1

    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(
          Map(
            template1 -> Set(party),
            template2 -> Set(party),
          ),
          Set(),
        ),
        EventProjectionProperties(
          true,
          Set.empty,
          Map(
            party.toString -> Map(
              template1 -> Projection(Set(iface1), false, false),
              template2 -> Projection(Set(iface1), false, false),
            )
          ),
        ),
      )
    ) // filter gets even more complicated, filters template1 and template2 for iface1, projects iface1 for both templates
  }

  it should "populate all contract arguments correctly" in new Scope {
    when(view.current())
      .thenReturn(PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))))
    // subscribing to iface1
    val memoFunc = memoizedTransactionFilterProjection(
      packageMetadataView = view,
      transactionFilter = TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(iface1Filter))))
      ),
      verbose = true,
      alwaysPopulateArguments = true,
    )
    memoFunc() shouldBe Some(
      (
        TemplatePartiesFilter(Map(template1 -> Set(party)), Set()),
        EventProjectionProperties(
          true,
          Set(party),
          Map(party.toString -> Map(template1 -> Projection(Set(iface1), false, false))),
        ),
      )
    )
  }

  behavior of "IndexServiceImpl.wildcardFilter"

  it should "give empty result for the empty input" in new Scope {
    wildcardFilter(
      TransactionFilter(Map.empty)
    ) shouldBe Set.empty
  }

  it should "provide a party filter for wildcard filter" in new Scope {
    wildcardFilter(
      TransactionFilter(Map(party -> Filters(None)))
    ) shouldBe Set(party)
  }

  it should "support multiple wildcard filters" in new Scope {
    wildcardFilter(
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(None),
        )
      )
    ) shouldBe Set(
      party,
      party2,
    )

    wildcardFilter(
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(
            Some(InclusiveFilters(templateFilters = Set(template1Filter), interfaceFilters = Set()))
          ),
        )
      )
    ) shouldBe Set(party)
  }

  it should "be treated as wildcard filter if templateIds and interfaceIds are empty" in new Scope {
    wildcardFilter(
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(), Set()))))
    ) shouldBe Set(party)
  }

  behavior of "IndexServiceImpl.templateFilter"

  it should "give empty result for the empty input" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map.empty),
    ) shouldBe Map.empty
  }

  it should "provide an empty template filter for wildcard filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(None))),
    ) shouldBe Map.empty
  }

  it should "ignore wildcard filters and only include template filters" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(None),
        )
      ),
    ) shouldBe Map.empty

    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(
          party -> Filters(None),
          party2 -> Filters(
            Some(InclusiveFilters(templateFilters = Set(template1Filter), interfaceFilters = Set()))
          ),
        )
      ),
    ) shouldBe Map(
      template1 -> Set(party2)
    )
  }

  it should "ignore wildcard filter of the shape where templateIds and interfaceIds are empty" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(), Set())))),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for a simple template filter" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(template1Filter), Set())))),
    ) shouldBe Map(template1 -> Set(party))
  }

  it should "provide an empty template filter if no template implementing this interface" in new Scope {
    templateFilter(
      PackageMetadata(),
      TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(iface1Filter))))
      ),
    ) shouldBe Map.empty
  }

  it should "provide a template filter for related interface filter" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template1))),
      TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(iface1Filter))))
      ),
    ) shouldBe Map(template1 -> Set(party))
  }

  it should "merge template filter and interface filter together" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy = Map(iface1 -> Set(template2))),
      TransactionFilter(
        Map(
          party -> Filters(
            InclusiveFilters(Set(template1Filter), Set(iface1Filter))
          )
        )
      ),
    ) shouldBe Map(template1 -> Set(party), template2 -> Set(party))
  }

  it should "merge multiple interface filters" in new Scope {
    templateFilter(
      PackageMetadata(interfacesImplementedBy =
        Map(iface1 -> Set(template1), iface2 -> Set(template2))
      ),
      TransactionFilter(
        Map(
          party -> Filters(
            InclusiveFilters(
              templateFilters = Set(TemplateFilter(template3, false)),
              interfaceFilters = Set(
                iface1Filter,
                iface2Filter,
              ),
            )
          )
        )
      ),
    ) shouldBe Map(
      template1 -> Set(party),
      template2 -> Set(party),
      template3 -> Set(party),
    )
  }

  behavior of "IndexServiceImpl.unknownTemplatesOrInterfaces"

  it should "provide an empty list in case of empty filter and package metadata" in new Scope {
    checkUnknownIdentifiers(TransactionFilter(Map.empty), PackageMetadata()) shouldBe Right(())
  }

  it should "return an unknown template for not known template" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(template1Filter), Set())))),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Left(template1))
    )
  }

  it should "return an unknown interface for not known interface" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(), Set(iface1Filter))))),
      PackageMetadata(),
    ).left.value shouldBe RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound.Reject(
      unknownTemplatesOrInterfaces = Seq(Right(iface1))
    )
  }

  it should "return a package name on unknown package name" in new Scope {
    checkUnknownIdentifiers(
      TransactionFilter(
        Map(
          party -> Filters(
            InclusiveFilters(
              Set(template1Filter, packageNameScopedTemplateFilter),
              Set(iface1Filter),
            )
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
        Map(
          party -> Filters(
            InclusiveFilters(
              Set(template1Filter, unknownTemplateRefFilter),
              Set(iface1Filter),
            )
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
    checkUnknownIdentifiers(
      TransactionFilter(
        Map(
          party -> Filters(
            InclusiveFilters(
              Set(template1Filter, packageNameScopedTemplateFilter),
              Set(iface1Filter),
            )
          )
        )
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
        Map(
          party -> Filters(InclusiveFilters(Set(template1Filter), Set(iface1Filter))),
          party2 -> Filters(
            InclusiveFilters(Set(template2Filter, template3Filter), Set(iface2Filter))
          ),
        )
      ),
      PackageMetadata(interfaces = Set(iface1), templates = Set(template1)),
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

  behavior of "IndexServiceImpl.Enhancer"

  it should "not send pruning offsets when same from inception" in new Scope {

    private val pruningOffsets = List(Some(toOffset("4")))
    // private val pruningResponses = pruningOffsets.headOption.flatten.toList.map(generatePruningTransaction)
    private val getter = pruningOffsetGetter(pruningOffsets)

    private val enhancer = Enhancer[GetTransactionsResponse](getter, Source.repeat(1).take(3))
    private val messages =
      enhancer(generatePruningTransaction, Source.empty)
        .runWith(Sink.seq)
        .futureValue
    messages shouldBe empty
  }

  it should "not send pruning offsets when they don't change" in new Scope {

    private val pruningOffsets = List(None, Some(toOffset("4")))
    private val pruningResponses = List(generatePruningTransaction(toOffset("4")))
    private val getter = pruningOffsetGetter(pruningOffsets)

    private val enhancer = Enhancer[GetTransactionsResponse](getter, Source.repeat(1).take(3))
    private val messages =
      enhancer(
        generatePruningTransaction,
        Source.single(generateTransaction(toOffset("100"))).delay(1.day),
      )
        .runWith(Sink.seq)
        .futureValue
    messages should contain theSameElementsInOrderAs pruningResponses
  }

  it should "interleave pruning offsets with the other updates" in new Scope {

    private val pruningOffsets = List(None, Some("4"), Some("8"), Some("12")).map(_.map(toOffset))
    private val pruningResponses = pruningOffsets.collect { case Some(o) =>
      generatePruningTransaction(o)
    }
    private val trOffsets = List("104", "108").map(toOffset)
    private val trResponses = trOffsets.map(generateTransaction)

    private val getter = pruningOffsetGetter(pruningOffsets)

    private val enhancer = Enhancer[GetTransactionsResponse](getter, Source.repeat(1).take(3))
    private val messages =
      enhancer(generatePruningTransaction, Source(trResponses))
        .runWith(Sink.seq)
        .futureValue
    messages should contain theSameElementsInOrderAs interleave(pruningResponses, trResponses)
  }
}

object IndexServiceImplSpec {
  trait Scope extends MockitoSugar with TestEssentials with HasExecutorServiceGeneric {
    override def handleFailure(message: String): Nothing = fail(message)

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
    val view: PackageMetadataView = mock[PackageMetadataView]
    val packageResolutionForTemplate1: PackageResolution = PackageResolution(
      preference = LocalPackagePreference(
        Ref.PackageVersion.assertFromString("0.1"),
        template1.packageId,
      ),
      allPackageIdsForName = NonEmpty(Set, template1.packageId),
    )

    val pruningOffsetGetter: List[Option[Offset]] => () => Option[Offset] = givenOffsets => {
      var offsets = givenOffsets
      () => {
        offsets match {
          case Nil => None
          case head :: Nil => head
          case head :: rest =>
            offsets = rest
            head
        }
      }
    }
    def toOffset: String => Offset = o => Offset.fromByteArray(o.getBytes)
    def interleave[A](as: List[A], bs: List[A]): List[A] =
      as.zip(bs).flatMap { case (a, b) => List(a, b) }
    def generateTransaction(offset: Offset): GetTransactionsResponse =
      GetTransactionsResponse(Seq(Transaction(offset = offset.toString)))
    def generatePruningTransaction(offset: Offset): GetTransactionsResponse =
      GetTransactionsResponse(prunedOffset = offset.toString)
    implicit val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem("IndexServiceImplSpec")(executorService)
    implicit val materializer: Materializer = Materializer(actorSystem)
  }
}
