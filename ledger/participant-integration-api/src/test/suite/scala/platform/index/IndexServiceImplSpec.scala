// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.{Filters, InclusiveFilters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.daml.platform.index.IndexServiceImplSpec.Scope
import com.daml.platform.index.IndexServiceImpl.{
  checkUnknownTemplatesOrInterfaces,
  memoizedTransactionFilterProjection,
  templateFilter,
}
import com.daml.platform.store.packagemeta.PackageMetadataView
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexServiceImplSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  behavior of "IndexServiceImpl.unknownTemplatesOrInterfaces"

  it should "provide an empty list in case of empty filter and package metadata" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(Map.empty),
      PackageMetadata(),
    ) shouldBe List()
  }

  it should "return an unknown template for not known template" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(template1), Set())))),
      PackageMetadata(),
    ) shouldBe List(Left(template1))
  }

  it should "return an unknown interface for not known interface" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(InterfaceFilter(iface1, true)))))
      ),
      PackageMetadata(),
    ) shouldBe List(Right(iface1))
  }

  it should "return zero unknown interfaces for known interface" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(
        Map(party -> Filters(InclusiveFilters(Set(), Set(InterfaceFilter(iface1, true)))))
      ),
      PackageMetadata(interfaces = Set(iface1)),
    ) shouldBe List()

  }

  it should "return zero unknown templates for known templates" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(Map(party -> Filters(InclusiveFilters(Set(template1), Set())))),
      PackageMetadata(templates = Set(template1)),
    ) shouldBe List()
  }

  it should "only return unknown templates and interfaces" in new Scope {
    checkUnknownTemplatesOrInterfaces(
      new TransactionFilter(
        Map(
          party -> Filters(InclusiveFilters(Set(template1), Set(InterfaceFilter(iface1, true)))),
          party2 -> Filters(
            InclusiveFilters(Set(template2, template3), Set(InterfaceFilter(iface2, true)))
          ),
        )
      ),
      PackageMetadata(
        templates = Set(template1),
        interfaces = Set(iface1),
      ),
    ) shouldBe List(Right(iface2), Left(template2), Left(template3))
  }

  behavior of "IndexServiceImpl.invalidTemplateOrInterfaceMessage"

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging
  it should "provide no message if the list of invalid templates or interfaces is empty" in new Scope {
    LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(List.empty)
      .cause shouldBe ""
  }

  it should "combine a message containing invalid interfaces and templates together" in new Scope {
    LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(
        List(Right(iface2), Left(template2), Left(template3))
      )
      .cause shouldBe "Templates do not exist: [PackageName:ModuleName:template2, PackageName:ModuleName:template3]. Interfaces do not exist: [PackageName:ModuleName:iface2]."
  }

  it should "provide a message for invalid templates" in new Scope {
    LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(
        List(Left(template2), Left(template3))
      )
      .cause shouldBe "Templates do not exist: [PackageName:ModuleName:template2, PackageName:ModuleName:template3]."
  }

  it should "provide a message for invalid interfaces" in new Scope {
    LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound
      .Reject(
        List(Right(iface1), Right(iface2))
      )
      .cause shouldBe "Interfaces do not exist: [PackageName:ModuleName:iface1, PackageName:ModuleName:iface2]."
  }
}

object IndexServiceImplSpec {
  trait Scope extends MockitoSugar {
    val party = Ref.Party.assertFromString("party")
    val party2 = Ref.Party.assertFromString("party2")
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val template3 = Ref.Identifier.assertFromString("PackageName:ModuleName:template3")
    val iface1 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface2")
    val view = mock[PackageMetadataView]
  }
}
