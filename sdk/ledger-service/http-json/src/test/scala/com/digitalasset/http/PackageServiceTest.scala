// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.{
  genDomainTemplateId,
  genDuplicateModuleEntityTemplateIds,
  nonEmptySetOf,
}
import com.daml.http.PackageService.TemplateIdMap
import com.daml.ledger.api.{v1 => lav1}
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class PackageServiceTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "PackageService.buildTemplateIdMap" - {
    "identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDuplicateModuleEntityTemplateIds) { ids =>
        toNoPkgSet(ids) should have size 1L
        val map = PackageService.buildTemplateIdMap(ids)
        map.all shouldBe ids
        map.unique shouldBe Map.empty
      }

    "2 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1))
        map.all shouldBe Set(id0, id1)
        map.unique shouldBe Map.empty
      }

    "pass one specific test case that was failing" in {
      val id0 = domain.TemplateId.fromLedgerApi(lav1.value.Identifier("a", "f4", "x"))
      val id1 = domain.TemplateId.fromLedgerApi(lav1.value.Identifier("b", "f4", "x"))
      val map = PackageService.buildTemplateIdMap(Set(id0, id1))
      map.all shouldBe Set(id0, id1)
      map.unique shouldBe Map.empty
    }

    "3 identifiers with the same (moduleName, entityName) are not unique" in
      forAll(genDomainTemplateId) { id0 =>
        val id1 = appendToPackageId("aaaa")(id0)
        val id2 = appendToPackageId("bbbb")(id1)
        val map = PackageService.buildTemplateIdMap(Set(id0, id1, id2))
        map.all shouldBe Set(id0, id1, id2)
        map.unique shouldBe Map.empty
      }

    "TemplateIdMap.all should contain dups and unique identifiers" in
      forAll(nonEmptySetOf(genDomainTemplateId), genDuplicateModuleEntityTemplateIds) {
        (xs, dups) =>
          uniqueModuleEntity(dups) shouldBe false
          whenever(uniqueModuleEntity(xs) && noModuleEntityIntersection(xs, dups)) {
            val map = PackageService.buildTemplateIdMap((xs ++ dups))
            map.all should ===(xs ++ dups)
            dups.foreach { x =>
              map.all.contains(x) shouldBe true
            }
            xs.foreach { x =>
              map.all.contains(x) shouldBe true
            }
          }
      }

    "TemplateIdMap.unique should not contain dups" in
      forAll(nonEmptySetOf(genDomainTemplateId), genDuplicateModuleEntityTemplateIds) {
        (xs, dups) =>
          uniqueModuleEntity(dups) shouldBe false
          whenever(uniqueModuleEntity(xs) && noModuleEntityIntersection(xs, dups)) {
            val map = PackageService.buildTemplateIdMap((xs ++ dups))
            map.all should ===(dups ++ xs)
            xs.foreach { x =>
              map.unique.get(PackageService.key2(x)) shouldBe Some(x)
            }
            dups.foreach { x =>
              map.unique.get(PackageService.key2(x)) shouldBe None
            }
          }
      }
  }

  "PackageService.resolveTemplateId" - {

    "should resolve unique Template ID by (moduleName, entityName)" in forAll(
      nonEmptySetOf(genDomainTemplateId)
    ) { ids =>
      val map = PackageService.buildTemplateIdMap(ids)
      val uniqueIds: Set[domain.TemplateId.RequiredPkg] = map.unique.values.toSet
      uniqueIds.foreach { id =>
        val unresolvedId: domain.TemplateId.OptionalPkg = id.copy(packageId = None)
        PackageService.resolveTemplateId(map)(unresolvedId) shouldBe Some(id)
      }
    }

    "should resolve fully qualified Template ID" in forAll(nonEmptySetOf(genDomainTemplateId)) {
      ids =>
        val map = PackageService.buildTemplateIdMap(ids)
        ids.foreach { id =>
          val unresolvedId: domain.TemplateId.OptionalPkg = id.copy(packageId = Some(id.packageId))
          PackageService.resolveTemplateId(map)(unresolvedId) shouldBe Some(id)
        }
    }

    "should return None for unknown Template ID" in forAll(
      Generators.genDomainTemplateIdO[Option[String]]
    ) { templateId: domain.TemplateId.OptionalPkg =>
      val map = TemplateIdMap(Set.empty, Map.empty)
      PackageService.resolveTemplateId(map)(templateId) shouldBe None
    }
  }

  private def appendToPackageId(x: String)(a: domain.TemplateId.RequiredPkg) =
    a.copy(packageId = a.packageId + x)

  private def uniqueModuleEntity(as: Set[domain.TemplateId.RequiredPkg]): Boolean =
    toNoPkgSet(as).size == as.size

  private def noModuleEntityIntersection(
      as: Set[domain.TemplateId.RequiredPkg],
      bs: Set[domain.TemplateId.RequiredPkg],
  ): Boolean =
    !(toNoPkgSet(as) exists toNoPkgSet(bs))

  private def toNoPkgSet(xs: Set[domain.TemplateId.RequiredPkg]): Set[domain.TemplateId.NoPkg] =
    xs.map(_ copy (packageId = ()))
}
